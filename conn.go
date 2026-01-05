package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	hagrpc "github.com/litesql/go-ha/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrTimedOut = errors.New("Timed out")

type Conn struct {
	SQLiteConn
	disableDDLSync bool
	enableRedirect bool

	currentRedirectTarget string
	grpcClientConn        *grpc.ClientConn

	leader        ha.LeaderProvider
	replicationID string
	reqCh         chan *sqlv1.QueryRequest
	resCh         chan *sqlv1.QueryResponse

	txseq uint64

	activeTransaction bool

	txseqTracker ha.TxSeqTracker
	timeout      time.Duration
}

func (c *Conn) Deserialize(b []byte, _ string) error {
	return c.SQLiteConn.Deserialize(b)
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	slog.Debug("ExecContext", "query", query, "enableRedirect", c.enableRedirect)
	stmts, errParse := ha.Parse(ctx, query)
	if errParse != nil {
		return nil, errParse
	}

	var modifies bool
	for _, stmt := range stmts {
		if stmt.ModifiesDatabase() {
			modifies = true
			break
		}
	}
	if (modifies || c.activeTransaction) && c.enableRedirect && !c.leader.IsLeader() {
		slog.Debug("Redirecting", "to", c.leader.RedirectTarget(), "query", query)
		params := make([]*sqlv1.NamedValue, len(args))
		for i, arg := range args {
			val, err := hagrpc.ToAnypb(arg.Value)
			if err != nil {
				return nil, err
			}
			params[i] = &sqlv1.NamedValue{
				Name:    arg.Name,
				Ordinal: int64(arg.Ordinal),
				Value:   val,
			}
		}
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()

		select {
		case c.reqCh <- &sqlv1.QueryRequest{
			Type:          sqlv1.QueryType_QUERY_TYPE_EXEC,
			Sql:           query,
			Params:        params,
			ReplicationId: c.replicationID,
		}:
			res := <-c.resCh
			if res.Error != "" {
				return nil, errors.New(res.Error)
			}
			if res.Txseq > 0 {
				c.txseq = res.Txseq
			}
			return result{
				lastInsertId: res.LastInsertId,
				rowsAffected: res.RowsAffected,
			}, nil
		case <-ctx.Done():
			if !c.activeTransaction {
				return nil, driver.ErrBadConn
			}
			return nil, ErrTimedOut
		}
	}

	var ddlCommands strings.Builder
	if !c.disableDDLSync {
		for _, stmt := range stmts {
			if stmt.DDL() {
				ddlCommands.WriteString(stmt.SourceWithIfExists())
			}
		}
	}
	if ddlCommands.Len() > 0 {
		if err := addSQLChange(c.SQLiteConn, ddlCommands.String(), nil); err != nil {
			return nil, err
		}
	}
	res, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err != nil && ddlCommands.Len() > 0 {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, toNamedValues(args))
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	slog.Debug("QuerContext", "query", query, "enableRedirect", c.enableRedirect)
	if c.leader.IsLeader() {
		return c.SQLiteConn.QueryContext(ctx, query, args)
	}
	if query == "SELECT received_seq FROM ha_stats WHERE subject = ?" {
		return c.SQLiteConn.QueryContext(ctx, query, args)
	}
	stmts, errParse := ha.Parse(ctx, query)
	if errParse != nil {
		return nil, errParse
	}

	var modifies bool
	for _, stmt := range stmts {
		if stmt.ModifiesDatabase() {
			modifies = true
			break
		}
	}
	if (modifies || c.activeTransaction) && c.enableRedirect {
		return c.redirectQuery(ctx, query, args)
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	ctxTimeout, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
LOOP:
	for {
		if c.txseqTracker.LatestSeq() >= c.txseq {
			break LOOP
		}

		select {
		case <-ctxTimeout.Done():
			return c.redirectQuery(ctx, query, args)
		case <-ticker.C:
		}
	}
	return c.SQLiteConn.QueryContext(ctx, query, args)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, toNamedValues(args))
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.enableRedirect && !c.leader.IsLeader() {
		ctx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		select {
		case c.reqCh <- &sqlv1.QueryRequest{
			Type:          sqlv1.QueryType_QUERY_TYPE_EXEC,
			Sql:           "BEGIN",
			ReplicationId: c.replicationID,
		}:
			res := <-c.resCh
			if res.Error != "" {
				return nil, errors.New(res.Error)
			}
			c.activeTransaction = true
			return &tx{
				Conn: c,
			}, nil
		case <-ctx.Done():
			return nil, driver.ErrBadConn
		}
	}
	c.activeTransaction = true
	return c.SQLiteConn.BeginTx(ctx, opts)
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) redirectQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	slog.Debug("Redirecting query", "to", c.leader.RedirectTarget())
	params := make([]*sqlv1.NamedValue, len(args))
	for i, arg := range args {
		val, err := hagrpc.ToAnypb(arg.Value)
		if err != nil {
			return nil, err
		}
		params[i] = &sqlv1.NamedValue{
			Name:    arg.Name,
			Ordinal: int64(arg.Ordinal),
			Value:   val,
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_UNSPECIFIED,
		Sql:           query,
		Params:        params,
		ReplicationId: c.replicationID,
	}:
		res := <-c.resCh
		if res.Error != "" {
			return nil, errors.New(res.Error)
		}
		if res.Txseq > 0 {
			c.txseq = res.Txseq
		}
		return &rows{
			data: res.ResultSet,
		}, nil
	case <-ctx.Done():
		if c.activeTransaction {
			return nil, ErrTimedOut
		}
		return nil, driver.ErrBadConn
	}
}

type tx struct {
	*Conn
}

func (tx *tx) Commit() error {
	select {
	case tx.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC,
		Sql:           "COMMIT",
		ReplicationId: tx.replicationID,
	}:
		res := <-tx.resCh
		if res.Error != "" {
			return errors.New(res.Error)
		}
		tx.Conn.activeTransaction = false
	case <-time.After(tx.Conn.timeout):
		return ErrTimedOut
	}

	return nil
}

func (tx *tx) Rollback() error {
	select {
	case tx.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC,
		Sql:           "ROLLBACK",
		ReplicationId: tx.replicationID,
	}:
		res := <-tx.resCh
		if res.Error != "" {
			return errors.New(res.Error)
		}
		tx.Conn.activeTransaction = false
	case <-time.After(tx.timeout):
		return ErrTimedOut
	}
	return nil
}

func (c *Conn) Close() error {
	c.activeTransaction = false
	if c.grpcClientConn != nil {
		c.grpcClientConn.Close()
	}
	return c.SQLiteConn.Close()
}

func (c *Conn) start() error {
	if c.leader.IsLeader() {
		return nil
	}
	target := c.leader.RedirectTarget()
	lower := strings.ToLower(target)
	// http(s) protocols are used for the HTTP leader proxy middleware
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return nil
	}
	if target == c.currentRedirectTarget {
		return nil
	}
	if c.grpcClientConn != nil {
		c.grpcClientConn.Close()
	}
	var err error
	c.grpcClientConn, err = grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		slog.Debug("connect to grpc", "target", target, "error", err)
		return driver.ErrBadConn
	}
	client := sqlv1.NewDatabaseServiceClient(c.grpcClientConn)
	stream, err := client.Query(context.Background())
	if err != nil {
		slog.Debug("query over grpc", "target", target, "error", err)
		return driver.ErrBadConn
	}
	c.currentRedirectTarget = target

	go func() {
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				c.currentRedirectTarget = ""
				c.activeTransaction = false
				return // Stream closed
			}
			if err != nil {
				c.currentRedirectTarget = ""
				c.activeTransaction = false
				slog.Debug("failed to receive message", "error", err)
				return
			}
			c.resCh <- msg
		}
	}()

	go func() {
		for req := range c.reqCh {
			err := stream.Send(req)
			if err != nil {
				c.currentRedirectTarget = ""
				c.activeTransaction = false
				slog.Debug("failed to send message", "error", err)
				return
			}
		}
	}()

	return nil
}

type result struct {
	lastInsertId int64
	rowsAffected int64
}

func (r result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type rows struct {
	data  *sqlv1.Data
	index int
}

func (r *rows) Columns() []string {
	if r.data == nil {
		return []string{}
	}
	return r.data.GetColumns()
}

func (r *rows) Close() error {
	r.data = nil
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.data == nil || r.data.Rows == nil || r.index >= len(r.data.Rows) {
		return io.EOF
	}
	row := r.data.Rows[r.index]
	for i, val := range row.GetValues() {
		dest[i] = hagrpc.FromAnypb(val)
	}
	r.index++
	return nil
}

type rawer interface {
	Raw() driver.Conn
}

func haSqliteConn(conn *sql.Conn) (*Conn, error) {
	var haSqliteConn *Conn
	err := conn.Raw(func(driverConn any) error {
		switch c := driverConn.(type) {
		case *Conn:
			haSqliteConn = c
			return nil
		case rawer:
			switch c2 := c.Raw().(type) {
			case *Conn:
				haSqliteConn = c2
				return nil
			default:
				return fmt.Errorf("not a sqlite connection: %T", c2)
			}
		default:
			return fmt.Errorf("not a sqlite connection: %T", conn)
		}
	})
	return haSqliteConn, err
}

func sqliteConn(conn *sql.Conn) (SQLiteConn, error) {
	var sqliteConn SQLiteConn
	err := conn.Raw(func(driverConn any) error {
		switch c := driverConn.(type) {
		case *Conn:
			sqliteConn = c.SQLiteConn
			return nil
		case SQLiteConn:
			sqliteConn = c
			return nil
		case rawer:
			switch c2 := c.Raw().(type) {
			case *Conn:
				sqliteConn = c2.SQLiteConn
				return nil
			case SQLiteConn:
				sqliteConn = c2
				return nil
			default:
				return fmt.Errorf("not a sqlite connection: %T", c2)
			}
		default:
			return fmt.Errorf("not a sqlite connection: %T", conn)
		}
	})
	return sqliteConn, err
}

func toNamedValues(vals []driver.Value) (r []driver.NamedValue) {
	r = make([]driver.NamedValue, len(vals))
	for i, val := range vals {
		r[i] = driver.NamedValue{Value: val, Ordinal: i + 1}
	}
	return r
}
