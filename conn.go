package sqliteha

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	haconnect "github.com/litesql/go-ha/connect"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var ErrTimedOut = errors.New("Timed out")

var queryRouterHintMatcher = regexp.MustCompile(`(?i)/\*\+\s*db=(.*?)\s*\*/`).FindStringSubmatch

type contextKey int

const ignoreQueryRouterKey contextKey = iota

type ProxiedQuerierExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Conn struct {
	SQLiteConn

	connector      *ha.Connector
	enableRedirect bool

	currentRedirectTarget string
	grpcClientConn        *grpc.ClientConn
	reqCh                 chan *sqlv1.QueryRequest
	resCh                 chan *sqlv1.QueryResponse

	txseq uint64

	activeTransaction bool

	txseqTracker ha.TxSeqTracker

	invalid bool

	proxiedTxExecer           ProxiedQuerierExecer
	currentWritePosition      uint64
	latestTransactionPosition uint64
}

func (c *Conn) Deserialize(b []byte, _ string) error {
	return c.SQLiteConn.Deserialize(b)
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var (
		modifies bool
		stmts    []*ha.Statement
	)
	if c.redirectToGrpc(true) || !c.connector.DisableDDLSync() || c.connector.ProxiedDB() != nil || c.twoPhaseEnabled() {
		var err error
		stmts, err = ha.Parse(ctx, query)
		if err != nil {
			if !ha.LocalDB(ctx) {
				if c.redirectToGrpc(true) {
					slog.Debug("invalid sqlite syntax, redirecting to leader", "error", err)
					res, err2 := c.redirectExecToGrpc(ctx, query, args)
					if err2 != nil {
						return nil, errors.Join(err, err2)
					}
					return res, nil
				} else if c.connector.ProxiedDB() != nil {
					slog.Debug("invalid sqlite syntax, redirecting to proxied db", "error", err)
					res, err2 := c.proxiedQuerierExecer().ExecContext(ctx, query, toSqlValues(args)...)
					if err2 != nil {
						return nil, errors.Join(err, err2)
					} else {
						c.updateProxiedPosition(ctx)
					}
					return res, nil
				}
			}

			return nil, err
		}

		for _, stmt := range stmts {
			if c.twoPhaseEnabled() && isTransactionControl(stmt) {
				return nil, errors.New("use database/sql transaction methods when two-phase commit is enabled")
			}
			if stmt.ModifiesDatabase() {
				modifies = true
				break
			}
		}
	}
	if c.redirectToGrpc(modifies) {
		return c.redirectExecToGrpc(ctx, query, args)
	}

	var ddlCommands strings.Builder
	if !c.connector.DisableDDLSync() {
		for _, stmt := range stmts {
			if stmt.DDL() {
				ddlCommands.WriteString(stmt.SourceWithIfExists())
			}
		}
	}
	if ddlCommands.Len() > 0 {
		clearTableSchemaCache(c.connector.ReplicationID())
		if err := addSQLChange(c.SQLiteConn, ddlCommands.String(), nil); err != nil {
			return nil, err
		}
	}
	var (
		res driver.Result
		err error
	)
	if c.connector.ProxiedDB() != nil && modifies && !ha.LocalDB(ctx) {
		res, err = c.proxiedQuerierExecer().ExecContext(ctx, query, toSqlValues(args)...)
		if err == nil {
			c.updateProxiedPosition(ctx)
		}
	} else if modifies && !c.activeTransaction && !ha.LocalDB(ctx) && c.twoPhaseEnabled() {
		res, err = c.execLocalTwoPhase(ctx, query, args)
	} else {
		res, err = c.SQLiteConn.ExecContext(ctx, query, args)
	}
	if err != nil && ddlCommands.Len() > 0 {
		removeLastChange(c.SQLiteConn)
	}
	return res, err
}

func (c *Conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, toNamedValues(args))
}

func (c *Conn) IsValid() bool {
	return !c.invalid
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if ha.LocalDB(ctx) {
		return c.SQLiteConn.QueryContext(ctx, query, args)
	}
	var (
		modifies bool
		stmts    []*ha.Statement
	)
	if c.redirectToGrpc(true) || !c.connector.DisableDDLSync() || c.connector.ProxiedDB() != nil || c.twoPhaseEnabled() {
		var err error
		stmts, err = ha.Parse(ctx, query)
		if err != nil {
			if c.redirectToGrpc(true) {
				slog.Debug("invalid sqlite syntax, redirecting to leader", "error", err)
				res, err2 := c.redirectQuery(ctx, query, args)
				if err2 != nil {
					return nil, errors.Join(err, err2)
				}
				return res, nil
			} else if c.connector.ProxiedDB() != nil {
				slog.Debug("invalid sqlite syntax, redirecting to proxied db", "error", err)
				res, err2 := c.redirectQueryToProxied(ctx, query, args)
				if err2 != nil {
					return nil, errors.Join(err, err2)
				}
				return res, nil
			}
			return nil, err
		}

		for _, stmt := range stmts {
			if c.twoPhaseEnabled() && isTransactionControl(stmt) {
				return nil, errors.New("use database/sql transaction methods when two-phase commit is enabled")
			}
			if stmt.ModifiesDatabase() {
				modifies = true
				break
			}
		}
	}
	if c.redirectToGrpc(modifies) {
		return c.redirectQuery(ctx, query, args)
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	ctxTimeout, cancel := context.WithTimeout(ctx, c.connector.GrpcTimeout())
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
	if len(stmts) == 1 && !c.ignoreQueryRouter(ctx) {
		qr := c.connector.QueryRouter()
		queryRouterExp := queryRouterHintMatcher(query)
		if len(queryRouterExp) == 2 {
			if exp, err := regexp.Compile(strings.TrimSpace(queryRouterExp[1])); err == nil {
				qr = exp
			}
		}
		if qr != nil && qr.String() != "self" {
			return ha.CrossShardQuery(context.WithValue(ctx, ignoreQueryRouterKey, true), stmts[0], args, qr, func(c *sql.Conn) (driver.QueryerContext, error) {
				return sqliteConn(c)
			})
		}
	}
	if c.connector.ProxiedDB() != nil && (modifies || c.activeTransaction) {
		rows, err := c.redirectQueryToProxied(ctx, query, args)
		if err != nil {
			return nil, err
		}
		if modifies {
			c.updateProxiedPosition(ctx)
		}
		return rows, err
	}

	if c.currentWritePosition == 0 || c.connector.ProxiedDB() == nil {
		return c.queryLocal(ctx, query, args, modifies)
	}

	tickerRYW := time.NewTicker(time.Millisecond)
	defer tickerRYW.Stop()

	ctxRYWTimeout, cancel := context.WithTimeout(ctx, c.connector.GrpcTimeout())
	defer cancel()
LOOPRYW: //RYW = Read Your Writes
	for {
		replicaPosition, err := c.connector.ProxiedPositionProvider().ReplicaPosition(ctx)
		if err != nil {
			slog.Debug("get replica position", "error", err)
		}
		slog.Debug("checking positions", "replica", replicaPosition, "currentWritePosition", c.currentWritePosition)
		if replicaPosition >= c.currentWritePosition {
			break LOOPRYW
		}

		select {
		case <-ctxRYWTimeout.Done():
			return c.redirectQueryToProxied(ctx, query, args)
		case <-tickerRYW.C:
		}
	}
	return c.queryLocal(ctx, query, args, modifies)
}

func (c *Conn) twoPhaseEnabled() bool {
	_, ok := c.connector.Publisher().(ha.TwoPhaseCommitPreparer)
	return ok
}

func isTransactionControl(stmt *ha.Statement) bool {
	switch stmt.Type() {
	case ha.TypeBegin, ha.TypeCommit, ha.TypeRollback, ha.TypeSavepoint, ha.TypeRelease:
		return true
	default:
		return false
	}
}

func (c *Conn) recoverTwoPhase(ctx context.Context) error {
	if ha.LocalDB(ctx) {
		return nil
	}
	if recoverer, ok := c.connector.Publisher().(interface{ RecoverTwoPhaseCommits() error }); ok {
		return recoverer.RecoverTwoPhaseCommits()
	}
	return nil
}

func (c *Conn) execLocalTwoPhase(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.recoverTwoPhase(ctx); err != nil {
		return nil, err
	}
	tx, err := c.SQLiteConn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	c.activeTransaction = true
	res, err := c.SQLiteConn.ExecContext(ctx, query, args)
	if err != nil {
		c.activeTransaction = false
		return nil, errors.Join(err, tx.Rollback())
	}
	if err := (&txLocal{Tx: tx, c: c, ctx: ctx}).Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Conn) queryLocal(ctx context.Context, query string, args []driver.NamedValue, modifies bool) (driver.Rows, error) {
	if !modifies || c.activeTransaction || !c.twoPhaseEnabled() || ha.LocalDB(ctx) {
		return c.SQLiteConn.QueryContext(ctx, query, args)
	}
	if err := c.recoverTwoPhase(ctx); err != nil {
		return nil, err
	}
	tx, err := c.SQLiteConn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	c.activeTransaction = true
	rows, err := c.SQLiteConn.QueryContext(ctx, query, args)
	if err != nil {
		c.activeTransaction = false
		return nil, errors.Join(err, tx.Rollback())
	}
	return &twoPhaseRows{Rows: rows, tx: &txLocal{Tx: tx, c: c, ctx: ctx}}, nil
}

func (c *Conn) updateProxiedPosition(ctx context.Context) {
	if c.connector.ProxiedPositionProvider() == nil {
		return
	}
	position, err := c.connector.ProxiedPositionProvider().SourcePosition(ctx)
	if err == nil {
		c.currentWritePosition = position
	}
}

func (c *Conn) redirectExecToGrpc(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	slog.Debug("Redirecting", "to", c.connector.LeaderProvider().RedirectTarget(), "query", query)
	params := make([]*sqlv1.NamedValue, len(args))
	for i, arg := range args {
		val, err := haconnect.ToAnypb(arg.Value)
		if err != nil {
			return nil, err
		}
		params[i] = &sqlv1.NamedValue{
			Name:    arg.Name,
			Ordinal: int64(arg.Ordinal),
			Value:   val,
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.connector.GrpcTimeout())
	defer cancel()

	select {
	case c.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:           query,
		Params:        params,
		ReplicationId: c.connector.ReplicationID(),
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

func (c *Conn) proxiedQuerierExecer() ProxiedQuerierExecer {
	if c.proxiedTxExecer != nil {
		return c.proxiedTxExecer
	}
	return c.connector.ProxiedDB()
}

func (c *Conn) redirectQueryToProxied(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.proxiedQuerierExecer().QueryContext(ctx, query, toSqlValues(args)...)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnsCount := len(columns)
	if columnsCount == 0 {
		return nil, fmt.Errorf("no columns")
	}

	dataRows := make(chan []any, 1)
	go func() {
		defer func() {
			rows.Close()
			close(dataRows)
		}()
		for rows.Next() {
			values := make([]any, columnsCount)
			valuePtrs := make([]any, columnsCount)
			for i := range values {
				valuePtrs[i] = &values[i]
			}
			if err := rows.Scan(valuePtrs...); err != nil {
				return
			}
			for i, val := range values {
				if b, ok := val.([]uint8); ok {
					values[i] = string(b)
				}
			}
			dataRows <- values
		}
	}()

	return &driverRows{columns: columns, data: dataRows}, nil
}

type driverRows struct {
	columns []string
	data    chan []any
	index   int
}

func (r *driverRows) Columns() []string {
	return r.columns
}

func (r *driverRows) Next(dest []driver.Value) error {
	values, ok := <-r.data
	if !ok {
		return io.EOF
	}
	for i := range len(values) {
		dest[i] = values[i]
	}
	return nil
}

func (r *driverRows) Close() error {
	return nil
}

func (c *Conn) ignoreQueryRouter(ctx context.Context) bool {
	val := ctx.Value(ignoreQueryRouterKey)
	if val == nil {
		return false
	}
	return val.(bool)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, toNamedValues(args))
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.redirectToGrpc(true) {
		ctx, cancel := context.WithTimeout(ctx, c.connector.GrpcTimeout())
		defer cancel()
		select {
		case c.reqCh <- &sqlv1.QueryRequest{
			Type:          sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
			Sql:           "BEGIN",
			ReplicationId: c.connector.ReplicationID(),
		}:
			res := <-c.resCh
			if res.Error != "" {
				return nil, errors.New(res.Error)
			}
			c.activeTransaction = true
			return &txGRPC{
				Conn: c,
			}, nil
		case <-ctx.Done():
			return nil, driver.ErrBadConn
		}
	}
	if c.connector.ProxiedDB() != nil && !ha.LocalDB(ctx) {
		proxiedTx, err := c.connector.ProxiedDB().BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.IsolationLevel(opts.Isolation),
			ReadOnly:  opts.ReadOnly,
		})
		if err != nil {
			return nil, err
		}
		c.proxiedTxExecer = proxiedTx
		c.activeTransaction = true
		c.latestTransactionPosition = c.currentWritePosition
		return &txProxied{
			Tx: proxiedTx,
			c:  c,
		}, nil
	}
	if err := c.recoverTwoPhase(ctx); err != nil {
		return nil, err
	}
	tx, err := c.SQLiteConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	c.activeTransaction = true
	return &txLocal{Tx: tx, c: c, ctx: ctx}, nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) redirectQuery(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	slog.Debug("Redirecting query", "to", c.connector.LeaderProvider().RedirectTarget(), "query", query)
	params := make([]*sqlv1.NamedValue, len(args))
	for i, arg := range args {
		val, err := haconnect.ToAnypb(arg.Value)
		if err != nil {
			return nil, err
		}
		params[i] = &sqlv1.NamedValue{
			Name:    arg.Name,
			Ordinal: int64(arg.Ordinal),
			Value:   val,
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.connector.GrpcTimeout())
	defer cancel()
	select {
	case c.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC_QUERY,
		Sql:           query,
		Params:        params,
		ReplicationId: c.connector.ReplicationID(),
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

type txGRPC struct {
	*Conn
}

func (tx *txGRPC) Commit() error {
	select {
	case tx.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:           "COMMIT",
		ReplicationId: tx.connector.ReplicationID(),
	}:
		res := <-tx.resCh
		if res.Error != "" {
			return errors.New(res.Error)
		}
		tx.Conn.activeTransaction = false
	case <-time.After(tx.Conn.connector.GrpcTimeout()):
		return ErrTimedOut
	}

	return nil
}

func (tx *txGRPC) Rollback() error {
	select {
	case tx.reqCh <- &sqlv1.QueryRequest{
		Type:          sqlv1.QueryType_QUERY_TYPE_EXEC_UPDATE,
		Sql:           "ROLLBACK",
		ReplicationId: tx.connector.ReplicationID(),
	}:
		res := <-tx.resCh
		if res.Error != "" {
			return errors.New(res.Error)
		}
		tx.Conn.activeTransaction = false
	case <-time.After(tx.connector.GrpcTimeout()):
		return ErrTimedOut
	}
	return nil
}

type txLocal struct {
	driver.Tx
	c   *Conn
	ctx context.Context
}

func (tx *txLocal) Commit() error {
	if tx.c == nil {
		return tx.Tx.Commit()
	}
	preparer, ok := tx.c.connector.Publisher().(ha.TwoPhaseCommitPreparer)
	if !ok {
		err := tx.Tx.Commit()
		tx.c.activeTransaction = false
		return err
	}
	cs := snapshotChangeSet(tx.c.SQLiteConn)
	if cs == nil || len(cs.Changes) == 0 {
		err := tx.Tx.Commit()
		tx.c.activeTransaction = false
		return err
	}
	prepared, err := preparer.PrepareTwoPhaseCommit(cs)
	if err != nil {
		tx.c.activeTransaction = false
		return errors.Join(err, tx.Tx.Rollback())
	}
	ctx := tx.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	err = prepared.RecordCommitDecision(func(writeCtx context.Context, query string, args ...any) error {
		if writeCtx == nil {
			writeCtx = ctx
		}
		named := make([]driver.NamedValue, len(args))
		for i, arg := range args {
			named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
		}
		_, err := tx.c.SQLiteConn.ExecContext(writeCtx, query, named)
		return err
	})
	if err != nil {
		abortErr := prepared.Abort()
		tx.c.activeTransaction = false
		return errors.Join(err, abortErr, tx.Tx.Rollback())
	}
	if err := tx.Tx.Commit(); err != nil {
		tx.c.activeTransaction = false
		_ = tx.Tx.Rollback()
		committed, resolveErr := prepared.ResolveCommit()
		if committed {
			clearChangeSet(tx.c.SQLiteConn)
			publishChangeSetCDC(tx.c.connector, cs)
			if resolveErr != nil {
				return fmt.Errorf("%w: %w", ha.ErrTwoPhaseCommitPending, resolveErr)
			}
			return nil
		}
		return errors.Join(err, resolveErr)
	}
	tx.c.activeTransaction = false
	clearChangeSet(tx.c.SQLiteConn)
	commitErr := prepared.Commit()
	publishChangeSetCDC(tx.c.connector, cs)
	if commitErr != nil {
		return fmt.Errorf("%w: %w", ha.ErrTwoPhaseCommitPending, commitErr)
	}
	return nil
}

func (tx *txLocal) Rollback() error {
	if tx.c != nil {
		tx.c.activeTransaction = false
	}
	return tx.Tx.Rollback()
}

type twoPhaseRows struct {
	driver.Rows
	tx       *txLocal
	finished bool
}

func (r *twoPhaseRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	if errors.Is(err, io.EOF) {
		if rows, ok := r.Rows.(driver.RowsNextResultSet); ok && rows.HasNextResultSet() {
			return io.EOF
		}
		return r.finish(true)
	}
	if err != nil {
		return errors.Join(err, r.finish(false))
	}
	return nil
}

func (r *twoPhaseRows) Close() error {
	if r.finished {
		return nil
	}
	dest := make([]driver.Value, len(r.Rows.Columns()))
	for {
		for {
			err := r.Rows.Next(dest)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return errors.Join(err, r.finish(false))
			}
		}
		rows, ok := r.Rows.(driver.RowsNextResultSet)
		if !ok || !rows.HasNextResultSet() {
			err := r.finish(true)
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if err := rows.NextResultSet(); err != nil {
			return errors.Join(err, r.finish(false))
		}
	}
}

func (r *twoPhaseRows) finish(commit bool) error {
	if r.finished {
		return nil
	}
	r.finished = true
	closeErr := r.Rows.Close()
	if !commit || closeErr != nil {
		return errors.Join(closeErr, r.tx.Rollback())
	}
	if err := r.tx.Commit(); err != nil {
		return err
	}
	return io.EOF
}

func (r *twoPhaseRows) HasNextResultSet() bool {
	rows, ok := r.Rows.(driver.RowsNextResultSet)
	return ok && rows.HasNextResultSet()
}

func (r *twoPhaseRows) NextResultSet() error {
	rows, ok := r.Rows.(driver.RowsNextResultSet)
	if !ok {
		return io.EOF
	}
	if err := rows.NextResultSet(); err != nil {
		if errors.Is(err, io.EOF) {
			return r.finish(true)
		}
		return errors.Join(err, r.finish(false))
	}
	return nil
}

func (r *twoPhaseRows) ColumnTypeDatabaseTypeName(index int) string {
	rows, ok := r.Rows.(driver.RowsColumnTypeDatabaseTypeName)
	if !ok {
		return ""
	}
	return rows.ColumnTypeDatabaseTypeName(index)
}

func (r *twoPhaseRows) ColumnTypeLength(index int) (int64, bool) {
	rows, ok := r.Rows.(driver.RowsColumnTypeLength)
	if !ok {
		return 0, false
	}
	return rows.ColumnTypeLength(index)
}

func (r *twoPhaseRows) ColumnTypeNullable(index int) (bool, bool) {
	rows, ok := r.Rows.(driver.RowsColumnTypeNullable)
	if !ok {
		return false, false
	}
	return rows.ColumnTypeNullable(index)
}

func (r *twoPhaseRows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	rows, ok := r.Rows.(driver.RowsColumnTypePrecisionScale)
	if !ok {
		return 0, 0, false
	}
	return rows.ColumnTypePrecisionScale(index)
}

func (r *twoPhaseRows) ColumnTypeScanType(index int) reflect.Type {
	rows, ok := r.Rows.(driver.RowsColumnTypeScanType)
	if !ok {
		return reflect.TypeOf(new(any)).Elem()
	}
	return rows.ColumnTypeScanType(index)
}

type txProxied struct {
	*sql.Tx
	c *Conn
}

func (tx *txProxied) Commit() error {
	tx.c.activeTransaction = false
	tx.c.proxiedTxExecer = nil
	return tx.Tx.Commit()
}

func (tx *txProxied) Rollback() error {
	tx.c.activeTransaction = false
	tx.c.proxiedTxExecer = nil
	tx.c.currentWritePosition = tx.c.latestTransactionPosition
	return tx.Tx.Rollback()
}

func (c *Conn) ResetSession(ctx context.Context) error {
	c.activeTransaction = false
	c.currentWritePosition = 0
	c.latestTransactionPosition = 0
	c.proxiedTxExecer = nil
	return nil
}

func (c *Conn) Close() error {
	var err error
	if c.grpcClientConn != nil {
		err = c.grpcClientConn.Close()
	}
	return errors.Join(err, c.SQLiteConn.Close())
}

func (c *Conn) Mutex() *sync.Mutex {
	return c.connector.Mutex()
}

func (c *Conn) redirectToGrpc(modifies bool) bool {
	return (modifies || c.activeTransaction) && c.enableRedirect && !c.connector.LeaderProvider().IsLeader() && c.currentRedirectTarget != ""
}

func (c *Conn) start() error {
	if c.connector.LeaderProvider().IsLeader() {
		if c.grpcClientConn != nil {
			c.grpcClientConn.Close()
		}
		return nil
	}
	target := c.connector.LeaderProvider().RedirectTarget()
	lower := strings.ToLower(target)
	// http(s) protocols are used for the HTTP leader proxy middleware
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return nil
	}
	if target == c.currentRedirectTarget {
		return nil
	}

	c.currentRedirectTarget = target

	if c.grpcClientConn != nil {
		c.grpcClientConn.Close()
	}
	var err error
	var dialOpts []grpc.DialOption

	if c.connector.GrpcInsecure() {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	if c.connector.GrpcToken() != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcCredentials{token: c.connector.GrpcToken()}))
	}
	c.grpcClientConn, err = grpc.NewClient(target, dialOpts...)
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

	go func() {
		sesisonTarget := target
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				if c.currentRedirectTarget == sesisonTarget {
					c.invalid = true
					c.currentRedirectTarget = ""
				}
				return // Stream closed
			}
			if err != nil {
				if c.currentRedirectTarget == sesisonTarget {
					c.invalid = true
					c.currentRedirectTarget = ""
				}
				st, ok := status.FromError(err)
				if ok && st.Code() != codes.Canceled {
					slog.Debug("failed to receive message", "error", err)
					go func() {
						c.resCh <- &sqlv1.QueryResponse{
							Error: err.Error(),
						}
					}()
				}
				return
			}
			c.resCh <- msg
		}
	}()

	go func() {
		sesisonTarget := target
		for {
			select {
			case <-time.After(25 * time.Second):
				err := stream.Send(&sqlv1.QueryRequest{
					Type: sqlv1.QueryType_QUERY_TYPE_PING,
				})
				if err != nil {
					c.currentRedirectTarget = ""
					c.activeTransaction = false
					slog.Debug("failed to send ping", "error", err)
					if c.currentRedirectTarget == sesisonTarget {
						c.invalid = true
						c.currentRedirectTarget = ""
					}
					return
				}
				<-c.resCh // wait for pong
			case req := <-c.reqCh:
				err := stream.Send(req)
				if err != nil {
					c.currentRedirectTarget = ""
					c.activeTransaction = false
					slog.Debug("failed to send message", "error", err)
					if c.currentRedirectTarget == sesisonTarget {
						c.invalid = true
						c.currentRedirectTarget = ""
					}
					return
				}
			}
		}
	}()

	return nil
}

type grpcCredentials struct {
	token string
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return false
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
		dest[i] = haconnect.FromAnypb(val)
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

func toSqlValues(vals []driver.NamedValue) (r []any) {
	if len(vals) == 0 {
		return nil
	}
	if vals[0].Name != "" {
		r = make([]any, len(vals))
		for i, val := range vals {
			r[i] = sql.Named(val.Name, val.Value)
		}
		return r
	}
	r = make([]any, len(vals))
	for _, val := range vals {
		r[val.Ordinal-1] = val.Value
	}
	return r
}
