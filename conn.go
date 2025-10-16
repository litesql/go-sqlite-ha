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
	"sync"

	"github.com/litesql/go-ha"
	"github.com/litesql/sqlite"
	sqlite3 "github.com/litesql/sqlite/lib"
)

type Conn struct {
	SQLiteConn
	disableDDLSync bool
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmts, errParse := ha.Parse(context.Background(), query)
	if errParse != nil {
		return nil, errParse
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

type connHooksProvider struct {
	nodeName       string
	filename       string
	disableDDLSync bool
	publisher      ha.CDCPublisher
}

func newConnHooksProvider(nodeName string, filename string, disableDDLSync bool, publisher ha.CDCPublisher) *connHooksProvider {
	return &connHooksProvider{
		nodeName:       nodeName,
		filename:       filename,
		disableDDLSync: disableDDLSync,
		publisher:      publisher,
	}
}

type ConnHooksRegister interface {
	RegisterPreUpdateHook(sqlite.PreUpdateHookFn)
	RegisterCommitHook(sqlite.CommitHookFn)
	RegisterRollbackHook(sqlite.RollbackHookFn)
}

func (p *connHooksProvider) RegisterHooks(c driver.Conn) (driver.Conn, error) {
	sqliteConn, _ := c.(SQLiteConn)
	enableCDCHooks(sqliteConn, p.nodeName, p.filename, p.publisher)
	return &Conn{
		SQLiteConn:     sqliteConn,
		disableDDLSync: p.disableDDLSync,
	}, nil
}

func (p *connHooksProvider) DisableHooks(conn *sql.Conn) error {
	sconn, err := sqliteConn(conn)
	if err != nil {
		return err
	}
	sconn.RegisterPreUpdateHook(nil)
	sconn.RegisterCommitHook(nil)
	sconn.RegisterRollbackHook(nil)
	return nil
}

func (p *connHooksProvider) EnableHooks(conn *sql.Conn) error {
	sconn, err := sqliteConn(conn)
	if err != nil {
		return err
	}
	enableCDCHooks(sconn, p.nodeName, p.filename, p.publisher)
	return nil
}

func enableCDCHooks(sconn SQLiteConn, nodeName, filename string, publisher ha.CDCPublisher) {
	changeSetSessionsMu.Lock()
	defer changeSetSessionsMu.Unlock()

	cs := ha.NewChangeSet(nodeName, filename)
	changeSetSessions[sconn] = cs
	sconn.RegisterPreUpdateHook(func(d sqlite.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		rows, err := sconn.QueryContext(context.Background(), fmt.Sprintf("SELECT name, type FROM %s.PRAGMA_TABLE_INFO('%s')", change.Database, change.Table), nil)
		if err != nil {
			slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
			return
		}
		defer rows.Close()
		var columns, types []string
		for {
			dataRow := []driver.Value{new(string), new(string)}

			err := rows.Next(dataRow)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					slog.Error("failed to read table columns", "error", err, "table", change.Table)
				}
				break
			}
			if v, ok := dataRow[0].(string); ok {
				columns = append(columns, v)
			}
			if v, ok := dataRow[1].(string); ok {
				types = append(types, v)
			}
		}
		change.Columns = columns

		for i, t := range types {
			if t != "BLOB" {
				if i < len(change.OldValues) && change.OldValues[i] != nil {
					change.OldValues[i] = convert(change.OldValues[i])
				}
				if i < len(change.NewValues) && change.NewValues[i] != nil {
					change.NewValues[i] = convert(change.NewValues[i])
				}
			}
		}

		cs.AddChange(change)
	})

	sconn.RegisterCommitHook(func() int32 {
		if err := cs.Send(publisher); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return 1
		}
		return 0
	})
	sconn.RegisterRollbackHook(func() {
		cs.Clear()
	})
}

type SQLiteConn interface {
	driver.Conn
	sqlite.ExecQuerierContext
	NewBackup(string) (*sqlite.Backup, error)
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
		default:
			return fmt.Errorf("not a sqlite3 connection")
		}
	})
	return sqliteConn, err
}

var (
	changeSetSessions   = make(map[SQLiteConn]*ha.ChangeSet)
	changeSetSessionsMu sync.Mutex
)

func addSQLChange(conn SQLiteConn, sql string, args []any) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	cs.AddChange(ha.Change{
		Operation: "SQL",
		Command:   sql,
		Args:      args,
	})
	return nil
}

func removeLastChange(conn SQLiteConn) error {
	cs := changeSetSessions[conn]
	if cs == nil {
		return errors.New("no changeset session for the connection")
	}
	if len(cs.Changes) > 0 {
		cs.Changes = cs.Changes[:len(cs.Changes)-1]
	}
	return nil
}

func convert(src any) any {
	switch v := src.(type) {
	case []byte:
		return string(v)
	default:
		return src
	}
}

func getChange(d *sqlite.SQLitePreUpdateData) (c ha.Change, ok bool) {
	ok = true
	c = ha.Change{
		Database: d.DatabaseName,
		Table:    d.TableName,
		OldRowID: d.OldRowID,
		NewRowID: d.NewRowID,
	}
	count := d.Count()
	switch d.Op {
	case sqlite3.SQLITE_UPDATE:
		c.Operation = "UPDATE"
		c.OldValues = make([]any, count)
		c.NewValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
			c.NewValues[i] = &c.NewValues[i]
		}
		d.Old(c.OldValues...)
		d.New(c.NewValues...)
	case sqlite3.SQLITE_INSERT:
		c.Operation = "INSERT"
		c.NewValues = make([]any, count)
		for i := range count {
			c.NewValues[i] = &c.NewValues[i]
		}
		d.New(c.NewValues...)
	case sqlite3.SQLITE_DELETE:
		c.Operation = "DELETE"
		c.OldValues = make([]any, count)
		for i := range count {
			c.OldValues[i] = &c.OldValues[i]
		}
		d.Old(c.OldValues...)
	default:
		c.Operation = fmt.Sprintf("UNKNOWN - %d", d.Op)
	}

	return
}

func toNamedValues(vals []driver.Value) (r []driver.NamedValue) {
	r = make([]driver.NamedValue, len(vals))
	for i, val := range vals {
		r[i] = driver.NamedValue{Value: val, Ordinal: i + 1}
	}
	return r
}
