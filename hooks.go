package sqliteha

import (
	"database/sql/driver"
	"fmt"

	"github.com/litesql/go-ha"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type ConnHooksRegister interface {
	RegisterPreUpdateHook(sqlite.PreUpdateHookFn)
	RegisterCommitHook(sqlite.CommitHookFn)
	RegisterRollbackHook(sqlite.RollbackHookFn)
}

type SQLiteConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	sqlite.ExecQuerierContext
	sqlite.HookRegisterer
	NewBackup(string) (*sqlite.Backup, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type moderncConn struct {
	SQLiteConn
}

func (c moderncConn) Deserialize(b []byte, _ string) error {
	return c.SQLiteConn.Deserialize(b)
}

func (c moderncConn) UnwrapSQLiteConn() driver.Conn {
	return c.SQLiteConn
}

type hooksAdapter struct{}

func (a *hooksAdapter) WrapConn(c driver.Conn, _ *ha.Connector) (ha.SQLiteConn, error) {
	sqliteConn, ok := c.(SQLiteConn)
	if !ok {
		return nil, fmt.Errorf("not a sqlite connection: %T", c)
	}
	return moderncConn{SQLiteConn: sqliteConn}, nil
}

func (a *hooksAdapter) EnableHooks(conn ha.SQLiteConn, cb ha.HookCallbacks) {
	sconn := conn.(moderncConn).SQLiteConn
	sconn.RegisterPreUpdateHook(func(d sqlite.SQLitePreUpdateData) {
		if d.TableName == ha.TwoPhaseCommitDecisionTable || d.TableName == ha.OutboxTable {
			return
		}
		if change, ok := getChange(&d); ok {
			cb.OnPreUpdate(change)
		}
	})
	sconn.RegisterCommitHook(func() int32 {
		if err := cb.OnCommit(); err != nil {
			return 1
		}
		return 0
	})
	sconn.RegisterRollbackHook(cb.OnRollback)
}

func (a *hooksAdapter) DisableHooks(conn ha.SQLiteConn) {
	sconn := conn.(moderncConn).SQLiteConn
	sconn.RegisterPreUpdateHook(nil)
	sconn.RegisterCommitHook(nil)
	sconn.RegisterRollbackHook(nil)
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
