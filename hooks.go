package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type connHooksProvider struct {
	*ha.Connector
}

type ConnHooksRegister interface {
	RegisterPreUpdateHook(sqlite.PreUpdateHookFn)
	RegisterCommitHook(sqlite.CommitHookFn)
	RegisterRollbackHook(sqlite.RollbackHookFn)
}

func (p *connHooksProvider) RegisterHooks(c driver.Conn, connector *ha.Connector) (driver.Conn, error) {
	p.Connector = connector
	sqliteConn, _ := c.(SQLiteConn)
	// Register the ha_history virtual table module. --- IGNORED because modernc.org/sqlite/vtab couldn't register module for an openned connection ---
	/*
		err := vtab.RegisterModule(nil, "ha_history", &historyModule{connector: connector})
		if err != nil {
			return nil, fmt.Errorf("failed to register ha_history vtab module: %w", err)
		}
	*/
	enableCDCHooks(sqliteConn, p.Connector)
	conn := &Conn{
		SQLiteConn:              sqliteConn,
		disableDDLSync:          p.DisableDDLSync(),
		enableRedirect:          true,
		replicationID:           p.ReplicationID(),
		leader:                  p.LeaderProvider(),
		reqCh:                   make(chan *sqlv1.QueryRequest),
		resCh:                   make(chan *sqlv1.QueryResponse),
		txseqTracker:            p.Subscriber(),
		timeout:                 p.GrpcTimeout(),
		token:                   p.GrpcToken(),
		insecure:                p.GrpcInsecure(),
		proxiedDB:               connector.ProxiedDB(),
		proxiedPositionProvider: connector.ProxiedPositionProvider(),
		queryRouter:             p.QueryRouter(),
	}
	return conn, conn.start()
}

func (p *connHooksProvider) DisableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	sconn.RegisterPreUpdateHook(nil)
	sconn.RegisterCommitHook(nil)
	sconn.RegisterRollbackHook(nil)
	sconn.enableRedirect = false
	return nil
}

func (p *connHooksProvider) EnableHooks(conn *sql.Conn) error {
	sconn, err := haSqliteConn(conn)
	if err != nil {
		return err
	}
	enableCDCHooks(sconn.SQLiteConn, p.Connector)
	sconn.enableRedirect = true
	return sconn.start()
}

type tableSchema struct {
	columns, types, pkColumns []string
}

var tableSchemaCache = make(map[string]map[string]*tableSchema)

func clearTableSchemaCache(replicationID string) {
	delete(tableSchemaCache, replicationID)
}

func getTableSchema(sconn SQLiteConn, replicationID string, database string, table string) (*tableSchema, error) {
	key := fmt.Sprintf("%s.%s", database, table)
	replicationTables, ok := tableSchemaCache[replicationID]
	if !ok {
		replicationTables = map[string]*tableSchema{}
		tableSchemaCache[replicationID] = replicationTables
	} else {
		if schema, ok := replicationTables[key]; ok {
			return schema, nil
		}
	}
	var schema tableSchema
	rows, err := sconn.QueryContext(context.Background(), fmt.Sprintf("SELECT name, type, pk FROM %s.PRAGMA_TABLE_INFO('%s') ORDER BY cid", database, table), nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for {
		dataRow := []driver.Value{new(string), new(string), new(int64)}

		err := rows.Next(dataRow)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Error("failed to read table columns", "error", err, "table", table)
			}
			break
		}
		if v, ok := dataRow[0].(string); ok {
			schema.columns = append(schema.columns, v)
		} else {
			continue
		}
		if v, ok := dataRow[1].(string); ok {
			schema.types = append(schema.types, v)
		}
		if v, ok := dataRow[2].(int64); ok && v > 0 {
			schema.pkColumns = append(schema.pkColumns, dataRow[0].(string))
		}
	}
	replicationTables[key] = &schema
	return &schema, nil

}

func enableCDCHooks(sconn SQLiteConn, connector *ha.Connector) {
	cs := ha.NewChangeSet(connector.NodeName(), connector.ReplicationID())
	changeSetSessionsMu.Lock()
	changeSetSessions[sconn] = cs
	changeSetSessionsMu.Unlock()
	sconn.RegisterPreUpdateHook(func(d sqlite.SQLitePreUpdateData) {
		change, ok := getChange(&d)
		if !ok {
			return
		}
		schema, err := getTableSchema(sconn, connector.ReplicationID(), change.Database, change.Table)
		if err != nil {
			slog.Error("failed to read columns", "error", err, "database", change.Database, "table", change.Table)
			return
		}
		change.Columns = schema.columns
		change.PKColumns = schema.pkColumns
		for i, t := range schema.types {
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
		if err := cs.Send(connector.Publisher()); err != nil {
			slog.Error("failed to send changeset", "error", err)
			return 1
		}
		if connector.CDCPublisher() != nil {
			data := cs.DebeziumData()
			if len(data) > 0 {
				if err := connector.CDCPublisher().Publish(data); err != nil {
					slog.Error("failed to send cdc", "error", err)
					return 1
				}
			}
		}
		return 0
	})
	sconn.RegisterRollbackHook(func() {
		cs.Clear()
	})
}

type SQLiteConn interface {
	driver.Conn
	driver.ConnBeginTx
	sqlite.ExecQuerierContext
	sqlite.HookRegisterer
	NewBackup(string) (*sqlite.Backup, error)
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

var (
	changeSetSessions   = make(map[SQLiteConn]*ha.ChangeSet)
	changeSetSessionsMu sync.RWMutex
)

func addSQLChange(conn SQLiteConn, sql string, args []any) error {
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

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
	changeSetSessionsMu.RLock()
	defer changeSetSessionsMu.RUnlock()

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
