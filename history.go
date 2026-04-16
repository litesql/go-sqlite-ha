package sqliteha

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/litesql/go-ha"
	haconnect "github.com/litesql/go-ha/connect"
	_ "modernc.org/sqlite"
	"modernc.org/sqlite/vtab"
)

func NewHistoryModule(connector *ha.Connector) vtab.Module {
	return &historyModule{connector: connector}
}

type historyModule struct {
	connector *ha.Connector
}

type historyTable struct {
	connector *ha.Connector
}

type historyCursor struct {
	connector *ha.Connector
	rows      []haconnect.HistoryItem
	pos       int
}

func (m *historyModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	// args: [moduleName, dbName, tableName, ...module args]
	err := ctx.Declare(fmt.Sprintf(`
		CREATE TABLE %s (
			seq INT,
			sql TEXT,
			timestamp TEXT
		)`, args[0]))
	if err != nil {
		return nil, err
	}
	return &historyTable{connector: m.connector}, nil
}

func (m *historyModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	err := ctx.Declare(fmt.Sprintf(`
		CREATE TABLE %s (
			seq INT,
			sql TEXT,
			timestamp TEXT
		)`, args[0]))
	if err != nil {
		return nil, err
	}
	return &historyTable{connector: m.connector}, nil
}

func (t *historyTable) BestIndex(info *vtab.IndexInfo) error {
	for i := range info.Constraints {
		c := &info.Constraints[i]
		if c.Usable && c.Column == 0 && c.Op == vtab.OpGE {
			c.ArgIndex = 1
			c.Omit = false
			info.IdxNum = 1
			return nil
		}
	}
	info.IdxNum = 0 // latest transaction
	return nil
}

func (t *historyTable) Open() (vtab.Cursor, error) {
	return &historyCursor{connector: t.connector}, nil
}

func (t *historyTable) Disconnect() error { return nil }
func (t *historyTable) Destroy() error    { return nil }

func (c *historyCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	var seq uint64
	if len(vals) > 0 {
		switch v := vals[0].(type) {
		case int64:
			if v < 0 {
				v = 1
			}
			seq = uint64(v)
		default:
			return fmt.Errorf("invalid seq value type: %T", vals[0])
		}
	}
	var err error
	c.rows, err = c.connector.HistoryBySeq(context.Background(), seq)
	if err != nil {
		return err
	}
	c.pos = 0
	return nil
}

func (c *historyCursor) Next() error {
	if c.pos < len(c.rows) {
		c.pos++
	}
	return nil
}

func (c *historyCursor) Eof() bool {
	return c.pos >= len(c.rows)
}

func (c *historyCursor) Column(col int) (vtab.Value, error) {
	if c.pos < len(c.rows) {
		switch col {
		case 0:
			return int64(c.rows[c.pos].Seq), nil
		case 1:
			return strings.Join(c.rows[c.pos].SQL, ";\n"), nil
		case 2:
			return time.Unix(0, c.rows[c.pos].Timestamp).Format("2006-01-02T15:04:05.999"), nil
		}
	}

	return nil, nil
}

func (c *historyCursor) Rowid() (int64, error) {
	return int64(c.pos + 1), nil
}

func (c *historyCursor) Close() error {
	return nil
}
