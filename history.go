package sqliteha

import (
	"github.com/litesql/go-ha"
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
	ha.HistoryCursor
}

func (m *historyModule) Create(ctx vtab.Context, args []string) (vtab.Table, error) {
	if err := ctx.Declare(ha.HistoryTableDeclaration(args[0])); err != nil {
		return nil, err
	}
	return &historyTable{connector: m.connector}, nil
}

func (m *historyModule) Connect(ctx vtab.Context, args []string) (vtab.Table, error) {
	return m.Create(ctx, args)
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
	return &historyCursor{HistoryCursor: ha.HistoryCursor{Connector: t.connector}}, nil
}

func (t *historyTable) Disconnect() error { return nil }
func (t *historyTable) Destroy() error    { return nil }

func (c *historyCursor) Filter(idxNum int, idxStr string, vals []vtab.Value) error {
	var first any
	if len(vals) > 0 {
		first = vals[0]
	}
	return c.FilterValue(len(vals) > 0, first)
}

func (c *historyCursor) Eof() bool {
	return c.EOF()
}

func (c *historyCursor) Column(col int) (vtab.Value, error) {
	return c.ColumnValue(col), nil
}
