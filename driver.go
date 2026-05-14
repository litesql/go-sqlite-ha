package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	_ "unsafe"

	"github.com/litesql/go-ha"
	"modernc.org/sqlite"
)

func init() {
	sql.Register("sqlite-ha", &Driver{})
}

type Driver struct {
	once           sync.Once
	ConnectionHook sqlite.ConnectionHookFn
	Options        []ha.Option
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

//go:linkname drivers database/sql.drivers
var drivers map[string]driver.Driver

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	dsn, opts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, d.Options...)
	drv := new(sqlite.Driver)
	if driver, ok := drivers["sqlite"]; ok {
		drv = driver.(*sqlite.Driver)
	}
	d.once.Do(func() {
		if d.ConnectionHook != nil {
			drv.RegisterConnectionHook(d.ConnectionHook)
		}
	})
	return ha.NewConnector(dsn, drv, func(cfg ha.ConnHooksConfig) ha.ConnHooksProvider {
		return newConnHooksProvider(cfg)
	}, Backup, opts...)
}

func NewConnector(name string, opts ...ha.Option) (*ha.Connector, error) {
	dsn, nameOpts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, nameOpts...)
	drv := new(sqlite.Driver)
	if driver, ok := drivers["sqlite"]; ok {
		drv = driver.(*sqlite.Driver)
	}
	return ha.NewConnector(dsn, drv, func(cfg ha.ConnHooksConfig) ha.ConnHooksProvider {
		return newConnHooksProvider(cfg)
	}, Backup, opts...)

}
