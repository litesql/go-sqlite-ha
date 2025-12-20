package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

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

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	dsn, opts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, d.Options...)
	var drv sqlite.Driver
	d.once.Do(func() {
		if d.ConnectionHook != nil {
			drv.RegisterConnectionHook(d.ConnectionHook)
		}
	})
	return ha.NewConnector(dsn, &drv, func(nodeName, filename string, disableDDLSync bool, publisher ha.Publisher, cdc ha.CDCPublisher) ha.ConnHooksProvider {
		return newConnHooksProvider(nodeName, filename, disableDDLSync, publisher, cdc)
	}, Backup, opts...)
}

func NewConnector(name string, opts ...ha.Option) (*ha.Connector, error) {
	dsn, nameOpts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, nameOpts...)
	var drv sqlite.Driver
	return ha.NewConnector(dsn, &drv, func(nodeName, filename string, disableDDLSync bool, publisher ha.Publisher, cdc ha.CDCPublisher) ha.ConnHooksProvider {
		return newConnHooksProvider(nodeName, filename, disableDDLSync, publisher, cdc)
	}, Backup, opts...)

}
