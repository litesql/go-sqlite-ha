package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/litesql/go-ha"
	"github.com/litesql/sqlite"
)

func init() {
	sql.Register("sqlite-ha", &Driver{})
}

type Driver struct {
	Options []ha.Option
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
	return ha.NewConnector(dsn, &drv, func(nodeName, filename string, disableDDLSync bool, publisher ha.CDCPublisher) ha.ConnHooksProvider {
		return newConnHooksProvider(nodeName, filename, disableDDLSync, publisher)
	}, Backup, opts...)
}

func NewConnector(name string, opts ...ha.Option) (*ha.Connector, error) {
	dsn, nameOpts, err := ha.NameToOptions(name)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	opts = append(opts, nameOpts...)
	var drv sqlite.Driver
	return ha.NewConnector(dsn, &drv, func(nodeName, filename string, disableDDLSync bool, publisher ha.CDCPublisher) ha.ConnHooksProvider {
		return newConnHooksProvider(nodeName, filename, disableDDLSync, publisher)
	}, Backup, opts...)

}
