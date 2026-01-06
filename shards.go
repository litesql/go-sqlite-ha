package sqliteha

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"sync"

	"github.com/litesql/go-ha"
)

func (c *Conn) crossShardQuery(ctx context.Context, query string, args []driver.NamedValue, queryRouter *regexp.Regexp, stmt *ha.Statement) (driver.Rows, error) {
	slog.Debug("Executing cross-sharding query", "query", query, "queryRouter", queryRouter.String())
	dbs := make([]*sql.DB, 0)
	for _, dsn := range ha.ListDSN() {
		if !queryRouter.MatchString(dsn) {
			continue
		}
		connector, _ := ha.LookupConnector(dsn)
		if connector == nil {
			continue
		}
		dbs = append(dbs, connector.DB())
	}

	if len(dbs) == 0 {
		return nil, fmt.Errorf("no databases available to execute query based on queryRouter=%s", queryRouter.String())
	}
	return unionQueries(context.WithValue(ctx, ignoreQueryRouterKey, true), dbs, query, args, stmt.HasDistinct())
}

func unionQueries(ctx context.Context, dbs []*sql.DB, query string, args []driver.NamedValue, hasDistinct bool) (driver.Rows, error) {
	var result shardsResults
	if hasDistinct {
		result = &multiDistinctResults{
			&multiResults{},
		}
	} else {
		result = &multiResults{}
	}
	//TODO create a worker pool
	chErr := make(chan error, len(dbs))
	chRows := make(chan driver.Rows, len(dbs))
	var errs error
	go func() {
		var wg sync.WaitGroup
		for _, db := range dbs {
			wg.Go(func() {
				rows, err := queryDB(ctx, db, query, args)
				if err != nil {
					chErr <- err
					return
				}
				chRows <- rows
			})
		}
		wg.Wait()
		close(chErr)
		close(chRows)
	}()

	var wg sync.WaitGroup
	wg.Go(func() {
		for err := range chErr {
			if !errors.Is(err, sql.ErrNoRows) {
				errs = errors.Join(errs, err)
			}
		}
	})

	for rows := range chRows {
		result.setColumns(rows.Columns())
		size := len(rows.Columns())
		for {
			row := make([]driver.Value, size)
			err := rows.Next(row)
			if err != nil {
				if err == io.EOF {
					break
				}
				break
			}
			result.append(row)
		}
	}
	wg.Wait()

	if len(result.getValues()) == 0 && errs != nil {
		return nil, errs
	}
	return result, nil
}

func queryDB(ctx context.Context, db *sql.DB, query string, args []driver.NamedValue) (driver.Rows, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	sqlConn, err := sqliteConn(conn)
	if err != nil {
		return nil, err
	}
	return sqlConn.QueryContext(ctx, query, args)
}

type shardsResults interface {
	driver.Rows
	append([]driver.Value)
	setColumns([]string)
	getValues() [][]driver.Value
}

type multiResults struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *multiResults) setColumns(columns []string) {
	r.columns = columns
}

func (r *multiResults) Columns() []string {
	return r.columns
}

func (r *multiResults) Close() error {
	r.values = nil
	return nil
}

func (r *multiResults) Next(dest []driver.Value) error {
	if r.values == nil || r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *multiResults) append(row []driver.Value) {
	r.values = append(r.values, row)
}

func (r *multiResults) getValues() [][]driver.Value {
	return r.values
}

type multiDistinctResults struct {
	*multiResults
}

func (r *multiDistinctResults) append(row []driver.Value) {
	var exists bool
	for _, val := range r.values {
		if slices.Equal(val, row) {
			exists = true
			break
		}
	}
	if !exists {
		r.values = append(r.values, row)
	}
}
