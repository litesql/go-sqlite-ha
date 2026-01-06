package sqliteha

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/litesql/go-ha"
)

func (c *Conn) crossShardQuery(ctx context.Context, query string, args []driver.NamedValue, queryRouter *regexp.Regexp, stmt *ha.Statement) (driver.Rows, error) {
	slog.Debug("Executing cross-shard query", "query", query, "queryRouter", queryRouter.String())
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
	return unionQueries(context.WithValue(ctx, ignoreQueryRouterKey, true), dbs, query, args, stmt)
}

func unionQueries(ctx context.Context, dbs []*sql.DB, query string, args []driver.NamedValue, stmt *ha.Statement) (driver.Rows, error) {
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

	chValues := make(chan []driver.Value)
	if !stmt.HasDistinct() && len(stmt.OrderBy()) == 0 {
		result := newStreamResults(chValues)
		var nextErrs error
		chFirstResponse := make(chan struct{})
		var hasValues bool
		go func() {
			defer close(chValues)
			for rows := range chRows {
				size := len(rows.Columns())
				for {
					values := make([]driver.Value, size)
					err := rows.Next(values)
					if err != nil {
						if err == io.EOF {
							break
						}
						nextErrs = errors.Join(nextErrs, err)
						break
					}
					if !hasValues {
						hasValues = true
						result.setColumns(rows.Columns())
						close(chFirstResponse)
					}
					chValues <- values
				}
			}
			if !hasValues {
				close(chFirstResponse)
			}
		}()
		<-chFirstResponse
		if !hasValues {
			wg.Wait()
			errs = errors.Join(errs, nextErrs)
			if errs != nil {
				return nil, errs
			}
			return nil, sql.ErrNoRows
		}
		return result, nil
	}
	result := newBufferedResults(stmt.HasDistinct())
	var nextErrs error
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
				nextErrs = errors.Join(nextErrs, err)
				break
			}
			result.append(row)
		}
	}
	wg.Wait()
	errs = errors.Join(errs, nextErrs)
	if result.empty() && errs != nil {
		return nil, errs
	}
	if err := result.sort(stmt.OrderBy()); err != nil {
		return nil, err
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

func newStreamResults(results chan []driver.Value) *streamResults {
	return &streamResults{
		results: results,
	}
}

type streamResults struct {
	columns    []string
	results    chan []driver.Value
	hasResults bool
}

func (r *streamResults) setColumns(columns []string) {
	r.columns = columns
}

func (r *streamResults) Columns() []string {
	return r.columns
}

func (r *streamResults) Close() error {
	return nil
}

func (r *streamResults) Next(dest []driver.Value) error {
	src, ok := <-r.results
	if !ok {
		return io.EOF
	}
	r.hasResults = true
	copy(dest, src)
	return nil
}

type bufferedResults struct {
	distinct bool
	columns  []string
	values   [][]driver.Value
	index    int
}

func newBufferedResults(distinct bool) *bufferedResults {
	return &bufferedResults{
		distinct: distinct,
	}
}

func (r *bufferedResults) setColumns(columns []string) {
	r.columns = columns
}

func (r *bufferedResults) Columns() []string {
	return r.columns
}

func (r *bufferedResults) Close() error {
	r.values = nil
	return nil
}

func (r *bufferedResults) Next(dest []driver.Value) error {
	if r.values == nil || r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (r *bufferedResults) append(row []driver.Value) {
	if !r.distinct {
		r.values = append(r.values, row)
		return
	}
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

func (r *bufferedResults) empty() bool {
	return len(r.values) == 0
}

func (r *bufferedResults) sort(orderBy []string) error {
	if len(orderBy) == 0 {
		return nil
	}
	if err := r.validOrderByClause(orderBy); err != nil {
		return err
	}

	slices.SortFunc(r.values, r.sortFunc(orderBy))
	return nil
}

func (r *bufferedResults) sliceIndexByNameOrOrder(column string) int {
	for i, col := range r.columns {
		if strings.EqualFold(column, col) {
			return i
		}
	}

	if i, err := strconv.Atoi(column); err == nil {
		return i - 1
	}
	return -1
}

func (r *bufferedResults) validOrderByClause(orderBy []string) error {
	for _, column := range orderBy {
		var exists bool
		column := strings.TrimSuffix(column, " DESC")
		for _, col := range r.columns {
			if strings.EqualFold(column, col) {
				exists = true
				break
			}
		}
		if !exists {
			if i, err := strconv.Atoi(column); err == nil && i >= 1 && len(column) >= i {
				exists = true
			}
		}
		if !exists {
			return fmt.Errorf("invalid orderBy clause: %s", column)
		}
	}
	return nil
}

type orderClause struct {
	order int
	desc  bool
}

func (r *bufferedResults) sortFunc(orderBy []string) func(a []driver.Value, b []driver.Value) int {
	clauses := make([]orderClause, 0)
	for _, ob := range orderBy {
		column, desc := strings.CutSuffix(ob, " DESC")
		clauses = append(clauses, orderClause{
			order: r.sliceIndexByNameOrOrder(column),
			desc:  desc,
		})
	}
	return func(a, b []driver.Value) int {
		for _, term := range clauses {
			var result int
			aValue := a[term.order]
			bValue := b[term.order]
			switch x := aValue.(type) {
			case int64:
				if y, ok := bValue.(int64); ok {
					result = cmp.Compare(x, y)
				}
			case float64:
				if y, ok := bValue.(float64); ok {
					result = cmp.Compare(x, y)
				}
			case string:
				if y, ok := bValue.(string); ok {
					result = cmp.Compare(x, y)
				}
			case time.Time:
				if y, ok := bValue.(time.Time); ok {
					result = x.Compare(y)
				}
			case []byte:
				if y, ok := bValue.([]byte); ok {
					result = bytes.Compare(x, y)
				}
			}
			if result != 0 {
				if term.desc {
					return result * -1
				}
				return result
			}

		}
		return 0
	}
}
