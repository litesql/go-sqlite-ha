package main

import (
	"context"
	"database/sql"
	"fmt"

	sqliteha "github.com/litesql/go-sqlite-ha"
)

func main() {
	c1, err := sqliteha.NewConnector("file:_examples/shards/shard1.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		panic(err)
	}
	defer c1.Close()

	db1 := sql.OpenDB(c1)
	defer db1.Close()

	_, err = db1.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT);
		INSERT INTO users VALUES('Shard 1');
	`)
	if err != nil {
		panic(err)
	}

	c2, err := sqliteha.NewConnector("file:_examples/shards/shard2.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&queryRouter=shard[0-9]\\.db")
	if err != nil {
		panic(err)
	}
	defer c2.Close()

	db2 := sql.OpenDB(c2)
	defer db2.Close()

	_, err = db2.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT);
		INSERT INTO users VALUES('Shard 2');
	`)
	if err != nil {
		panic(err)
	}

	rows, err := db2.QueryContext(context.Background(), "SELECT rowid, name FROM users")
	if err != nil {
		panic(err)
	}
	var (
		id   int
		name string
	)
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID=%d Name=%s\n", id, name)
	}
}
