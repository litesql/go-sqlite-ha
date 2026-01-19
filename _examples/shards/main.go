package main

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/litesql/go-sqlite-ha"
)

func main() {
	db1, err := sql.Open("sqlite-ha", "file:_examples/shards/shard1.db?_journal=WAL&_timeout=5000")
	if err != nil {
		panic(err)
	}
	defer db1.Close()

	_, err = db1.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT, x REAL);
		INSERT INTO users VALUES('Shard 1', 42);
	`)
	if err != nil {
		panic(err)
	}

	db2, err := sql.Open("sqlite-ha", "file:_examples/shards/shard2.db?_journal=WAL&_timeout=5000&queryRouter=shard[0-9]\\.db")
	if err != nil {
		panic(err)
	}
	defer db2.Close()

	_, err = db2.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT, x REAL);
		INSERT INTO users VALUES('Shard 2', 50);
		INSERT INTO users VALUES('Shard 1', 14);
	`)
	if err != nil {
		panic(err)
	}
	//query on all shards
	rows, err := db2.QueryContext(context.Background(), "SELECT rowid, name FROM users ORDER BY rowid DESC LIMIT 5")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var (
		id   int
		name string
	)
	fmt.Println("All shards results")
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID=%d Name=%s\n", id, name)
	}

	// override queryRouter using SQL hint /*+ db=DSN */
	rows, err = db2.QueryContext(context.Background(), "SELECT /*+ db=shard1 */ rowid, name FROM users")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	fmt.Println("Shard 1 results")
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("ID=%d Name=%s\n", id, name)
	}

	rows, err = db2.QueryContext(context.Background(), "SELECT /*+ db=.* */ avg(x), count(*), min(rowid), max(rowid+1), name FROM users GROUP BY name")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	fmt.Println("Shard aggregate results")
	var count, min, max int
	var avg float64
	for rows.Next() {
		err := rows.Scan(&avg, &count, &min, &max, &name)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Avg=%v Count=%d Min=%d Max=%d Name=%s\n", avg, count, min, max, name)
	}

}
