package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/litesql/go-sqlite-ha"
)

// You need to previously exec go run ./_examples/node1

func main() {
	db, err := sql.Open("sqlite-ha", "file:_examples/node2/my.db?_journal=WAL&_timeout=5000&replicationURL=nats://localhost:4222&name=node2")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	time.Sleep(2 * time.Second) // wait for sync
	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM users").Scan(&name)
	if err != nil {
		panic(err)
	}

	fmt.Println("User:", name)
}
