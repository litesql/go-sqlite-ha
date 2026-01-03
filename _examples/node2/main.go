package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/litesql/go-sqlite-ha"
)

// You need to previously exec go run ./_examples/node1

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	db, err := sql.Open("sqlite-ha", "file:_examples/node2/my.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&replicationURL=nats://localhost:4223&name=node2&leaderProvider=static:localhost:5001")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.ExecContext(context.Background(), "INSERT INTO users(name) VALUES('redirect to leader')")
	if err != nil {
		panic(err)
	}
	var name string
	err = db.QueryRowContext(context.Background(), "SELECT name FROM users ORDER BY rowid DESC LIMIT 1").Scan(&name)
	if err != nil {
		panic(err)
	}

	fmt.Println("User:", name)
}
