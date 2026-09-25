package main

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
)

// Two Phase Commit example
// Run _example/node1 first
// go run ./_examples/node1
func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	c, err := sqliteha.NewConnector("file:_examples/2pc/my.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&2pcPeers=http://localhost:5001(secret-token)&2pcTimeout=3s",
		ha.WithName("node_two_phase_commit"),
		ha.WithReplicationSubscriber(ha.NewNoopSubscriber()),
		ha.WithAutoStart(true))
	if err != nil {
		panic(err)
	}
	defer c.Close()
	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT);
		INSERT INTO users VALUES('HA user 2PC param');
	`)
	if err != nil {
		panic(err)
	}
}
