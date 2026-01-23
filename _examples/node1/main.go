package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	c, err := sqliteha.NewConnector("file:_examples/node1/my.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)",
		ha.WithName("node1"),
		ha.WithGrpcPort(5001),
		ha.WithGrpcToken("secret-token"),
		ha.WithEmbeddedNatsConfig(&ha.EmbeddedNatsConfig{
			Port: 4223,
		}),
		ha.WithAutoStart(false))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	db := sql.OpenDB(c)
	defer db.Close()

	err = c.Start(db)
	if err != nil {
		panic(err)
	}
	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT);
		INSERT INTO users VALUES('HA user');
	`)
	if err != nil {
		panic(err)
	}

	slog.Info("Press CTRL+C to exit")
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
}
