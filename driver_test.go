package sqliteha_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/litesql/go-ha"
	sqliteha "github.com/litesql/go-sqlite-ha"
)

func TestConnector(t *testing.T) {
	pub := new(fakePublisher)
	connector, err := sqliteha.NewConnector("file:/test.db?vfs=memdb", ha.WithCDCPublisher(pub))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	_, err = db.ExecContext(context.TODO(), "CREATE TABLE users(ID INTEGER PRIMARY KEY, name TEXT); CREATE TABLE users2(ID INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	if len(pub.changes) != 1 {
		t.Errorf("want 1 changes, but got %d", len(pub.changes))
	}
	if pub.changes[0].Operation != "SQL" {
		t.Errorf("expect SQL operation, but got %q", pub.changes[0].Operation)
	}
	want := "CREATE TABLE IF NOT EXISTS users(ID INTEGER PRIMARY KEY, name TEXT); CREATE TABLE IF NOT EXISTS users2(ID INTEGER PRIMARY KEY, name TEXT)"
	if pub.changes[0].Command != want {
		t.Errorf("want %q, got %q", want, pub.changes[0].Command)
	}
	_, err = db.ExecContext(context.TODO(), "INSERT INTO users(name) VALUES(?)", "test")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	if len(pub.changes) != 1 {
		t.Errorf("want 1 changes, but got %d", len(pub.changes))
	}
	if pub.changes[0].Operation != "INSERT" {
		t.Errorf("expect INSERT operation, but got %q", pub.changes[0].Operation)
	}
}

type fakePublisher struct {
	err      error
	changes  []ha.Change
	sequence uint64
}

func (f *fakePublisher) Publish(cs *ha.ChangeSet) error {
	f.changes = cs.Changes
	return f.err
}

func (f *fakePublisher) SetSequence(s uint64) {
	f.sequence = s
}

func (f *fakePublisher) Sequence() uint64 {
	return f.sequence
}
