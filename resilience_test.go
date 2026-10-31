package sqliteha_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/litesql/go-ha"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	_ "github.com/litesql/go-ha"
)

type resilienceHooks struct{}

func (resilienceHooks) RegisterHooks(conn driver.Conn, _ *ha.Connector) (driver.Conn, error) {
	return conn, nil
}

func (resilienceHooks) DisableHooks(*sql.Conn) error { return nil }
func (resilienceHooks) EnableHooks(*sql.Conn) error  { return nil }

type resilienceNode struct {
	conn       *nats.Conn
	db         *sql.DB
	subscriber *ha.NATSSubscriber
}

func newResilienceNode(t *testing.T, nc *nats.Conn, dbPath, node, durable, stream, subject string) *resilienceNode {
	t.Helper()

	db, err := sql.Open("sqlite-ha", dbPath)
	if err != nil {
		t.Fatalf("open sqlite database for %s: %v", node, err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS events (id INTEGER NOT NULL)`); err != nil {
		t.Fatalf("create events table for %s: %v", node, err)
	}

	nodeConn, err := nats.Connect(nc.ConnectedUrl(),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(50*time.Millisecond),
		nats.Timeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("connect NATS for %s: %v", node, err)
	}
	t.Cleanup(nodeConn.Close)

	subscriber, err := ha.NewNATSSubscriber(ha.NATSSubscriberConfig{
		Node:         node,
		Durable:      durable,
		NatsConn:     nodeConn,
		Stream:       stream,
		Subject:      subject,
		Policy:       "all",
		DB:           db,
		ConnProvider: resilienceHooks{},
		RowIdentify:  ha.PK,
	})
	if err != nil {
		t.Fatalf("create subscriber for %s: %v", node, err)
	}
	if err := subscriber.Start(); err != nil {
		t.Fatalf("start subscriber for %s: %v", node, err)
	}
	return &resilienceNode{conn: nodeConn, db: db, subscriber: subscriber}
}

func newResilienceNATSServer(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()

	ns, err := server.NewServer(&server.Options{
		ServerName: fmt.Sprintf("resilience-%d", port),
		Host:       "127.0.0.1",
		Port:       port,
		JetStream:  true,
		StoreDir:   storeDir,
		NoLog:      true,
		NoSigs:     true,
	})
	if err != nil {
		t.Fatalf("create NATS server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("NATS server did not become ready")
	}
	return ns
}

func resilienceNATSConn(t *testing.T, url string) *nats.Conn {
	t.Helper()

	nc, err := nats.Connect(url,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(50*time.Millisecond),
		nats.Timeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func resilienceStream(t *testing.T, nc *nats.Conn, stream, subject string) *ha.NATSPublisher {
	t.Helper()

	publisher, err := ha.NewNATSPublisher(nc, subject, 3*time.Second, &jetstream.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Storage:   jetstream.FileStorage,
		Replicas:  1,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("create replication publisher: %v", err)
	}
	return publisher
}

func resilienceChangeSet(replicationID string, id int) *ha.ChangeSet {
	return &ha.ChangeSet{
		Node:     "publisher",
		Filename: replicationID,
		Changes: []ha.Change{{
			Operation: "SQL",
			Command:   fmt.Sprintf("INSERT INTO events(id) VALUES (%d)", id),
		}},
	}
}

func waitForSequence(t *testing.T, subscriber *ha.NATSSubscriber, want uint64) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if got := subscriber.LatestSeq(); got >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("subscriber sequence = %d, want at least %d", subscriber.LatestSeq(), want)
}

func waitForResilienceLeader(t *testing.T, leaders []*ha.DynamicLeader, excluded *ha.DynamicLeader) *ha.DynamicLeader {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, candidate := range leaders {
			if candidate != excluded && candidate.IsLeader() {
				return candidate
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no active leader elected before timeout")
	return nil
}

func TestResilienceLeaderFailover(t *testing.T) {
	port := freePort(t)
	ns := newResilienceNATSServer(t, port, t.TempDir())
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	const (
		size        = 3
		clusterName = "resilience_leader_failover"
	)
	leaders := make([]*ha.DynamicLeader, 0, size)
	connections := make([]*nats.Conn, 0, size)
	cancels := make([]context.CancelFunc, 0, size)
	for i := range size {
		nc := resilienceNATSConn(t, ns.ClientURL())
		connections = append(connections, nc)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		leader, err := ha.StartLeaderElection(ctx, fmt.Sprintf("127.0.0.1:%d", 5100+i), nc,
			clusterName, size, 1, filepath.Join(t.TempDir(), fmt.Sprintf("election-%d.log", i)))
		if err != nil {
			t.Fatalf("start leader election for node %d: %v", i, err)
		}
		leaders = append(leaders, leader)
		cancels = append(cancels, cancel)
	}

	firstLeader := waitForResilienceLeader(t, leaders, nil)
	firstIndex := -1
	for i, candidate := range leaders {
		if candidate == firstLeader {
			firstIndex = i
			break
		}
	}
	connections[firstIndex].Close()
	secondLeader := waitForResilienceLeader(t, leaders, firstLeader)
	if secondLeader == firstLeader {
		t.Fatal("leadership did not move after the leader's NATS connection was lost")
	}
	for _, cancel := range cancels {
		cancel()
	}
}

func TestResilienceNATSDisconnectAndReconnect(t *testing.T) {
	port := freePort(t)
	storeDir := t.TempDir()
	ns := newResilienceNATSServer(t, port, storeDir)
	serverURL := ns.ClientURL()
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	producerConn := resilienceNATSConn(t, serverURL)
	publisher := resilienceStream(t, producerConn, "resilience_disconnect", "resilience.disconnect")
	node := newResilienceNode(t, producerConn, filepath.Join(t.TempDir(), "node.db"),
		"node-a", "node-a", "resilience_disconnect", "resilience.disconnect")

	ns.Shutdown()
	ns.WaitForShutdown()
	deadline := time.Now().Add(5 * time.Second)
	for node.conn.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if node.conn.IsConnected() {
		t.Fatal("NATS client remained connected after server shutdown")
	}

	ns = newResilienceNATSServer(t, port, storeDir)
	if ns.ClientURL() != serverURL {
		t.Fatalf("restarted NATS URL = %q, want %q", ns.ClientURL(), serverURL)
	}
	if err := node.conn.FlushTimeout(10 * time.Second); err != nil {
		t.Fatalf("flush after NATS reconnect: %v", err)
	}
	if !node.conn.IsConnected() {
		t.Fatal("NATS client did not reconnect")
	}

	if err := publisher.Publish(resilienceChangeSet("resilience_disconnect", 1)); err != nil {
		t.Fatalf("publish after NATS reconnect: %v", err)
	}
	waitForSequence(t, node.subscriber, 1)
	var count int
	if err := node.db.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("count events after NATS reconnect: %v", err)
	}
	if count != 1 {
		t.Fatalf("applied events = %d, want 1 after reconnect", count)
	}
}

func TestResilienceDuplicateMessagesAreDeduplicated(t *testing.T) {
	port := freePort(t)
	ns := newResilienceNATSServer(t, port, t.TempDir())
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	nc := resilienceNATSConn(t, ns.ClientURL())
	const (
		stream  = "resilience_duplicate"
		subject = "resilience.duplicate"
	)
	publisher := resilienceStream(t, nc, stream, subject)
	node := newResilienceNode(t, nc, filepath.Join(t.TempDir(), "node.db"),
		"node-a", "node-a", stream, subject)
	changeset := resilienceChangeSet(stream, 7)
	for range 2 {
		if err := publisher.Publish(changeset); err != nil {
			t.Fatalf("publish duplicate changeset: %v", err)
		}
	}

	waitForSequence(t, node.subscriber, 1)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("create JetStream client: %v", err)
	}
	streamInfo, err := js.Stream(context.Background(), stream)
	if err != nil {
		t.Fatalf("get replication stream: %v", err)
	}
	info, err := streamInfo.Info(context.Background())
	if err != nil {
		t.Fatalf("get replication stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("stream contains %d messages after duplicate publish, want 1", info.State.Msgs)
	}
	var count int
	if err := node.db.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("count events after duplicate delivery: %v", err)
	}
	if count != 1 {
		t.Fatalf("applied events = %d after duplicate publish, want 1", count)
	}
}

func TestResilienceNodeRestartAndOfflineRecovery(t *testing.T) {
	port := freePort(t)
	ns := newResilienceNATSServer(t, port, t.TempDir())
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	nc := resilienceNATSConn(t, ns.ClientURL())
	const (
		stream  = "resilience_restart"
		subject = "resilience.restart"
	)
	publisher := resilienceStream(t, nc, stream, subject)
	dbPath := filepath.Join(t.TempDir(), "node.db")
	node := newResilienceNode(t, nc, dbPath, "node-a", "node-a", stream, subject)

	if err := publisher.Publish(resilienceChangeSet(stream, 1)); err != nil {
		t.Fatalf("publish before node restart: %v", err)
	}
	waitForSequence(t, node.subscriber, 1)
	node.conn.Close()
	if err := node.db.Close(); err != nil {
		t.Fatalf("close node database: %v", err)
	}

	restarted := newResilienceNode(t, nc, dbPath, "node-a", "node-a", stream, subject)
	if got := restarted.subscriber.LatestSeq(); got != 1 {
		t.Fatalf("restarted subscriber sequence = %d, want persisted sequence 1", got)
	}
	if err := publisher.Publish(resilienceChangeSet(stream, 2)); err != nil {
		t.Fatalf("publish after node restart: %v", err)
	}
	waitForSequence(t, restarted.subscriber, 2)

	restarted.conn.Close()
	if err := restarted.db.Close(); err != nil {
		t.Fatalf("close node database before offline interval: %v", err)
	}
	for id := 3; id <= 5; id++ {
		if err := publisher.Publish(resilienceChangeSet(stream, id)); err != nil {
			t.Fatalf("publish message %d while node is offline: %v", id, err)
		}
	}

	recovered := newResilienceNode(t, nc, dbPath, "node-a", "node-a", stream, subject)
	waitForSequence(t, recovered.subscriber, 5)
	var count int
	if err := recovered.db.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil {
		t.Fatalf("count events after offline recovery: %v", err)
	}
	if count != 5 {
		t.Fatalf("applied events after offline recovery = %d, want 5", count)
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
