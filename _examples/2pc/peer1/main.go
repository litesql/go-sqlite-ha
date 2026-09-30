package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log/slog"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/litesql/go-ha"
	sqlv1 "github.com/litesql/go-ha/api/sql/v1"
	sqliteha "github.com/litesql/go-sqlite-ha"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Two Phase Commit example
func main() {

	slog.SetLogLoggerLevel(slog.LevelDebug)
	c, err := sqliteha.NewConnector("file:_examples/2pc/peer1/my.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&2pcPeers=http://localhost:6002&2pcTimeout=2s",
		ha.WithGrpcPort(6001),
		ha.WithName("node_two_phase_commit_1"),
		ha.WithReplicationSubscriber(ha.NewNoopSubscriber()),
		ha.WithAutoStart(true))
	if err != nil {
		panic(err)
	}
	defer c.Close()

	chErr := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go checkPeerConn(ctx, "http://localhost:6002", "", chErr)
	err = <-chErr
	if err != nil {
		panic(err)
	}

	db := sql.OpenDB(c)
	defer db.Close()

	_, err = db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS users(name TEXT);
		INSERT INTO users VALUES('HA user 2PC peer1');
	`)
	if err != nil {
		slog.Error("Failed to create table and insert data", "error", err)
	} else {
		slog.Info("Table created and data inserted successfully")
	}

	slog.Info("Press CTRL+C to exit")
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
}

func checkPeerConn(ctx context.Context, remote, token string, chErr chan error) {
	u, err := url.Parse(remote)
	if err != nil {
		chErr <- err
	}

	var dialOpts []grpc.DialOption

	if strings.HasPrefix(remote, "http://") {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}
	if token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(grpcCredentials{token: token}))
	}

	cc, err := grpc.NewClient(u.Host, dialOpts...)
	if err != nil {
		chErr <- err
		return
	}
	defer cc.Close()

	for {
		select {
		case <-ctx.Done():
			chErr <- ctx.Err()
			return
		case <-time.Tick(1 * time.Second):
			slog.Debug("checking peer", "remote", remote)
			client, err := sqlv1.NewDatabaseServiceClient(cc).ChangeSet(ctx)
			if err != nil {
				continue
			}
			defer client.CloseSend()

			err = client.Send(&sqlv1.ChangeSetRequest{
				Type: sqlv1.CangeSetRequestType_CHANGESET_REQUEST_TYPE_PING,
			})
			if err != nil {
				chErr <- err
				return
			}

			resp, err := client.Recv()
			if err != nil {
				chErr <- err
				return
			}

			if resp.Error != "" {
				chErr <- fmt.Errorf("ping error: %w", err)
				return
			}

			chErr <- nil
		}
	}
}

type grpcCredentials struct {
	token string
}

func (c grpcCredentials) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": c.token,
	}, nil
}

func (c grpcCredentials) RequireTransportSecurity() bool {
	return false
}
