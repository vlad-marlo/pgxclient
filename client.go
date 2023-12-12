package client

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"os"
	"strings"
	"testing"
	"time"
)

type Client struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

// NewTest prepares test client.
//
// If error occurred while creating connection then test will be skipped.
// Second argument cleanup function to close connection and rollback all changes.
func NewTest(t testing.TB) (*Client, func()) {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), os.Getenv("TEST_DB_URI"))
	if err != nil {
		t.Skipf("can not create pool: %v", err)
	}

	cli := &Client{
		pool: pool,
		log:  zap.L(),
	}

	if err = TryWithAttemptsCtx(context.Background(), pool.Ping, 5, 200*time.Millisecond); err != nil {
		t.Skipf("can not get access to db: %v", err)
	}
	return cli, func() {
		teardown(cli.pool)()
	}
}

func BadCli(t testing.TB) *Client {
	t.Helper()

	pool, err := pgxpool.New(context.Background(), "postgresql://postgres:postgres@localhost:4321/unknown_db")
	if err != nil {
		t.Skipf("can not create pool: %v", err)
	}

	cli := &Client{
		pool: pool,
		log:  zap.L(),
	}

	if err = TryWithAttemptsCtx(context.Background(), pool.Ping, 5, 200*time.Millisecond); err == nil {
		t.Skip("must have no connection to database")
	}
	return cli
}

// teardown return func for defer it to clear tables.
//
// Always pass one or more tables in it.
func teardown(pool *pgxpool.Pool, tables ...string) func() {
	return func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("TRUNCATE %s CASCADE;", strings.Join(tables, ", ")))
		pool.Close()
	}
}

// L return global client logger.
//
// If client is nil object then global logger will be returned.
func (cli *Client) L() *zap.Logger {
	if cli == nil {
		zap.L().Error("unexpectedly got nil client dereference")
		return zap.L()
	}
	return cli.log
}

// P returns client's configured logger.
//
// If client is nil object then will be returned nil pool.
func (cli *Client) P() *pgxpool.Pool {
	if cli == nil {
		zap.L().Error("unexpectedly got nil client dereference")
		return nil
	}
	return cli.pool
}
