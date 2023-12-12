package fx

import (
	"context"
	"fmt"
	pgxzap "github.com/jackc/pgx-zap"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	pgxUUID "github.com/vgarvardt/pgx-google-uuid/v5"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"time"
)

type Client struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

const (
	RetryAttempts = 4
	RetryDelay    = 2 * time.Second
)

// New opens new postgres connection, configures it and return prepared client.
func New(lc fx.Lifecycle, uri string, log *zap.Logger) (*Client, error) {
	var pool *pgxpool.Pool

	c, err := pgxpool.ParseConfig(
		uri,
	)
	if err != nil {
		return nil, fmt.Errorf("error while parsing db uri: %w", err)
	}

	c.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxUUID.Register(conn.TypeMap())
		return nil
	}

	var lvl = tracelog.LogLevelError
	c.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   pgxzap.NewLogger(log),
		LogLevel: lvl,
	}

	pool, err = pgxpool.NewWithConfig(context.Background(), c)
	if err != nil {
		return nil, fmt.Errorf("postgres: init pgxpool: %w", err)
	}

	cli := &Client{
		pool: pool,
		log:  log,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return TryWithAttemptsCtx(ctx, pool.Ping, RetryAttempts, RetryDelay)
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})
	log.Info("created postgres client")
	return cli, nil
}

// TryWithAttempts tries to get non-error result of calling function f with delay.
func TryWithAttempts(f func() error, attempts uint, delay time.Duration) (err error) {
	err = f()
	if err == nil {
		return nil
	}

	for i := uint(1); i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}
		zap.L().Warn("got error in attempter", zap.Uint("attempts", i+1), zap.NamedError("error", err))
		time.Sleep(delay)
	}
	return err
}

// TryWithAttemptsCtx is helper function that calls TryWithAttempts with function f transformed to closure that does not
// require ctx as necessary argument.
func TryWithAttemptsCtx(ctx context.Context, f func(context.Context) error, attempts uint, delay time.Duration) (err error) {
	return TryWithAttempts(func() error { return f(ctx) }, attempts, delay)
}
