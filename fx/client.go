package fx

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
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
	log.Info("initializing postgres client with config", zap.Any("cfg", cfg))

	c, err := pgxpool.ParseConfig(
		uri,
	)
	if err != nil {
		return nil, fmt.Errorf("error while parsing db uri: %w", err)
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
			return pgxclient.TryWithAttemptsCtx(ctx, pool.Ping, RetryAttempts, RetryDelay)
		},
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})
	log.Info("created postgres client")
	return cli, nil
}
