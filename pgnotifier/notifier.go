package pgnotifier

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Notifier interface {
	Listen(ctx context.Context) error
	UnListen(ctx context.Context) error
	Blocking(ctx context.Context)
	NonBlocking(ctx context.Context)
	Wait(ctx context.Context) (*pgconn.Notification, error)
	NotificationChannel() chan *pgconn.Notification
	Close() error
}

type notifier struct {
	channel         string
	conn            *pgxpool.Conn
	notifierChannel chan *pgconn.Notification
	mu              sync.Mutex
	cancel          chan struct{}
}

func New(ctx context.Context, channel string, pool *pgxpool.Pool) (Notifier, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &notifier{
		channel:         channel,
		conn:            conn,
		notifierChannel: make(chan *pgconn.Notification),
		mu:              sync.Mutex{},
	}, nil
}

func (n *notifier) Listen(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.conn.Conn().Exec(ctx, "LISTEN "+n.channel)
	return err
}

func (n *notifier) Blocking(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		select {
		case <-n.cancel:
			return
		case <-ctx.Done():
			return
		default:
			notification, err := n.conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				panic(err)
			}
			n.notifierChannel <- notification
		}
	}
}

func (n *notifier) NonBlocking(ctx context.Context) {
	go n.Blocking(ctx)
}

func (n *notifier) UnListen(ctx context.Context) error {
	// implementation goes here
	return nil
}

func (n *notifier) Wait(ctx context.Context) (*pgconn.Notification, error) {
	// implementation goes here
	return nil, nil
}

func (n *notifier) NotificationChannel() chan *pgconn.Notification {
	return n.notifierChannel
}

func (n *notifier) Close() error {
	// implementation goes here
	return nil
}
