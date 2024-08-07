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
	Blocking(ctx context.Context) error
	NonBlocking(ctx context.Context)
	Wait(ctx context.Context) (*pgconn.Notification, error)
	NotificationChannel() chan *pgconn.Notification
	ErrorChannel() chan error
	Close(ctx context.Context) error
}

type notifier struct {
	channel         string
	conn            *pgxpool.Conn
	notifierChannel chan *pgconn.Notification
	mu              sync.Mutex
	cancel          chan struct{}
	errorChannel    chan error
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
		cancel:          make(chan struct{}, 1),
		errorChannel:    make(chan error),
	}, nil
}

func (n *notifier) Listen(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.conn.Conn().Exec(ctx, "LISTEN "+n.channel)
	return err
}

func (n *notifier) Blocking(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		select {
		case <-n.cancel:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			notification, err := n.conn.Conn().WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			n.notifierChannel <- notification
		}
	}
}

func (n *notifier) NonBlocking(ctx context.Context) {
	go func() {
		err := n.Blocking(ctx)
		n.errorChannel <- err
	}()
}

func (n *notifier) UnListen(ctx context.Context) error {
	n.cancel <- struct{}{}
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.conn.Conn().Exec(ctx, "UNLISTEN "+n.channel)
	return err
}

func (n *notifier) Wait(ctx context.Context) (*pgconn.Notification, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.conn.Conn().WaitForNotification(ctx)
}

func (n *notifier) NotificationChannel() chan *pgconn.Notification {
	return n.notifierChannel
}

func (n *notifier) ErrorChannel() chan error {
	return n.errorChannel
}

func (n *notifier) Close(ctx context.Context) error {

	if err := n.UnListen(ctx); err != nil {
		return err
	}

	close(n.notifierChannel)
	close(n.cancel)
	close(n.errorChannel)

	if err := n.conn.Conn().Close(ctx); err != nil {
		return err
	}

	n.conn.Release()
	return nil

}
