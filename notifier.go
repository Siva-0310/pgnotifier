package pgnotifier

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Notifier defines the interface for notification handling.
type Notifier interface {
	// Listen subscribes to notifications on the specified topic.
	// It begins listening for notifications on the given topic.
	// Returns an error if the subscription fails.
	Listen(ctx context.Context, topic string) error

	// UnListen unsubscribes from notifications on the specified topic.
	// It stops listening for notifications on the given topic.
	// Returns an error if the unsubscription fails.
	UnListen(ctx context.Context, topic string) error

	// Blocking blocks until a notification is received or the context is canceled.
	// It listens for notifications and sends them to the notifierChannel.
	// Returns an error if one occurs or the context is canceled.
	Blocking(ctx context.Context) error

	// NonBlocking starts a goroutine that listens for notifications in a non-blocking manner.
	// It uses the Blocking method internally and sends any encountered errors to the errorChannel.
	// The method returns immediately after starting the goroutine.
	NonBlocking(ctx context.Context)

	// Wait waits for a single notification.
	// It blocks until a notification is received or an error occurs.
	// Returns the notification or an error if one occurs.
	Wait(ctx context.Context) (*pgconn.Notification, error)

	// NotificationChannel returns a channel for receiving notifications.
	// Notifications are sent to this channel by the Blocking or NonBlocking methods.
	NotificationChannel() chan *pgconn.Notification

	// ErrorChannel returns a channel for receiving errors.
	// Errors encountered during notification handling are sent to this channel.
	ErrorChannel() chan error

	// StopBlocking stops the blocking operation initiated by the Blocking method.
	// It signals cancellation to stop the blocking operation.
	StopBlocking()

	// Close closes the notifier, unsubscribing from all topics and releasing resources.
	// It signals cancellation to stop any running blocking operations, closes channels,
	// and releases the connection back to the pool.
	// Returns an error if any issues occur during the closure process.
	Close(ctx context.Context, topic string) error
}

// notifier implements the Notifier interface for handling PostgreSQL notifications.
type notifier struct {
	mu              sync.Mutex
	wg              sync.WaitGroup
	conn            *pgxpool.Conn
	notifierChannel chan *pgconn.Notification
	cancel          context.CancelFunc
	errorChannel    chan error
}

// New creates a new notifier instance using the provided connection pool.
// It acquires a connection from the pool and initializes the notifier instance.
// Returns the notifier instance and an error if the connection acquisition fails.
func New(ctx context.Context, pool *pgxpool.Pool) (Notifier, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &notifier{
		conn:            conn,
		mu:              sync.Mutex{},
		wg:              sync.WaitGroup{},
		notifierChannel: make(chan *pgconn.Notification),
		errorChannel:    make(chan error, 1),
	}, nil
}

// Listen subscribes to notifications on the specified topic.
// It begins listening for notifications on the given topic.
// Returns an error if the subscription fails.
func (n *notifier) Listen(ctx context.Context, topic string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.conn.Conn().Exec(ctx, "LISTEN "+topic)
	return err
}

// Blocking blocks until a notification is received or the context is canceled.
// Notifications are sent to the notifierChannel, and the method returns
// an error if one occurs or the context is canceled.
func (n *notifier) Blocking(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	for {
		select {
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

// NonBlocking starts a goroutine that listens for notifications in a non-blocking manner.
// Any errors encountered are sent to the errorChannel.
func (n *notifier) NonBlocking(ctx context.Context) {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		err := n.Blocking(ctx)
		n.errorChannel <- err
	}()
}

// UnListen unsubscribes from notifications on the specified topic.
// It also signals cancellation to any running blocking operations.
func (n *notifier) UnListen(ctx context.Context, topic string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	_, err := n.conn.Exec(ctx, "UNLISTEN \""+topic+"\"")
	return err
}

// Wait waits for a single notification. It returns the notification or an error if one occurs.
func (n *notifier) Wait(ctx context.Context) (*pgconn.Notification, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.conn.Conn().WaitForNotification(ctx)
}

// NotificationChannel returns a channel for receiving notifications.
func (n *notifier) NotificationChannel() chan *pgconn.Notification {
	return n.notifierChannel
}

// ErrorChannel returns a channel for receiving errors.
func (n *notifier) ErrorChannel() chan error {
	return n.errorChannel
}

// StopBlocking stops the blocking operation initiated by the Blocking method.
// It signals cancellation to stop the blocking operation.
func (n *notifier) StopBlocking() {
	if n.cancel != nil {
		n.cancel()
	}
}

// Close closes the notifier, unsubscribing from all topics and releasing resources.
// It signals cancellation to stop any running blocking operations, closes channels,
// and releases the connection back to the pool
// Returns an error if any issues occur during the closure process.
func (n *notifier) Close(ctx context.Context, topic string) error {

	n.StopBlocking()
	n.wg.Wait()

	if err := n.UnListen(ctx, topic); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	close(n.notifierChannel)
	close(n.errorChannel)

	if err := n.conn.Conn().Close(ctx); err != nil {
		return err
	}

	n.conn.Release()
	return nil
}
