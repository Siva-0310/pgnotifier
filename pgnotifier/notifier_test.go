package pgnotifier

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

func setupTestNotifier(t *testing.T) (Notifier, *pgxpool.Pool, func()) {
	ctx := context.Background()

	// Replace with your PostgreSQL connection string
	connString := "postgres://user:password@localhost:5432/alerts"

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}

	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}

	// Cleanup function
	cleanup := func() {
		notifier.Close(ctx)
		pool.Close()
	}

	return notifier, pool, cleanup
}

func TestNotifierBlocking(t *testing.T) {
	notifier, pool, cleanup := setupTestNotifier(t)
	defer cleanup()

	// Test Listen
	t.Log("Calling Listen")
	err := notifier.Listen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	t.Log("Listen succeeded")

	// Test Blocking
	t.Log("Starting Blocking in a goroutine")
	go func() {
		err := notifier.Blocking(context.Background())
		if err != nil {
			t.Errorf("Blocking failed: %v", err)
		}
		t.Log("Blocking finished")
	}()

	// Simulate sending a notification to the channel
	t.Log("Sending notification to the channel")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
	if err != nil {
		t.Fatalf("Sending notification failed: %v", err)
	}

	t.Log("Waiting for notification")
	select {
	case notification := <-notifier.NotificationChannel():
		assert.Equal(t, "test_payload", notification.Payload)
		t.Log("Received notification successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for notification")
	}

	// Test UnListen
	t.Log("Calling UnListen")
	err = notifier.UnListen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("UnListen failed: %v", err)
	}
	t.Log("UnListen succeeded")
}

func TestNotifierNonBlocking(t *testing.T) {
	notifier, pool, cleanup := setupTestNotifier(t)
	defer cleanup()

	// Test Listen
	t.Log("Calling Listen")
	err := notifier.Listen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	t.Log("Listen succeeded")

	// Test NonBlocking
	t.Log("Starting NonBlocking")
	notifier.NonBlocking(context.Background())
	t.Log("NonBlocking started")

	// Simulate sending a notification to the channel
	t.Log("Sending notification to the channel")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
	if err != nil {
		t.Fatalf("Sending notification for non-blocking failed: %v", err)
	}

	t.Log("Waiting for non-blocking error")
	select {
	case notification := <-notifier.NotificationChannel():
		assert.Equal(t, "test_payload", notification.Payload)
		t.Log("Received notification successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for notification")
	}

	// Test UnListen
	t.Log("Calling UnListen")
	err = notifier.UnListen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("UnListen failed: %v", err)
	}
	t.Log("UnListen succeeded")
}
