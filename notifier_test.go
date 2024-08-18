package pgnotifier

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var pool *pgxpool.Pool

func createPostgresConn(connString string) *pgxpool.Pool {
	var pool *pgxpool.Pool
	var err error

	for i := 0; i < 5; i++ {
		pool, err = pgxpool.New(context.Background(), connString)
		if err == nil {
			log.Println("Successfully connected to PostgreSQL.")
			return pool
		}

		log.Printf("Failed to connect to PostgreSQL, attempt %d: %v\n", i+1, err)
		time.Sleep(5 * time.Second)
	}

	log.Fatalf("Failed to connect to PostgreSQL after 5 attempts: %v", err)
	return nil
}

func TestNonBlocking(t *testing.T) {
	// Create notifier instance
	ctx := context.Background()
	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}
	defer notifier.Close(ctx, "test_channel")

	t.Log("Notifier created and initialized")

	// Start listening on the channel
	if err := notifier.Listen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to listen on channel: %v", err)
	}
	t.Log("Listening on channel 'test_channel'")

	// Start non-blocking mode
	notifier.NonBlocking(ctx)
	t.Log("Non-blocking mode started")

	time.Sleep(1 * time.Second)

	//Send a notification
	go func() {
		time.Sleep(100 * time.Millisecond) // Ensure the notifier is set up
		_, err := pool.Exec(ctx, "NOTIFY test_channel, 'test payload'")
		if err != nil {
			t.Errorf("Failed to send notification: %v", err)
		} else {
			t.Log("Notification sent to channel 'test_channel'")
		}
	}()

	//Wait for notification
	select {
	case notification := <-notifier.NotificationChannel():
		t.Log("Notification received")
		if notification == nil {
			t.Fatal("Received nil notification")
		}
		if notification.Payload != "test payload" {
			t.Errorf("Expected 'test payload', got '%s'", notification.Payload)
		} else {
			t.Logf("Notification payload verified: '%s'", notification.Payload)
		}
	case err := <-notifier.ErrorChannel():
		t.Fatalf("Error occurred: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for notification")
	}

	t.Log("Test completed")
}

func TestWaitNormal(t *testing.T) {
	ctx := context.Background()
	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}
	defer notifier.Close(ctx, "test_channel")

	// Start listening on the channel
	if err := notifier.Listen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to listen on channel: %v", err)
	}
	t.Log("Listening on channel 'test_channel'")

	// Send a notification
	go func() {
		time.Sleep(100 * time.Millisecond) // Ensure the notifier is set up
		_, err := pool.Exec(ctx, "NOTIFY test_channel, 'test payload'")
		if err != nil {
			t.Errorf("Failed to send notification: %v", err)
		}
	}()

	// Wait for notification
	notification, err := notifier.Wait(ctx)
	if err != nil {
		t.Fatalf("Error occurred: %v", err)
	}
	if notification == nil {
		t.Fatal("Received nil notification")
	}
	if notification.Payload != "test payload" {
		t.Errorf("Expected 'test payload', got '%s'", notification.Payload)
	} else {
		t.Logf("Notification payload verified: '%s'", notification.Payload)
	}

	t.Log("TestWaitNormal completed successfully")
}

func TestCloseWhileWaiting(t *testing.T) {
	ctx := context.Background()
	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}
	defer notifier.Close(ctx, "test_channel")

	// Start listening on the channel
	if err := notifier.Listen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to listen on channel: %v", err)
	}
	t.Log("Listening on channel 'test_channel'")

	// Use a context with a timeout for waiting
	waitCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Wait for notification or handle cancellation
	_, err = notifier.Wait(waitCtx)
	if err == nil {
		t.Fatal("Expected error but got none")
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Unexpected error: %v", err)
	} else {
		t.Logf("Correctly handled the close during wait %v", err)
	}

	t.Log("TestCloseWhileWaiting completed successfully")
}

func TestUnListen(t *testing.T) {
	ctx := context.Background()
	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}
	defer notifier.Close(ctx, "test_channel")

	// Start listening on the channel
	if err := notifier.Listen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to listen on channel: %v", err)
	}
	t.Log("Listening on channel 'test_channel'")

	// Unlisten from the channel
	if err := notifier.UnListen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to unlisten on channel: %v", err)
	}
	t.Log("Unlistened from channel 'test_channel'")
}

func TestExternalContextCancellation(t *testing.T) {
	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the notifier
	notifier, err := New(ctx, pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}
	defer notifier.Close(context.Background(), "test_channel")

	// Start listening on the channel
	if err := notifier.Listen(ctx, "test_channel"); err != nil {
		t.Fatalf("Failed to listen on channel: %v", err)
	}
	t.Log("Listening on channel 'test_channel'")

	// Start non-blocking mode
	notifier.NonBlocking(ctx)
	t.Log("Non-blocking mode started")

	//Simulate some delay before canceling the context
	time.Sleep(2 * time.Second)
	cancel()
	//Wait for some time to ensure NonBlocking operation has time to respond to cancellation
	err = <-notifier.ErrorChannel()
	if err == nil {
		t.Fatal("Expected error from ErrorChannel but got none")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Unexpected error: %v", err)
	}
	t.Logf("Error from ErrorChannel as expected: %v", err)

	t.Log("TestExternalContextCancellation completed successfully")
}

func TestMain(m *testing.M) {
	pool = createPostgresConn("postgres://user:password@localhost:5432/alerts")
	defer pool.Close()
	m.Run()
}
