package pgnotifier

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	done := make(chan struct{})
	go func() {
		defer close(done)
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
		if notification.Payload != "test_payload" {
			t.Errorf("Expected payload 'test_payload', got '%s'", notification.Payload)
		} else {
			t.Log("Received notification successfully")
		}
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

	// Wait for the blocking goroutine to finish
	<-done
}

func TestNotifierNonBlocking(t *testing.T) {
	notifier, pool, _ := setupTestNotifier(t)
	defer pool.Close()
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
		if notification.Payload != "test_payload" {
			t.Errorf("Expected payload 'test_payload', got '%s'", notification.Payload)
		} else {
			t.Log("Received notification successfully")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for notification")
	}

	// Test UnListen
	t.Log("Calling Close")
	err = notifier.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	t.Log("Close succeeded")
}

func TestNotifierWait(t *testing.T) {
	notifier, pool, cleanup := setupTestNotifier(t)
	defer cleanup()

	// Test Listen
	t.Log("Calling Listen")
	err := notifier.Listen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("Listen failed: %v", err)
	}
	t.Log("Listen succeeded")

	t.Log("Sending notification to the channel")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	errChan := make(chan error, 1) // Buffer size of 1 to avoid blocking

	go func() {
		defer close(errChan)
		time.Sleep(1 * time.Second) // Adjusted sleep to reduce test duration
		_, err := pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
		if err != nil {
			errChan <- err
		}
	}()

	t.Log("Calling Wait")
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()

	notification, err := notifier.Wait(waitCtx)
	if err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
	if notification == nil {
		t.Fatal("No notification received in Wait")
	}
	if notification.Payload != "test_payload" {
		t.Errorf("Expected payload 'test_payload', got '%s'", notification.Payload)
	} else {
		t.Log("Wait received notification successfully")
	}

	// Check if there was an error in sending notification
	if err = <-errChan; err != nil {
		t.Fatalf("Error sending notification: %v", err)
	}

	t.Log("Calling Close")
	err = notifier.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	t.Log("Close succeeded")
}
