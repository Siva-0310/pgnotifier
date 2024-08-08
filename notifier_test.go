package pgnotifier

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// setUpPgxpool sets up the PostgreSQL connection pool.
func setUpPgxpool(t *testing.T) *pgxpool.Pool {
	ctx := context.Background()

	// Replace with your PostgreSQL connection string
	connString := "postgres://user:password@localhost:5432/alerts"

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}

	return pool
}

func TestNotifier(t *testing.T) {
	// Set up PostgreSQL connection pool
	t.Log("Setting up PostgreSQL connection pool")
	pool := setUpPgxpool(t)
	defer pool.Close()

	// Create a new notifier
	t.Log("Creating a new notifier")
	notifier, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("Failed to create notifier: %v", err)
	}

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe to a notification topic
	t.Log("Subscribing to notification topic 'test_channel'")
	err = notifier.Listen(ctx, "test_channel")
	if err != nil {
		t.Fatalf("Failed to listen to test_channel: %v", err)
	}

	// Start non-blocking notification listening
	t.Log("Starting non-blocking notification listening")
	notifier.NonBlocking(ctx)

	// Send a notification
	t.Log("Sending a notification to 'test_channel'")
	_, err = pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
	if err != nil {
		t.Fatalf("Sending notification failed: %v", err)
	}

	// Wait for the notification or an error
	t.Log("Waiting for notification or error")
	select {
	case notification := <-notifier.NotificationChannel():
		t.Log("Notification received")
		if notification.Payload != "test_payload" {
			t.Errorf("Expected payload 'test_payload', got '%s'", notification.Payload)
		} else {
			t.Log("Received notification successfully")
		}
	case err := <-notifier.ErrorChannel():
		t.Log("Error received")
		if err != nil {
			t.Errorf("Received error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Timed out waiting for notification")
	}

	// Stop the blocking operation (if any)
	notifier.StopBlocking()

	// Test sending another notification after some delay
	t.Log("Testing delayed notification sending")
	errchan := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Second) // Simulate a delay before sending the second notification
		t.Log("Sending second notification to 'test_channel'")
		_, err := pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
		if err != nil {
			errchan <- err
		} else {
			errchan <- nil
		}
		t.Log("Second notification to 'test_channel' was sent")
	}()

	// Wait for the second notification with a timeout context
	t.Log("Waiting for second notification with timeout context")
	timeOutCtx, cancelTimeout := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelTimeout()

	notification, err := notifier.Wait(timeOutCtx)
	if err != nil {
		t.Log("Checking for errors in delayed notification sending")
		if timeOutCtx.Err() == context.DeadlineExceeded {
			// Check for errors in sending the notification
			if sendErr := <-errchan; sendErr != nil {
				t.Fatalf("Error sending notification: %v", sendErr)
			}
			t.Fatalf("Timed out waiting for second notification")
		} else {
			t.Fatalf("Error waiting for second notification: %v", err)
		}
	} else {
		t.Log("Second notification received")
		if notification.Payload != "test_payload" {
			t.Errorf("Expected payload 'test_payload' for second notification, got '%s'", notification.Payload)
		} else {
			t.Log("Received second notification successfully")
		}
	}

	// Unsubscribe from the notification topic
	t.Log("Calling UnListen")
	err = notifier.UnListen(context.Background(), "test_channel")
	if err != nil {
		t.Fatalf("UnListen failed: %v", err)
	}
	t.Log("UnListen succeeded")

	// Close the notifier and clean up resources
	t.Log("Calling Close method on notifier")
	err = notifier.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close notifier: %v", err)
	}
	t.Log("Notifier closed successfully")
}
