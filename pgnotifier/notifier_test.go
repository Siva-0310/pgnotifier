package pgnotifier

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connString := "postgresql://user:password@localhost:5432/alerts"
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		t.Fatalf("Unable to connect to database: %v", err)
	}
	defer pool.Close()

	listener, err := New(ctx, "coins_update", pool)
	if err != nil {
		t.Fatalf("Failed to create listener: %v", err)
	}
	defer listener.Close() // Ensure the listener is closed after the test

	notificationChannel := listener.NotificationChannel()

	// Start listening in blocking mode
	listener.Listen(ctx)
	go listener.Blocking(ctx)

	_, err = pool.Exec(ctx, "NOTIFY coins_update, 'test_payload'")
	if err != nil {
		t.Fatalf("Failed to send notification: %v", err)
	}

	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 10*time.Second)
	defer timeoutCancel()

	// Wait for notification
	select {
	case notification := <-notificationChannel:
		if notification == nil {
			t.Fatal("Expected notification but got nil")
		}
		if notification.Payload != "test_payload" {
			t.Errorf("Expected payload 'test_payload', got '%s'", notification.Payload)
		}
		fmt.Println(notification.Payload)
	case <-timeoutCtx.Done():
		t.Fatal("Test timed out waiting for notification")
	}

	fmt.Println("Test completed successfully")
	cancel()
}
