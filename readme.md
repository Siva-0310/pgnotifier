# `pgnotifier`

The `pgnotifier` package provides a Go interface for handling PostgreSQL notifications. It offers a simple way to subscribe to, receive, and manage notifications from a PostgreSQL database. This package supports both blocking and non-blocking modes for notification handling.

## Features

- **Subscribe to Notifications:** Easily listen to notifications on specific channels.
- **Real-Time Handling:** Receive notifications synchronously (blocking) or asynchronously (non-blocking).
- **Graceful Shutdown:** Cleanly stop notifications and release resources.
- **Error Handling:** Receive errors related to notifications through dedicated channels.

## Installation

To install the `pgnotifier` package, run:

```sh
go get github.com/Siva-0310/pgnotifier
```

## Usage

### Create a New Notifier

First, set up a PostgreSQL connection pool and create a new notifier instance.

```go
import (
    "context"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/Siva-0310/pgnotifier"
)

func main() {
    ctx := context.Background()

    // Set up PostgreSQL connection pool
    pool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/alerts")
    if err != nil {
        panic(err)
    }
    defer pool.Close()

    // Create a new notifier
    notifier, err := pgnotifier.New(ctx, pool)
    if err != nil {
        panic(err)
    }
    defer notifier.Close(ctx, "test_channel")
}
```

### Subscribe to Notifications

To start listening to notifications on a specific topic:

```go
err := notifier.Listen(ctx, "test_channel")
if err != nil {
    panic(err)
}
```

### Receiving Notifications

**Blocking Mode**

Blocks until a notification is received or the context is canceled.

```go
err := notifier.Blocking(ctx)
if err != nil {
    panic(err)
}
```

**Non-Blocking Mode**

Starts a goroutine to listen for notifications without blocking the main thread.

```go
notifier.NonBlocking(ctx)

select {
case notification := <-notifier.NotificationChannel():
    fmt.Println("Received notification:", notification.Payload)
case err := <-notifier.ErrorChannel():
    fmt.Println("Error:", err)
}
```

### Waiting for a Notification

Waits for a single notification, blocking until one is received or an error occurs.

```go
notification, err := notifier.Wait(ctx)
if err != nil {
    panic(err)
}
fmt.Println("Received notification:", notification.Payload)
```

### Unsubscribing from Notifications

To stop listening to notifications on a specific topic:

```go
err := notifier.UnListen(ctx, "test_channel")
if err != nil {
    panic(err)
}
```

### Closing the Notifier

Closes the notifier, unsubscribing from all topics and releasing resources.

```go
err := notifier.Close(ctx, "test_channel")
if err != nil {
    panic(err)
}
```

## Interface Methods

### `Notifier`

- **`Listen(ctx context.Context, topic string) error`**: Subscribes to notifications on the specified topic.
- **`UnListen(ctx context.Context, topic string) error`**: Unsubscribes from notifications on the specified topic.
- **`Blocking(ctx context.Context) error`**: Blocks until a notification is received or the context is canceled.
- **`NonBlocking(ctx context.Context)`**: Starts a goroutine for non-blocking notification listening.
- **`Wait(ctx context.Context) (*pgconn.Notification, error)`**: Waits for a single notification.
- **`NotificationChannel() chan *pgconn.Notification`**: Returns a channel for receiving notifications.
- **`ErrorChannel() chan error`**: Returns a channel for receiving errors.
- **`StopBlocking()`**: Stops the blocking operation.
- **`Close(ctx context.Context, topic string) error`**: Closes the notifier, releasing resources.

## Example

Here's a complete example demonstrating how to use the `pgnotifier` package:

```go
package main

import (
    "context"
    "fmt"
    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/Siva-0310/pgnotifier"
    "log"
)

func main() {
    ctx := context.Background()

    // Set up PostgreSQL connection pool
    pool, err := pgxpool.New(ctx, "postgres://user:password@localhost:5432/alerts")
    if err != nil {
        log.Fatalf("Unable to connect to database: %v", err)
    }
    defer pool.Close()

    // Create a new notifier
    notifier, err := pgnotifier.New(ctx, pool)
    if err != nil {
        log.Fatalf("Failed to create notifier: %v", err)
    }
    defer notifier.Close(ctx, "test_channel")

    // Subscribe to a notification topic
    err = notifier.Listen(ctx, "test_channel")
    if err != nil {
        log.Fatalf("Failed to listen to test_channel: %v", err)
    }

    // Start non-blocking notification listening
    notifier.NonBlocking(ctx)

    // Send a notification
    _, err = pool.Exec(ctx, "NOTIFY test_channel, 'test_payload'")
    if err != nil {
        log.Fatalf("Sending notification failed: %v", err)
    }

    // Wait for the notification or an error
    select {
    case notification := <-notifier.NotificationChannel():
        fmt.Println("Notification received:", notification.Payload)
    case err := <-notifier.ErrorChannel():
        log.Fatalf("Received error: %v", err)
    case <-ctx.Done():
        log.Fatalf("Timed out waiting for notification")
    }

    // Clean up
    err = notifier.UnListen(ctx, "test_channel")
    if err != nil {
        log.Fatalf("UnListen failed: %v", err)
    }
}
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
