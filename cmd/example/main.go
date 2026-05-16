package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/segfaultscribe/conduit/pkg/consumer"
	"github.com/segfaultscribe/conduit/pkg/event"
	"github.com/segfaultscribe/conduit/pkg/sink"
)

// Define a simple struct that matches your contract
type DebugSink struct{}

// Ensure it satisfies the interface at compile time
var _ sink.Sink = (*DebugSink)(nil)

func (d *DebugSink) Connect(ctx context.Context) error {
	log.Println("DebugSink: Connected to destination successfully.")
	return nil
}

func (d *DebugSink) Publish(ctx context.Context, ev *event.ChangeEvent) error {
	fmt.Printf(" [CDC EVENT] Op: %s | Table: %s.%s | Data: %v\n", ev.Operation, ev.Schema, ev.Table, ev.After)
	return nil
}

func (d *DebugSink) Close() error {
	log.Println("DebugSink: Flushed buffers and closed cleanly.")
	return nil
}

func main() {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?replication=database"
	}

	// Instantiate your managed consumer framework
	cdcConsumer := consumer.New(dbURL, "conduit_checkpoint.bin", &DebugSink{})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Println("Starting Conduit CDC Engine...")
	if err := cdcConsumer.Start(ctx); err != nil {
		log.Fatalf("Engine stopped with error: %v", err)
	}
}
