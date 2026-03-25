package main

import (
	"context"
	"log"
	"os"

	gde "github.com/joho/godotenv"
	"github.com/segfaultscribe/conduit/internal/checkpoint"
	"github.com/segfaultscribe/conduit/internal/consumer"
	"github.com/segfaultscribe/conduit/internal/event"
)

func main() {
	err := gde.Load()
	if err != nil {
		log.Println("Error loading .env file:")
		os.Exit(1)
	}
	// get the database URL from the environment file
	dbURL := os.Getenv("DB_URL")
	ctx := context.Background()

	cp := checkpoint.New("checkpoint.txt")
	Consumer := consumer.New(
		dbURL,
		cp,
		handleEventChange,
	)

	Consumer.Start(
		ctx,
	)
}

func handleEventChange(ctx context.Context, event *event.ChangeEvent) error {
	return nil
}
