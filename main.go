package main

import (
	"fmt"
	"log"
	"os"

	gde "github.com/joho/godotenv"
)

func main() {
	fmt.Println("Hello world!")

	err := gde.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	dbURL := os.Getenv("DB_URL")
	fmt.Println(dbURL)
}
