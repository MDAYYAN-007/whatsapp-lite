package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MDAYYAN-007/whatsapp-lite/server"
)

func main() {

	// Initialize a server
	srv := server.NewServer()

	// Start a server on port 8080 in a separate goroutine
	go func() {
		log.Println("Server starting on :8080")

		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server error:", err)
		}
	}()

	// Channel to listen for OS termination signals
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	<-signalChannel

	log.Println("Received termination signal, initiating graceful shutdown...")

	// Create context with timeout to graceful shutdown
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()

	// Perform graceful shutdown
	if err := srv.Shutdown(ctx); err != nil {
		log.Println("Graceful shutdown failed:", err)
	}

	log.Println("Server exited cleanly")
}
