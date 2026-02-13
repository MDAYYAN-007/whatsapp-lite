package main

import (
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/server"
)

func main() {
	srv := server.NewServer()

	log.Println("Server starting on :8080")

	if err := srv.Start(); err != nil {
		log.Fatal("Server error:", err)
	}
}
