package client

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/MDAYYAN-007/whatsapp-lite/internal/models"
	"github.com/gorilla/websocket"
)

// Client struct representing a single connected client
type Client struct {
	ID          string
	Username    string
	CurrentRoom string
	Conn        *websocket.Conn
	Send        chan []byte
}

// This function reads messages from the WebSocket connection and sends them to the room's broadcast channel
func (c *Client) ReadGo(serverMessage chan<- models.Message, disconnect chan<- *Client) {
	defer c.Conn.Close()

	for {
		_, data, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected:", c.ID)
			disconnect <- c
			return
		}

		var incoming models.Message
		if err := json.Unmarshal(data, &incoming); err != nil {
			log.Println("Invalid JSON from client")
			continue
		}

		if incoming.Type == "chat" && strings.TrimSpace(incoming.Content) == "" {
			continue
		}

		incoming.Username = c.Username
		incoming.Timestamp = time.Now().UTC().Format(time.RFC3339)

		// Forward to server for routing decision
		serverMessage <- incoming
	}
}

// This function listens on the Send channel of client and writes messages to the WebSocket connection
func (c *Client) WriteGo() {
	defer c.Conn.Close()

	for msg := range c.Send {
		if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			break
		}
	}
}
