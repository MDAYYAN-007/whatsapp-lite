package client

import (
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/internal/models"
	"github.com/gorilla/websocket"
)

// Client struct representing a single connected client
type Client struct {
	ID       string
	Username string
	Conn     *websocket.Conn
	Send     chan []byte
}

// This function reads messages from the WebSocket connection and sends them to the room's broadcast channel
func (c *Client) ReadGo(broadcast chan<- models.Message, leave chan<- *Client) {
	defer c.Conn.Close()

	// This is like an infinite loop that reads messages
	// If the client disconnects, it will break the loop and trigger the leave process
	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected:", c.ID)
			leave <- c
			break
		}

		broadcast <- models.Message{
			SenderID: c.ID,
			Content:  msg,
		}
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
