package client

import (
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/models"
	"github.com/gorilla/websocket"
)

type Client struct {
	ID       string
	Username string
	Conn     *websocket.Conn
	Send     chan []byte
}

func (c *Client) ReadGo(broadcast chan<- models.Message, leave chan<- *Client) {
	defer c.Conn.Close()

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

func (c *Client) WriteGo() {
	defer c.Conn.Close()

	for msg := range c.Send {
		if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			break
		}
	}
}
