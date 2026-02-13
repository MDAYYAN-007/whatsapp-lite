package client

import (
	"log"

	"github.com/gorilla/websocket"
)

type Client struct {
	Conn *websocket.Conn
	Send chan []byte
}

func (c *Client) ReadGo() {
	defer c.Conn.Close()

	for {
		_, msg, err := c.Conn.ReadMessage()

		if err != nil {
			log.Println("Client disconnected")
			break
		}

		log.Println("Received:", string(msg))
		c.Send <- msg
	}
}

func (c *Client) WriteGo() {
	defer c.Conn.Close()

	for msg := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}
	}
}
