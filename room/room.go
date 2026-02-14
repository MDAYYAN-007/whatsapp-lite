package room

import (
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/client"
	"github.com/MDAYYAN-007/whatsapp-lite/models"
)

type Room struct {
	Clients   map[*client.Client]bool
	Broadcast chan models.Message
	Join      chan *client.Client
	Leave     chan *client.Client
	Quit      chan struct{}
}

func NewRoom() *Room {
	return &Room{
		Clients:   make(map[*client.Client]bool),
		Broadcast: make(chan models.Message),
		Join:      make(chan *client.Client),
		Leave:     make(chan *client.Client),
		Quit:      make(chan struct{}),
	}
}

func (r *Room) Start() {
	for {
		select {

		case <-r.Quit:
			log.Println("Room shutting down")
			for c := range r.Clients {
				close(c.Send)
			}
			return

		case c := <-r.Join:
			r.Clients[c] = true

		case c := <-r.Leave:
			if _, ok := r.Clients[c]; ok {
				delete(r.Clients, c)
				close(c.Send)
			}
			r.Broadcast <- models.Message{
				SenderID: "system",
				Content:  []byte(c.Username + " left the room"),
			}

		case msg := <-r.Broadcast:

			var senderUsername string

			if msg.SenderID == "system" {
				senderUsername = "System"
			} else {
				for c := range r.Clients {
					if c.ID == msg.SenderID {
						senderUsername = c.Username
						break
					}
				}
			}

			formatted := []byte("[" + senderUsername + "]: " + string(msg.Content))

			for c := range r.Clients {

				if c.ID == msg.SenderID {
					continue
				}

				select {
				case c.Send <- formatted:
				default:
					close(c.Send)
					delete(r.Clients, c)
				}
			}
		}
	}
}

func (r *Room) Stop() {
	close(r.Quit)
}
