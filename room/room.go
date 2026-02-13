package room

import (
	"github.com/MDAYYAN-007/whatsapp-lite/client"
)

type Room struct {
	Clients   map[*client.Client]bool
	Broadcast chan []byte
	Join      chan *client.Client
	Leave     chan *client.Client
}

func NewRoom() *Room {
	return &Room{
		Clients:   make(map[*client.Client]bool),
		Broadcast: make(chan []byte),
		Join:      make(chan *client.Client),
		Leave:     make(chan *client.Client),
	}
}

func (r *Room) Start() {
	for {
		select {

		case c := <-r.Join:
			r.Clients[c] = true

		case c := <-r.Leave:
			if _, ok := r.Clients[c]; ok {
				delete(r.Clients, c)
				close(c.Send)
			}

		case msg := <-r.Broadcast:
			for c := range r.Clients {
				select {
				case c.Send <- msg:
				default:
					close(c.Send)
					delete(r.Clients, c)
				}
			}
		}
	}
}
