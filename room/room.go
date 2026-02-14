package room

import (
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/client"
	"github.com/MDAYYAN-007/whatsapp-lite/models"
)

// Room struct containing all necesssary componenets
type Room struct {
	Clients   map[*client.Client]bool // Active clients in this room
	Broadcast chan models.Message     // Incoming messages to broadcast
	Join      chan *client.Client     // Clients joining the room
	Leave     chan *client.Client     // Clients leaving the room
	Quit      chan struct{}           // Signal to stop the room
}

// Constructor to initialize a new Room instance
func NewRoom() *Room {
	return &Room{
		Clients:   make(map[*client.Client]bool),
		Broadcast: make(chan models.Message),
		Join:      make(chan *client.Client),
		Leave:     make(chan *client.Client),
		Quit:      make(chan struct{}),
	}
}

// Single goroutine that manages the room operations
func (r *Room) Start() {
	log.Println("Room started")

	for {
		select {

		// Handling graceful shutdown of the room
		case <-r.Quit:
			log.Println("Room shutting down")
			// Close all client channels to signal them to stop
			for c := range r.Clients {
				close(c.Send)
			}
			log.Println("Room stopped")
			return

		// Add new client to the room
		case c := <-r.Join:
			r.Clients[c] = true
			log.Printf("Client joined: %s | Total clients: %d\n", c.Username, len(r.Clients))

		// Handle client leaving the room
		case c := <-r.Leave:
			if _, ok := r.Clients[c]; ok {
				delete(r.Clients, c)
				close(c.Send)
				log.Printf("Client left: %s | Total clients: %d\n", c.Username, len(r.Clients))
			}

			// Send a message that a user has left the room
			r.Broadcast <- models.Message{
				SenderID: "system",
				Content:  []byte(c.Username + " left the room"),
			}

		// Incoming message to broadcast
		case msg := <-r.Broadcast:

			var senderUsername string

			// Fetch sender name
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

			// Add sender name to the message
			formatted := []byte("[" + senderUsername + "]: " + string(msg.Content))

			// Send message to all clients except sender
			for c := range r.Clients {
				if c.ID == msg.SenderID {
					continue
				}
				select {
				// If sender is not blocked, send the message
				case c.Send <- formatted:
				// Client is slow or blocked, remove from room to prevent room blocking
				default:
					log.Printf("Removing slow client: %s\n", c.Username)
					close(c.Send)
					delete(r.Clients, c)
				}
			}
		}
	}
}

// Function used to send a shutdown signal to the room goroutine.
func (r *Room) Stop() {
	close(r.Quit)
}
