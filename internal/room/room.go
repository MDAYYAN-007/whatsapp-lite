package room

import (
	"encoding/json"
	"log"

	"github.com/MDAYYAN-007/whatsapp-lite/internal/client"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/models"
)

// Room struct containing all necesssary componenets of a room
type Room struct {
	Name      string
	Clients   map[*client.Client]bool
	Broadcast chan models.Message
	Join      chan *client.Client
	Leave     chan *client.Client
	Quit      chan struct{}

	History    [][]byte
	MaxHistory int
}

// Constructor to initialize a new Room instance
func NewRoom(name string) *Room {
	return &Room{
		Name:       name,
		Clients:    make(map[*client.Client]bool),
		Broadcast:  make(chan models.Message),
		Join:       make(chan *client.Client),
		Leave:      make(chan *client.Client),
		Quit:       make(chan struct{}),
		History:    make([][]byte, 0),
		MaxHistory: 1000,
	}
}

// Single goroutine that manages the room operations
func (r *Room) Start() {
	log.Printf("Room started: %s\n", r.Name)

	for {
		select {

		// Handling graceful shutdown of the room
		case <-r.Quit:
			log.Println("Room shutting down")
			// Close all client channels to signal them to stop
			for c := range r.Clients {
				close(c.Send)
			}
			log.Printf("Room stopped: %s\n", r.Name)
			return

		// Add new client to the room
		case c := <-r.Join:

			duplicate := false
			for existing := range r.Clients {
				if existing.Username == c.Username {
					duplicate = true
					break
				}
			}

			if duplicate {
				errMsg := models.Message{
					Type:      "error",
					Room:      r.Name,
					Username:  "system",
					Content:   "Username already taken in this room",
					Timestamp: "",
				}

				payload, _ := json.Marshal(errMsg)
				c.Send <- payload
				close(c.Send)
				continue
			}

			r.Clients[c] = true

			// Send message history to new client
			for _, msg := range r.History {
				c.Send <- msg
			}

			joinMsg := models.Message{
				Type:      "join",
				Room:      r.Name,
				Username:  c.Username,
				Content:   "",
				Timestamp: "",
			}

			payload, _ := json.Marshal(joinMsg)

			r.History = append(r.History, payload)
			if len(r.History) > r.MaxHistory {
				r.History = r.History[1:]
			}

			for client := range r.Clients {
				client.Send <- payload
			}

			log.Printf("Client joined: %s | Total clients in room %s: %d\n", c.Username, r.Name, len(r.Clients))

		// Handle client leaving the room
		case c := <-r.Leave:
			if _, ok := r.Clients[c]; ok {
				delete(r.Clients, c)
				close(c.Send)
				log.Printf("Client left: %s | Total clients in room %s: %d\n", c.Username, r.Name, len(r.Clients))

				leaveMsg := models.Message{
					Type:      "leave",
					Room:      r.Name,
					Username:  c.Username,
					Content:   "",
					Timestamp: "",
				}

				payload, _ := json.Marshal(leaveMsg)
				r.History = append(r.History, payload)
				if len(r.History) > r.MaxHistory {
					r.History = r.History[1:]
				}

				for client := range r.Clients {
					client.Send <- payload
				}
			}

		// Incoming message to broadcast
		case msg := <-r.Broadcast:

			payload, err := json.Marshal(msg)
			if err != nil {
				log.Println("Failed to marshal message:", err)
				continue
			}

			// Store message in history
			r.History = append(r.History, payload)
			if len(r.History) > r.MaxHistory {
				r.History = r.History[1:]
			}

			// Send message to all clients except sender
			for c := range r.Clients {
				if msg.Type == "chat" && c.Username == msg.Username {
					continue
				}

				select {
				case c.Send <- payload:
				default:
					log.Printf("Removing slow client: %s from room %s\n", c.Username, r.Name)
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
