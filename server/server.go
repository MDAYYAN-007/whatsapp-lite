package server

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/MDAYYAN-007/whatsapp-lite/client"
	"github.com/MDAYYAN-007/whatsapp-lite/models"
	"github.com/MDAYYAN-007/whatsapp-lite/room"
	"github.com/gorilla/websocket"
)

// Server struct to manage HTTP server and chat rooms
type Server struct {
	httpServer *http.Server
	rooms      map[string]*room.Room
	mu         sync.Mutex
}

// Constructor to initialize a new Server instance
func NewServer() *Server {
	s := &Server{
		rooms: make(map[string]*room.Room),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWebSocket)

	s.httpServer = &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	log.Println("Server initialized")

	return s
}

// Function to start the HTTP server
func (s *Server) Start() error {
	log.Println("Listening on :8080")
	return s.httpServer.ListenAndServe()
}

// Function to gracefully shutdown the server and all rooms in it
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down HTTP server")

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	log.Printf("Shutting down %d active rooms\n", len(s.rooms))

	for name, r := range s.rooms {
		log.Println("Stopping room:", name)
		r.Stop()
	}
	s.mu.Unlock()

	log.Println("Server shutdown complete")
	return nil
}

// WebSocket upgrader used to upgrade HTTP connections to WebSocket connections
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// Handler that updates HTTP connections to WebSocket, creates a new client and manages room joining
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	log.Println("Incoming new Websocket connection")

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	// Getting the room name drom query params
	roomName := r.URL.Query().Get("room")
	if roomName == "" {
		roomName = "general"
	}
	log.Println("Client requested room:", roomName)

	room := s.getRoom(roomName)

	// Getting the username from query params
	username := r.URL.Query().Get("username")
	if username == "" {
		username = "Anonymous"
	}
	log.Println("Client username:", username)

	c := &client.Client{
		ID:       conn.RemoteAddr().String(),
		Username: username,
		Conn:     conn,
		Send:     make(chan []byte, 500),
	}

	log.Printf("Client connected: %s (%s)\n", username, c.ID)

	room.Join <- c

	room.Broadcast <- models.Message{
		SenderID: "system",
		Content:  []byte(username + " joined the room"),
	}

	go c.WriteGo()
	go c.ReadGo(room.Broadcast, room.Leave)
}

// This function retrieves an existing room or creates a new room if it doesnt exist
func (s *Server) getRoom(name string) *room.Room {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, exists := s.rooms[name]
	if !exists {
		log.Println("Creating new room:", name)
		r = room.NewRoom()
		s.rooms[name] = r
		go r.Start()
	} else {
		log.Println("Using existing room:", name)
	}
	return r
}
