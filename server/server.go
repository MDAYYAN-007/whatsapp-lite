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

type Server struct {
	httpServer *http.Server
	rooms      map[string]*room.Room
	mu         sync.Mutex
}

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

	return s
}

func (s *Server) Start() error {
	log.Println("Listening on :8080")
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down HTTP server")

	if err := s.httpServer.Shutdown(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	for _, r := range s.rooms {
		r.Stop()
	}
	s.mu.Unlock()

	log.Println("Server shutdown complete")
	return nil
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	roomName := r.URL.Query().Get("room")
	if roomName == "" {
		roomName = "general"
	}
	room := s.getRoom(roomName)

	username := r.URL.Query().Get("username")
	if username == "" {
		username = "Anonymous"
	}

	c := &client.Client{
		ID:       conn.RemoteAddr().String(),
		Username: username,
		Conn:     conn,
		Send:     make(chan []byte, 500),
	}

	room.Join <- c

	room.Broadcast <- models.Message{
		SenderID: "system",
		Content:  []byte(username + " joined the room"),
	}
	go c.WriteGo()
	go c.ReadGo(room.Broadcast, room.Leave)
}

func (s *Server) getRoom(name string) *room.Room {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, exists := s.rooms[name]
	if !exists {
		r = room.NewRoom()
		s.rooms[name] = r
		go r.Start()
	}
	return r
}
