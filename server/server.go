package server

import (
	"log"
	"net/http"

	"github.com/MDAYYAN-007/whatsapp-lite/client"
	"github.com/MDAYYAN-007/whatsapp-lite/room"
	"github.com/gorilla/websocket"
)

type Server struct {
	rooms map[string]*room.Room
}

func NewServer() *Server {
	return &Server{
		rooms: make(map[string]*room.Room),
	}
}

func (s *Server) Start() error {
	http.HandleFunc("/ws", s.handleWebSocket)

	log.Println("Listening on :8080")
	return http.ListenAndServe(":8080", nil)
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

	c := &client.Client{
		ID:   conn.RemoteAddr().String(),
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	room.Join <- c

	go c.WriteGo()
	go c.ReadGo(room.Broadcast, room.Leave)
}

func (s *Server) getRoom(name string) *room.Room {
	r, exists := s.rooms[name]
	if !exists {
		r = room.NewRoom()
		s.rooms[name] = r
		go r.Start()
	}
	return r
}
