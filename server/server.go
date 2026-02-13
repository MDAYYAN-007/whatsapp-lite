package server

import (
	"log"
	"net/http"

	"github.com/MDAYYAN-007/whatsapp-lite/client"
	"github.com/gorilla/websocket"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
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

	log.Println("Client connected")

	client := &client.Client{
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	go client.WriteGo()
	go client.ReadGo()
}
