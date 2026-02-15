package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/MDAYYAN-007/whatsapp-lite/internal/auth"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/client"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/models"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/room"
	"github.com/gorilla/websocket"
)

// Server struct to manage HTTP server and chat rooms
type Server struct {
	httpServer  *http.Server
	rooms       map[string]*room.Room
	authService *auth.AuthService
	clients     map[string]*client.Client
	mu          sync.Mutex

	clientMessages   chan models.Message
	clientDisconnect chan *client.Client

	privateHistory map[string][]models.Message
}

// Constructor to initialize a new Server instance
func NewServer() *Server {

	store := auth.NewInMemoryStore()
	authService := auth.NewAuthService(store)

	s := &Server{
		rooms:            make(map[string]*room.Room),
		authService:      authService,
		clients:          make(map[string]*client.Client),
		clientMessages:   make(chan models.Message),
		clientDisconnect: make(chan *client.Client),
		privateHistory:   make(map[string][]models.Message),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/login", s.handleLogin)
	mux.Handle("/me", s.authMiddleware(http.HandlerFunc(s.handleMe)))

	wsHandler := s.authMiddleware(http.HandlerFunc(s.handleWebSocket))
	mux.Handle("/ws", wsHandler)

	s.httpServer = &http.Server{
		Addr:    ":8080",
		Handler: s.corsMiddleware(mux),
	}

	// Start central router goroutine
	go s.router()

	log.Println("Server initialized")
	return s
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:3000")
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
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

// Handler that updates HTTP connections to WebSocket and registers client online
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	log.Println("Incoming new Websocket connection")

	username := r.Context().Value("username").(string)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	log.Printf("Client username: %s", username)

	c := &client.Client{
		ID:       conn.RemoteAddr().String(),
		Username: username,
		Conn:     conn,
		Send:     make(chan []byte, 500),
	}

	log.Printf("Client connected: %s (%s)\n", username, c.ID)

	// Register client as online
	s.mu.Lock()
	s.clients[username] = c
	s.mu.Unlock()

	var userMessages []models.Message

	s.mu.Lock()
	for key, messages := range s.privateHistory {
		parts := strings.Split(key, ":")
		if len(parts) == 2 && (parts[0] == username || parts[1] == username) {
			userMessages = append(userMessages, messages...)
		}
	}
	s.mu.Unlock()

	for _, msg := range userMessages {
		payload, err := json.Marshal(msg)
		if err == nil {
			c.Send <- payload
		}
	}

	go c.WriteGo()
	go c.ReadGo(s.clientMessages, s.clientDisconnect)
}

// Central router handling all client messages and disconnects
func (s *Server) router() {
	for {
		select {

		case msg := <-s.clientMessages:
			s.handleMessage(msg)

		case c := <-s.clientDisconnect:

			log.Println("Client disconnected cleanup:", c.Username)

			s.mu.Lock()
			delete(s.clients, c.Username)
			s.mu.Unlock()

			if c.CurrentRoom != "" {
				room := s.getRoom(c.CurrentRoom)
				room.Leave <- c
			}

			close(c.Send)
		}
	}
}

// Handles message routing based on message type
func (s *Server) handleMessage(msg models.Message) {

	s.mu.Lock()
	c := s.clients[msg.Username]
	s.mu.Unlock()

	if c == nil {
		return
	}

	switch msg.Type {

	case "join":

		if msg.Room == "" {
			return
		}

		// Leave old room if exists
		if c.CurrentRoom != "" {
			oldRoom := s.getRoom(c.CurrentRoom)
			oldRoom.Leave <- c
		}

		newRoom := s.getRoom(msg.Room)
		c.CurrentRoom = msg.Room
		newRoom.Join <- c

	case "leave":

		if c.CurrentRoom != "" {
			room := s.getRoom(c.CurrentRoom)
			room.Leave <- c
			c.CurrentRoom = ""
		}

	case "chat":

		if c.CurrentRoom == "" {
			return
		}

		msg.Room = c.CurrentRoom
		room := s.getRoom(c.CurrentRoom)
		room.Broadcast <- msg
	case "private":

		if msg.Content == "" || msg.Username == msg.To {
			return
		}

		msg.Timestamp = time.Now().UTC().Format(time.RFC3339)

		key := conversationKey(msg.Username, msg.To)
		s.mu.Lock()
		s.privateHistory[key] = append(s.privateHistory[key], msg)
		target := s.clients[msg.To]
		s.mu.Unlock()

		// If user is online, deliver the message
		if target != nil {
			payload, err := json.Marshal(msg)
			if err == nil {
				select {
				case target.Send <- payload:
				default:
				}
			}
		}

	case "typing":

		if msg.Room != "" && c.CurrentRoom == msg.Room {
			room := s.getRoom(msg.Room)
			room.Broadcast <- msg
			return
		}

		if msg.To != "" {
			s.mu.Lock()
			target := s.clients[msg.To]
			s.mu.Unlock()

			if target != nil {
				payload, err := json.Marshal(msg)
				if err == nil {
					select {
					case target.Send <- payload:
					default:
					}
				}
			}
		}

	}
}

// This function retrieves an existing room or creates a new room if it doesnt exist
func (s *Server) getRoom(name string) *room.Room {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, exists := s.rooms[name]
	if !exists {
		log.Println("Creating new room:", name)
		r = room.NewRoom(name)
		s.rooms[name] = r
		go r.Start()
	} else {
		log.Println("Using existing room:", name)
	}
	return r
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		cookie, err := r.Cookie("token")
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		username, err := auth.ValidateToken(cookie.Value)
		if err != nil {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}

		ctx := context.WithValue(r.Context(), "username", username)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	if err := s.authService.Register(req.Username, req.Password); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	token, err := s.authService.Login(req.Username, req.Password)
	if err != nil {
		http.Error(w, "Login after register failed", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	token, err := s.authService.Login(req.Username, req.Password)

	if err != nil {
		http.Error(w, "Invalid credentials", http.StatusUnauthorized)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    token,
		HttpOnly: true,
		Path:     "/",
	})

	w.WriteHeader(http.StatusOK)
}

func conversationKey(a, b string) string {
	if a < b {
		return a + ":" + b
	}
	return b + ":" + a
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	username := r.Context().Value("username").(string)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"username": username,
	})
}
