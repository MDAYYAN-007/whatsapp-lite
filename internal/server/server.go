package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/MDAYYAN-007/whatsapp-lite/internal/auth"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/client"
	"github.com/MDAYYAN-007/whatsapp-lite/internal/room"
	"github.com/gorilla/websocket"
)

// Server struct to manage HTTP server and chat rooms
type Server struct {
	httpServer  *http.Server
	rooms       map[string]*room.Room
	authService *auth.AuthService
	mu          sync.Mutex
}

// Constructor to initialize a new Server instance
func NewServer() *Server {

	store := auth.NewInMemoryStore()
	authService := auth.NewAuthService(store)
	s := &Server{
		rooms:       make(map[string]*room.Room),
		authService: authService,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/login", s.handleLogin)

	wsHandler := s.authMiddleware(http.HandlerFunc(s.handleWebSocket))
	mux.Handle("/ws", wsHandler)

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

	username := r.Context().Value("username").(string)

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

	room := s.getRoom(roomName)

	log.Printf("Client requested room: %s\n Client username: %s", roomName, username)

	c := &client.Client{
		ID:       conn.RemoteAddr().String(),
		Username: username,
		Conn:     conn,
		Send:     make(chan []byte, 500),
	}

	log.Printf("Client connected: %s (%s)\n", username, c.ID)

	room.Join <- c

	go c.WriteGo()
	go c.ReadGo(room.Broadcast, room.Leave, roomName)
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
