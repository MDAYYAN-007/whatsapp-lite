package auth

import (
	"errors"
	"sync"
)

// UserStore struct that defines the interface for user storage and retrieval
type UserStore interface {
	CreateUser(username, hashedPassword string) error
	GetUser(username string) (string, error)
}

// InMemoryStore that stores usename and hashed password in memory using a map
type InMemoryStore struct {
	users map[string]string
	mu    sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		users: make(map[string]string),
	}
}

func (s *InMemoryStore) CreateUser(username, hashedPassword string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return errors.New("user already exists")
	}

	s.users[username] = hashedPassword
	return nil
}

func (s *InMemoryStore) GetUser(username string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	password, exists := s.users[username]
	if !exists {
		return "", errors.New("user not found")
	}

	return password, nil
}
