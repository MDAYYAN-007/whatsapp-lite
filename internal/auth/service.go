package auth

import (
	"errors"

	"golang.org/x/crypto/bcrypt"
)

// AuthService handles authentication logic
type AuthService struct {
	store UserStore
}

func NewAuthService(store UserStore) *AuthService {
	return &AuthService{
		store: store,
	}
}

func (a *AuthService) Register(username, password string) error {
	if username == "" || password == "" {
		return errors.New("username and password required")
	}

	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	return a.store.CreateUser(username, string(hashed))
}

func (a *AuthService) Login(username, password string) (string, error) {
	hashedPassword, err := a.store.GetUser(username)
	if err != nil {
		return "", err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password)); err != nil {
		return "", errors.New("invalid credentials")
	}

	return GenerateToken(username)
}
