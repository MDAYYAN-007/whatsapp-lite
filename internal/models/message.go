package models

type Message struct {
	Type      string `json:"type"`
	Room      string `json:"room"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	To        string `json:"to,omitempty"`
	Timestamp string `json:"timestamp"`
}
