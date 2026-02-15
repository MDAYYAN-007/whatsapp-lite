package models

type Message struct {
	Type      string `json:"type"`
	Room      string `json:"room"`
	Username  string `json:"username"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
}
