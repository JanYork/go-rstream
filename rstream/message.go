package rstream

import (
	"encoding/json"
	"time"
)

type Message struct {
	ID      string    `json:"id"`
	Payload []byte    `json:"payload"`
	Created time.Time `json:"created"`
}

func (m Message) String() string {
	payload := string(m.Payload)
	return m.ID + " " + payload + " " + m.Created.String()
}

func (m Message) Bytes() []byte {
	payload := string(m.Payload)
	return []byte(m.ID + " " + payload + " " + m.Created.String())
}

func (m Message) JSON() (string, error) {
	marshal, err := json.Marshal(m)

	if err != nil {
		return "", err
	}

	return string(marshal), nil
}

func (m Message) Map() map[string]interface{} {
	return map[string]interface{}{
		"id":      m.ID,
		"payload": m.Payload,
		"created": m.Created,
	}
}

func (m Message) IsZero() bool {
	return m.ID == "" && len(m.Payload) == 0 && m.Created.IsZero()
}
