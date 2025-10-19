package node

import (
	"encoding/json"
	"time"
)

type MessageType int

const (
	JoinMsg MessageType = iota
	JoinAckMsg
	HeartbeatMsg
)

type Message struct {
	Type MessageType	`json:"type"`
	SenderAddr string	`json:"sender_addr"`
	Timestamp time.Time	`json:"timestamp"`
	Payload []byte		`json:"payload"`
}

func NewMessage(msgType MessageType, senderAddr string, payload []byte) *Message {
	return &Message{
		Type: msgType,
		SenderAddr: senderAddr,
		Timestamp: time.Now(),
		Payload: payload,
	}
}

func (m *Message) Serialize() ([]byte, error) {
	return json.Marshal(m)
}

func DeserializeMessage(data []byte) (*Message, error) {
	var message Message;
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, err
	}
	return &message, nil
}