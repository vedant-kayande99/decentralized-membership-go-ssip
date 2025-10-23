package node

import (
	"encoding/json"
	"time"
)

type MessageType int

const (
	JoinMsg MessageType = iota
	JoinAckMsg	
	GossipMsg
	PingMsg
	PingAckMsg
)

type Message struct {
	Type MessageType			`json:"type"`
	SenderAddr string			`json:"sender_addr"`
	Timestamp time.Time			`json:"timestamp"`
	Payload []byte				`json:"payload"`
	VectorClock map[string]int	`json:"vector_clock"`
}

func NewMessage(msgType MessageType, senderAddr string, payload []byte, clock map[string]int) *Message {
	return &Message{
		Type: msgType,
		SenderAddr: senderAddr,
		Timestamp: time.Now(),
		Payload: payload,
		VectorClock: clock,
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