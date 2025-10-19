package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
)

type Node struct {
	addr string
	conn *net.UDPConn
	memberList *MemberList
	running bool	
}

func NewNode(address string) (*Node, error) {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] failed to resolve address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("[ERROR] failed to listen on address: %v", err)
	}

	return &Node{
		addr: address,
		conn: conn,
		memberList: NewMemberList(address),
		running: false,		
	}, nil
}

func (n *Node) Start() {
	n.running = true
	go n.listen()
}

func (n *Node) Stop() {	
	n.running = false
	n.conn.Close()
}

func (n *Node) GetMemberList() *MemberList{
	return n.memberList
}

func (n *Node) listen() {
	buffer := make([]byte, 1024)
	for n.running {
		numBytes, _, err := n.conn.ReadFromUDP(buffer)
		if err != nil {
			if n.running {
				log.Printf("[ERROR] Failed to read UDP message: %v", err)
			}
			continue
		}
		log.Printf("[INFO] New message received")
		go n.handleMessage(buffer[:numBytes])
	}
}

func (n *Node) handleMessage(data []byte) {
	msg, err := DeserializeMessage(data)
	if err != nil {
		log.Printf("[ERROR] Failed to deserialize message: %v", err)
		return		
	}
	log.Printf("Message Received from: %s with message type: %v", msg.SenderAddr, msg.Type)
	log.Printf("Membership List: %v",n.memberList.members)

	switch msg.Type {
	case JoinMsg:
		n.handleJoin(msg)
	case JoinAckMsg:
		n.handleJoinAck(msg)
	case HeartbeatMsg:
		n.handleHeartbeat(msg)
	default:
		log.Printf("[WARN] Unknown message type: %v", msg.Type)
	}
}

func (n *Node) handleJoin(msg *Message) {
	// add the new node to the member list
	n.memberList.Add(msg.SenderAddr)

	// send back a JoinAck message with the complete updated member list
	members := n.memberList.GetMembers()
	membersBytes, err := json.Marshal(members)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal/serialize member list: %v", err)
		return
	}
	
	ackMsg := NewMessage(JoinAckMsg, n.addr, membersBytes)
	n.SendMessage(msg.SenderAddr, ackMsg)
}

func (n *Node) handleJoinAck(msg *Message) {
	var members []Member
	if err := json.Unmarshal(msg.Payload, &members); err != nil {
		log.Printf("[ERROR] Failed to deserialize message payload in JoinAck message: %v", err)
		return
	}
		
	n.memberList.SyncMembers(members)
}

func (n *Node) handleHeartbeat(msg *Message) {
	n.memberList.UpdateStatus(msg.SenderAddr, Alive)
}

func (n *Node) SendMessage(targetAddr string, message *Message) error {
	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to resolve target address: %v", err)
	}

	data, err := message.Serialize()
	if err != nil {
		return err
	}

	_, err = n.conn.WriteToUDP(data, addr)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to send message: %v", err)
	}

	return nil
}