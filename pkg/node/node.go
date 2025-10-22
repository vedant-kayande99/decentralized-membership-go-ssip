package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Node struct {
	addr string
	conn *net.UDPConn
	memberList *MemberList
	shutdown chan struct{}
	wg sync.WaitGroup	
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
		shutdown: make(chan struct{}),
	}, nil
}

func (n *Node) Start() {
	n.wg.Add(3)
	go n.listen()
	go n.heartBeatLoop()
	go n.gossipLoop()
	log.Printf("Node started at %s", n.addr)
}

func (n *Node) Stop() {
	close(n.shutdown)
	n.conn.Close()
	n.wg.Wait()
	log.Printf("Node %s stopped.", n.addr)		
}

func (n *Node) GetMemberList() *MemberList{
	return n.memberList
}

func (n *Node) listen() {
	defer n.wg.Done()
	buffer := make([]byte, 1024)
	for {
		numBytes, _, err := n.conn.ReadFromUDP(buffer)
		if err != nil {
			select {
			case <-n.shutdown:
				return
			default:
				log.Printf("[ERROR] Failed to read from UDP: %v", err)
			}
			continue
		}
		var msg Message
		if err := json.Unmarshal(buffer[:numBytes], &msg); err != nil {
			log.Printf("[ERROR] Failed to deserialize message: %v", err)
			continue
		}

		log.Printf("[INFO] New message received")
		go n.handleMessage(&msg)
	}
}

func (n *Node) heartBeatLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(1*time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			peer, err := n.memberList.GetRandomPeer()
			if err != nil {
				continue
			}

			msg := NewMessage(HeartbeatMsg, n.addr, nil)
			if err := n.SendMessage(peer.Addr, msg); err != nil {
				log.Printf("[ERROR] Failed to send heartbeat to %s: %v", peer.Addr, err)
			}
		case <-n.shutdown:
			return
		}
	}
}

func (n *Node) gossipLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(2*time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.memberList.IncrementHeartbeat()
			peer, err := n.memberList.GetRandomPeer()
			if err != nil {
				continue
			}

			members := n.memberList.GetMembers()
			membersBytes, err := json.Marshal(members)
			if err != nil {
				log.Printf("[ERROR] Failed to encode memebers list for gossip: %v", err)
			}
			
			msg := NewMessage(GossipMsg, n.addr, membersBytes)
			if err := n.SendMessage(peer.Addr, msg); err != nil {
				log.Printf("[ERROR] Failed to send gossip message to %s from %s: %v", peer.Addr, n.addr, err)
			}
		case <-n.shutdown:
			return
		}
	}
}

func (n *Node) handleMessage(msg *Message) {
	log.Printf("Message Received from: %s with message type: %v", msg.SenderAddr, msg.Type)

	switch msg.Type {
	case JoinMsg:
		n.handleJoin(msg)
	case JoinAckMsg:
		n.handleJoinAck(msg)
	case HeartbeatMsg:
		n.handleHeartbeat(msg)
	case GossipMsg:
		n.handleGossip(msg)
	default:
		log.Printf("[WARN] Unknown message type: %v", msg.Type)
	}
}

func (n *Node) handleJoin(msg *Message) {
	log.Printf("[INFO] Received Join request from %s", msg.SenderAddr)
	// add the new node to the member list
	member := &Member{Addr: msg.SenderAddr, Status: Alive, HeartbeatCounter: 0}
	n.memberList.Add(member)

	// send back a JoinAck message with the complete updated member list
	members := n.memberList.GetMembers()
	membersBytes, err := json.Marshal(members)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal/serialize member list for JoinAck: %v", err)
		return
	}
	
	ackMsg := NewMessage(JoinAckMsg, n.addr, membersBytes)
	if err := n.SendMessage(msg.SenderAddr, ackMsg); err != nil {
		log.Printf("[ERROR] Failed to send JoinAck message to %s: %v", msg.SenderAddr, err)
	}
}

func (n *Node) handleJoinAck(msg *Message) {
	log.Printf("[INFO] Received JoinAck request from %s", msg.SenderAddr)

	var members []Member
	if err := json.Unmarshal(msg.Payload, &members); err != nil {
		log.Printf("[ERROR] Failed to deserialize message payload in JoinAck message: %v", err)
		return
	}
		
	n.memberList.SyncMembers(members)
}

func (n *Node) handleGossip(msg *Message) {	
	var receivedMembers []Member
	if err := json.Unmarshal(msg.Payload, &receivedMembers); err != nil {
		log.Printf("[ERROR] Failed to deserialize gossip message with membership list from %s: %v", msg.SenderAddr, err)
		return
	}

	n.memberList.Merge(receivedMembers)
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