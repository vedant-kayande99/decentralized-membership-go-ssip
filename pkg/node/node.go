package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const (
	PingTimeout = 1 * time.Second
	IndirectPingCount = 3
	FailureCheckInterval = 2 * time.Second
)

type Node struct {
	addr string
	conn *net.UDPConn
	memberList *MemberList
	shutdown chan struct{}
	wg sync.WaitGroup	
	pingTable sync.Map
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
	go n.gossipLoop()
	go n.failureDetectorLoop()
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
		
		go n.handleMessage(&msg)
	}
}

func (n *Node) failureDetectorLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(FailureCheckInterval)
	defer ticker.Stop()

	for {		
		select {
		case <-ticker.C:
			peer, err := n.memberList.GetRandomPeer()
			if err != nil {
				continue
			}
			pingMsg := NewMessage(PingMsg, n.addr, nil, nil)
			log.Printf("[DEBUG] Sending ping to %s", peer.Addr)

			respChan := make(chan struct{})
			n.pingTable.Store(peer.Addr, respChan)

			if err := n.SendMessage(peer.Addr, pingMsg); err != nil {
				log.Printf("[ERROR] Failed to send ping to %s: %v", peer.Addr, err)
				n.pingTable.Delete(peer.Addr)
				continue
			}

			// go routine to wait for resp from peer node with timeout
			go func(peerAddr string, ch chan struct{}) {
				select {
				case <-ch:
					n.memberList.UpdateStatus(peerAddr, Alive)
					log.Printf("[DEBUG] Received ack from %s", peerAddr)
				case <-time.After(PingTimeout):
					log.Printf("[WARN] No direct response from %s, starting indirect probing", peerAddr)					
					n.startIndirectProbing(peerAddr)
				}
				n.pingTable.Delete(peerAddr)
			}(peer.Addr, respChan)

		case <-n.shutdown:
			return
		}
	}
}

func (n *Node) startIndirectProbing(peerAddr string) {
	peers, err := n.memberList.GetRandomPeers(IndirectPingCount, peerAddr)
	if err != nil {
		if err == ErrNoPeers {			
			n.memberList.UpdateStatus(peerAddr, Suspect)
			log.Printf("[WARN] No peers available for indirect probing marking %s as suspect", peerAddr)
			return
		}
		log.Printf("[ERROR] Failed to get peers for indirect probing: %v", err)
		return
	}

	indirectRespChan := make(chan struct{})
	n.pingTable.Store(peerAddr+"_indirect", indirectRespChan)

	for _, peer := range peers {
		payload, err := json.Marshal(map[string]string{"target":peerAddr})
		if err != nil {
			log.Printf("[ERROR] Faied to create payload for PingReqMsg: %v", err)
			continue
		}
		msg := NewMessage(PingReqMsg, n.addr, payload, nil)

		if err := n.SendMessage(peer.Addr, msg); err != nil {
			log.Printf("[ERROR] Failed to send PingReqMsg to %s: %v", peer.Addr, err)
			continue
		}
	}

	go func(peerAddr string, ch chan struct{}) {
		select {
		case <-ch:
			n.memberList.UpdateStatus(peerAddr, Alive)
			log.Printf("[INFO] Node %s confirmed alive through indirect probing", peerAddr)
		case <-time.After(PingTimeout):
			n.memberList.UpdateStatus(peerAddr, Suspect)
			log.Printf("[WARN] Node %s marked as suspect after failed indirect probing", peerAddr)
		}
		n.pingTable.Delete(peerAddr + "_indirect")
	}(peerAddr, indirectRespChan)
}

func (n *Node) gossipLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(10*time.Second)
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
			
			clock := n.memberList.GetVectorClock()
			msg := NewMessage(GossipMsg, n.addr, membersBytes, clock)
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
	case GossipMsg:
		n.handleGossip(msg)
	case PingMsg:
		n.handlePing(msg)
	case PingAckMsg:
		n.handlePingAck(msg)
	case PingReqMsg:
		n.handlePingReq(msg)
	case PingReqAckMsg:
		n.handlePingReqAck(msg)
	default:
		log.Printf("[WARN] Unknown message type: %v", msg.Type)
	}
}

func (n *Node) handleJoin(msg *Message) {
	log.Printf("[INFO] Received Join request from %s", msg.SenderAddr)
	// add the new node to the member list
	member := &Member{Addr: msg.SenderAddr, Status: Alive, HeartbeatCounter: 0}
	if existingMember, exists := n.memberList.GetMember(msg.SenderAddr); exists {
		member.HeartbeatCounter = existingMember.HeartbeatCounter + 1
	}
	
	n.memberList.Add(member)

	// send back a JoinAck message with the complete updated member list
	members := n.memberList.GetMembers()
	membersBytes, err := json.Marshal(members)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal/serialize member list for JoinAck: %v", err)
		return
	}
	
	ackMsg := NewMessage(JoinAckMsg, n.addr, membersBytes, n.memberList.GetVectorClock())
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

	// merge incomming gossip
	n.memberList.Merge(receivedMembers)

	// check if there is new info to send back
	delta := n.memberList.GetDelta(msg.VectorClock)
	if len(delta) > 0 {
		log.Printf("[INFO] Sending back a delta of size %d to %s", len(delta), msg.SenderAddr)
		deltaBytes, err := json.Marshal(delta)
		if err != nil {
			log.Printf("[ERROR] Failed to serialize/marshal delta info for gossip response: %v", err)
			return
		}

		responseClock := n.memberList.GetVectorClock()
		responseMsg := NewMessage(GossipMsg, n.addr, deltaBytes, responseClock)
		if err := n.SendMessage(msg.SenderAddr, responseMsg); err != nil {
			log.Printf("[ERROR] Failed to send gossip delta to %s: %v", msg.SenderAddr, err)
		}
	}
}

func (n *Node) handlePing(msg *Message) {	
		ackMsg := NewMessage(PingAckMsg, n.addr, msg.Payload, nil)
		if err := n.SendMessage(msg.SenderAddr, ackMsg); err != nil {
			log.Printf("[ERROR] Failed to send a ping ack to %s: %v", msg.SenderAddr, err)
		}	
}

func (n *Node) handlePingAck(msg *Message) {
	// no payload: this is response to direct ping
	if msg.Payload == nil {
		if ch, ok := n.pingTable.LoadAndDelete(msg.SenderAddr); ok {
			respChan := ch.(chan struct{})
			close(respChan)
		}
		return
	}

	// payload with target addr: this is response to an indirect ping (ping_req)
	if ch, ok := n.pingTable.LoadAndDelete(msg.SenderAddr + "_ping_req"); ok {
		respChan := ch.(chan struct{})
		close(respChan)
	}	
}

func (n *Node) handlePingReq(msg *Message) {
	var metadata map[string]string
	if err := json.Unmarshal(msg.Payload, &metadata); err != nil {
		log.Printf("[ERROR] Failed to deserialize message payload in PingReq message: %v", err)
		return
	}

	targetAddr := metadata["target"]
	respChan := make(chan struct{})
	n.pingTable.Store(targetAddr + "_ping_req", respChan)

	pingMsg := NewMessage(PingMsg, n.addr, msg.Payload, nil)
	if err := n.SendMessage(targetAddr, pingMsg); err != nil {
		log.Printf("[ERROR] Failed to forward ping to %s: %v", targetAddr, err)
		return
	}	

	go func(targetAddr string, senderAddr string, ch chan struct{}) {
		select {
		case <-ch:
			// target responded, inform back to sender			
			payload, err := json.Marshal(map[string]string{"target":targetAddr})
			if err != nil {
				log.Printf("[ERROR] Faied to create payload for PingReqAckMsg: %v", err)
				return				
			}
			ackMsg := NewMessage(PingReqAckMsg, n.addr, payload, nil)
			n.SendMessage(senderAddr, ackMsg)
		
		case <-time.After(PingTimeout):
			log.Printf("[DEBUG] PingReq timeout waiting for response from %s", targetAddr)			
		}
		n.pingTable.Delete(targetAddr + "_ping_req")
	}(targetAddr, msg.SenderAddr, respChan)
}

func (n *Node) handlePingReqAck(msg *Message) {
	var metadata map[string]string
	if err := json.Unmarshal(msg.Payload, &metadata); err != nil {
		log.Printf("[ERROR] Failed to deserialize message payload in PingReq message: %v", err)
		return
	}
	targetAddr := metadata["target"]

	if ch, ok := n.pingTable.LoadAndDelete(targetAddr + "_indirect"); ok {
		respChan := ch.(chan struct{})
		close(respChan)
	}
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