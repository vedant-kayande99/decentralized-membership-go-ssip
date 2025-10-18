package node

import (
	"fmt"
	"log"
	"net"
	"time"
)

type Node struct {
	addr *net.UDPAddr
	conn *net.UDPConn
	shutdown chan struct{}
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
		addr: addr,
		conn: conn,
		shutdown: make(chan struct{}),
	}, nil
}

func (node *Node) Start() {
	go node.listen()
}

func (node *Node) Stop() {
	close(node.shutdown)
	node.conn.Close()
}

func (node *Node) listen() {
	buffer := make([]byte, 1024)
	for {
		select {
		case <- node.shutdown:
			return
		default:
			node.conn.SetReadDeadline(time.Now().Add(1*time.Second))
			len, remoteAddr, err := node.conn.ReadFromUDP(buffer)
			if err != nil {
				if !err.(net.Error).Timeout() {
					log.Printf("[ERROR] Failed to read from UDP: %v", err)
				}
				continue
			}			
			message := string(buffer[:len])
			log.Printf("Received message from %v: %s", remoteAddr, message)
		}		
	}
}

func (node *Node) SendMessage(targetAddr string, message string) error {
	addr, err := net.ResolveUDPAddr("udp", targetAddr)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to resolve target address: %v", err)
	}

	_, err = node.conn.WriteToUDP([]byte(message), addr)
	if err != nil {
		return fmt.Errorf("[ERROR] Failed to send message: %v", err)
	}

	return nil
}