package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vedant-kayande99/decentralized-membership-go-ssip/pkg/node"
)


func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Address to listen on")
	peer := flag.String("peer", "", "Address of peer to join cluster")
	flag.Parse()

	n, err := node.NewNode(*addr)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create Node: %v", err)		
	} 

	n.Start()
	defer n.Stop()
	
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1*time.Second)
		defer ticker.Stop()		
		randSource := rand.NewSource(time.Now().UnixNano())
		randVal := rand.New(randSource)
		for {
			select {
			case <-ticker.C:
				members := n.GetMemberList().GetMembers()
				numMembers := len(members)
				if numMembers <= 0 {
					continue
				}				
				randIdx := randVal.Intn(numMembers)
				memberAddr := members[randIdx].Addr
				if memberAddr != *addr {
					msg := node.NewMessage(node.HeartbeatMsg, *addr, nil)
					if err := n.SendMessage(memberAddr, msg); err != nil {
						log.Printf("[ERROR] Failed to send heartbeat to %s: %v",memberAddr, err)
					}
				}
			case <-done:
				return				
			}
		}
	}()
	
	// join cluster if peer is specified
	if *peer != "" {
		joinMsg := node.NewMessage(node.JoinMsg, *addr, nil)
		if err := n.SendMessage(*peer, joinMsg); err != nil {
			log.Printf("[ERROR] Failed to send join message: %v", err)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	close(done)
	
	log.Println("Shutting Down...")
}