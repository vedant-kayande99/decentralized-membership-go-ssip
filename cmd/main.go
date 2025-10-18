package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vedant-kayande99/decentralized-membership-go-ssip/pkg/node"
)


func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Address to listen on")
	peer := flag.String("peer", "", "Address of peer to send messages to")
	flag.Parse()

	n, err := node.NewNode(*addr)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create Node: %v", err)		
	} 

	n.Start()
	defer n.Stop()

	done := make(chan struct{})
	if *peer != "" {
		go func() {
			ticker := time.NewTicker(2*time.Second)
			for {
				select {
				case <-ticker.C:
					err := n.SendMessage(*peer, "Hello from "+*addr)
					if err != nil {
						log.Printf("[ERROR] Failed to send message: %v", err)
					}
				case <-done:
					return
				}			
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	close(done)
	
	log.Println("Shutting Down...")
}