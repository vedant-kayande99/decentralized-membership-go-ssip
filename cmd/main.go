package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/vedant-kayande99/decentralized-membership-go-ssip/pkg/node"
)


func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "Address to listen on")
	peer := flag.String("peer", "", "Address of peer to join cluster")
	pprofAddr := flag.String("pprof", ":6060", "pprof http server address")

	flag.Parse()

	go func() {
        log.Printf("Starting pprof server on %s", *pprofAddr)
        if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
            log.Printf("pprof server failed: %v", err)
        }
    }()

	n, err := node.NewNode(*addr)
	if err != nil {
		log.Fatalf("[ERROR] Failed to create Node: %v", err)		
	} 

	n.Start()
	defer n.Stop()
	
	// join cluster if peer is specified
	if *peer != "" {
		joinMsg := node.NewMessage(node.JoinMsg, *addr, nil, nil)
		if err := n.SendMessage(*peer, joinMsg); err != nil {
			log.Printf("[ERROR] Failed to send join message: %v", err)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	
	log.Println("Shutting Down...")
}