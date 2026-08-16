package main

import (
	"flag"
	"log"
	"raft-lab/raft"
	"strings"
)

func main() {
	id := flag.String("id", "", "unique node identifier (e.g., node1)")
	port := flag.String("port", "", "listening port (e.g., 8001)")
	peers := flag.String("peers", "", "addresses of the other nodes, separated by commas")
	flag.Parse()

	if *id == "" || *port == "" {
		log.Fatal("usage: node -id=node1 -port=8001 -peers=localhost:8002,localhost:8003")
	}

	var peerList []string
	if *peers != "" {
		peerList = strings.Split(*peers, ",")
	}

	node := raft.NewNode(*id, peerList)
	node.Serve(":" + *port)
	node.Run() // locking loop (INCEPTION)
}
