package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

func (n *Node) Serve(addr string) {
	service := &RPCService{node: n}

	server := rpc.NewServer()
	if err := server.RegisterName("RPCService", service); err != nil {
		log.Fatalf("RPC registration failure: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultDebugPath, server)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listening failure on %s: %v", addr, err)
	}

	log.Printf("[%s] listen on %s", n.id, addr)
	go http.Serve(listener, mux)
}
