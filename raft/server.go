package raft

import (
	"encoding/json"
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
	mux.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var body struct{ Op, Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "JSON invalid", http.StatusBadRequest)
			return
		}

		index, term, isLeader := n.Start(body.Op, body.Key, body.Value)
		w.Header().Set("Content-Type", "application/json")
		if !isLeader {
			_, _, _, _, _, _, leaderID := n.Status()
			w.WriteHeader(http.StatusMisdirectedRequest)
			json.NewEncoder(w).Encode(map[string]any{
				"error":        "not leader",
				"known_leader": leaderID,
			})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": true,
			"index":    index,
			"term":     term,
		})
	})
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		val, ok := n.Get(key)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"key":   key,
			"value": val,
			"found": ok,
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		id, state, term, logLen, commitIndex, snapshotIndex, leaderID := n.Status()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":             id,
			"state":          state.String(),
			"term":           term,
			"log_length":     logLen,
			"commit_index":   commitIndex,
			"snapshot_index": snapshotIndex,
			"known_leader":   leaderID,
		})
	})

	mux.Handle(rpc.DefaultRPCPath, server)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listening failure on %s: %v", addr, err)
	}

	log.Printf("[%s] listen on %s", n.id, addr)
	go func() {
		_ = http.Serve(listener, mux)
	}()
}
