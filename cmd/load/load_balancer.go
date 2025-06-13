package main

import (
	"consistenthash/consistenthash"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type LoadBalancer struct {
	ch *consistenthash.ConsistentHash
}

func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		ch: consistenthash.New(),
	}
}

func (lb *LoadBalancer) AddServer(name string, capacity int) error {
	return lb.ch.AddWeightedNode(name, capacity)
}
func (lb *LoadBalancer) RemoveServer(name string) error {
	return lb.ch.RemoveNode(name)
}

func (lb *LoadBalancer) RouteRequest(sessionID string) (string, error) {
	return lb.ch.GetNode(sessionID)
}

func (lb *LoadBalancer) GetBackupServers(sessionID string, count int) ([]string, error) {
	return lb.ch.GetNodes(sessionID, count)
}

func main() {
	lb := NewLoadBalancer()

	servers := map[string]int{
		"backend-1": 1,
		"backend-2": 2,
		"backend-3": 1,
	}

	for server, capacity := range servers {
		if err := lb.AddServer(server, capacity); err != nil {
			log.Fatal(err)
		}
	}

	// simulating user sessions
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Load Balancer Session Routing:")
	sessionStats := make(map[string]int)

	for i := 0; i < 20; i++ {
		sessionID := fmt.Sprintf("session_%d", rand.Intn(1000))

		server, err := lb.RouteRequest(sessionID)
		if err != nil {
			log.Printf("Error routing session %s: %v", sessionID, err)
			continue
		}

		sessionStats[server]++
		fmt.Printf("Session %s -> %s\n", sessionID, server)
	}

	fmt.Println("\nServer load distribution:")
	for server, requests := range sessionStats {
		fmt.Printf("%s: %d requests\n", server, requests)
	}

	// failover
	fmt.Println("Failover example")
	sessionId := "critical_session_123"
	backups, err := lb.GetBackupServers(sessionId, 3)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Session %s failover chain: \n", sessionId)
	for i, backup := range backups {
		fmt.Printf("%d. %s\n", i + 1, backup)
	}
}
