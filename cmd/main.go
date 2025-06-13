package main

import (
	"consistenthash/consistenthash"
	"fmt"
	"log"
)

func main() {
	config := &consistenthash.Config{
		BaseReplicas: 50,
	}

	ch := consistenthash.NewWithConfig(config)

	// add weighted nodes
	if err := ch.AddWeightedNode("small-server", 1); err != nil {
		log.Fatal(err)
	}
	if err := ch.AddWeightedNode("medium-server", 2); err != nil {
		log.Fatal(err)
	}
	if err := ch.AddWeightedNode("large-server", 4); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Consistent Hash ring example")
	ch.PrintRing()

	testKeys := []string{"user1", "user2", "user3", "session1", "cache1", "data1"}

	fmt.Println("Key assignments:")
	for _, key := range testKeys {
		node, err := ch.GetNode(key)
		if err != nil {
			log.Printf("Error getting node for %s: %v", key, err)
			continue
		}
		fmt.Printf("Key '%s' -> Node '%s'\n", key, node)
	}

	fmt.Println("\nReplication example:")
	replicas, err := ch.GetNodes("important_data", 3)
	if err != nil {
		log.Printf("Error getting replicas: %v", err)
	} else {
		for i, replica := range replicas {
			fmt.Printf("Replica %d: %s\n", i+1, replica)
		}
	}

	stats := ch.GetStats()
	fmt.Printf("\nStatistics: Physical=%d, Virtual=%d, Weight=%d\n", stats.PhysicalNodes, stats.VirtualNodes, stats.TotalWeight)
}
