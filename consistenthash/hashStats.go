package consistenthash

import "fmt"

type Stats struct {
	PhysicalNodes int
	VirtualNodes  int
	TotalWeight   int
	BaseReplicas  int
}

func (ch *ConsistentHash) GetStats() Stats {
	totalWeight := 0
	for _, node := range ch.nodes {
		totalWeight += node.Weight
	}

	return Stats{
		PhysicalNodes: len(ch.nodes),
		VirtualNodes:  len(ch.keys),
		TotalWeight:   totalWeight,
		BaseReplicas:  ch.baseReplicas,
	}
}

func (ch *ConsistentHash) GetNodeWeights() map[string]int {
	weights := make(map[string]int)
	for name, node := range ch.nodes {
		weights[name] = node.Weight
	}

	return weights
}

// list of all node names
func (ch *ConsistentHash) GetNodeList() []string {
	nodes := make([]string, 0, len(ch.nodes))
	for name := range ch.nodes {
		nodes = append(nodes, name)
	}

	return nodes
}

func (ch *ConsistentHash) IsEmpty() bool {
	return len(ch.nodes) == 0
}

func (ch *ConsistentHash) PrintRing() {
	fmt.Println("Hash ring state: ")
	fmt.Printf("Physical nodes: %d, Virtual nodes: %d, Base replicas: %d\n", len(ch.nodes), len(ch.keys), ch.baseReplicas)

	fmt.Println("Node weights: ")
	for name, node := range ch.nodes {
		virtualNodes := node.VirtualNodeCount(ch.baseReplicas)
		fmt.Printf("%s: weight=%d, virtual_nodes=%d\n", name, node.Weight, virtualNodes)
	}

	if len(ch.keys) <= 20 {
		fmt.Println("Virtual nodes on ring:")
		for _, hash := range ch.keys {
			fmt.Printf("  Hash: %d -> Node: %s\n", hash, ch.hashMap[hash])
		}
	}
	fmt.Println()
}
