package consistenthash

import (
	"consistenthash/internal/utils"
	"fmt"
	"sort"
)

type ConsistentHash struct {
	baseReplicas int //no. of virtual nodes per physical node
	keys         []int
	hashMap      map[int]string   //hash to node name
	nodes        map[string]*Node //trackin active node with their weights
}

type Config struct {
	BaseReplicas int
}

func DefaultConfig() *Config { //returns default configuration
	return &Config{
		BaseReplicas: 50,
	}
}

// creates new consistent hash ring with given config
func NewWithConfig(config *Config) *ConsistentHash {
	return &ConsistentHash{
		baseReplicas: config.BaseReplicas,
		hashMap:      make(map[int]string),
		nodes:        make(map[string]*Node),
	}
}

func New() *ConsistentHash {
	return NewWithConfig(DefaultConfig())
}

func (ch *ConsistentHash) AddNode(nodeName string) error {
	return ch.AddWeightedNode(nodeName, 1)
}

func (ch *ConsistentHash) AddWeightedNode(nodeName string, weight int) error {
	if nodeName == "" {
		return fmt.Errorf("node name cannot be empty")
	}

	if weight <= 0 {
		weight= 1
	}

	if ch.nodes[nodeName] != nil {
		return fmt.Errorf("node %s already exists", nodeName)
	}

	node := NewNode(nodeName, weight)
	ch.nodes[nodeName] = node

	// no. of virtual nodes based on weight
	virtualNodeCount := node.VirtualNodeCount(ch.baseReplicas)
	for i := 0; i < virtualNodeCount; i++ {
		virtualNodeKey := utils.GenerateVirtualNodeKey(nodeName, i)
		hash := utils.Hash(virtualNodeKey)
		ch.keys = append(ch.keys, hash)
		ch.hashMap[hash] = nodeName
	}

	sort.Ints(ch.keys)	//keein keys sorted

	return nil
}


