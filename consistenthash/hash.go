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
		weight = 1
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

	sort.Ints(ch.keys) //keein keys sorted

	return nil
}

func (ch *ConsistentHash) RemoveNode(nodeName string) error {
	node := ch.nodes[nodeName]
	if node == nil {
		return fmt.Errorf("node %s doesnt exist", nodeName)
	}

	virtualNodeCount := node.VirtualNodeCount(ch.baseReplicas)

	// removin virtual nodes
	for i := 0; i < virtualNodeCount; i++ {
		virtualNodeKey := utils.GenerateVirtualNodeKey(nodeName, i)
		hash := utils.Hash(virtualNodeKey)

		delete(ch.hashMap, hash) //removin from hashmap

		idx := ch.search(hash)
		if idx < len(ch.keys) && ch.keys[idx] == hash {
			ch.keys = append(ch.keys[:idx], ch.keys[idx+1:]...)
		}

	}

	delete(ch.nodes, nodeName)

	return nil
}

func (ch *ConsistentHash) search(hash int) int {
	return sort.Search(len(ch.keys), func(i int) bool {
		return ch.keys[i] >= hash
	})
}

// returns first N nodes responsible for given key
func (ch *ConsistentHash) GetNodes(key string, count int) ([]string, error) {
	if len(ch.keys) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	if count <= 0 {
		return []string{}, nil
	}

	hash := utils.Hash(key)
	idx := ch.search(hash)
	nodes := make([]string, 0, count)
	seen := make(map[string]bool)

	for len(nodes) < count && len(nodes) < len(ch.nodes) {
		if idx >= len(ch.keys) {
			idx = 0
		}

		node := ch.hashMap[ch.keys[idx]]
		if !seen[node] {
			nodes = append(nodes, node)
			seen[node] = true
		}
		idx++
	}

	return nodes, nil
}

// return node resposnisble for givne key
func (ch *ConsistentHash) GetNode(key string) (string, error) {
	if len(ch.keys) == 0 {
		return "", fmt.Errorf("no nodes available")
	}

	hash := utils.Hash(key)

	// findin first node wit hash>= key hash
	idx := ch.search(hash)
	if idx == len(ch.keys) {
		idx = 0
	}

	return ch.hashMap[ch.keys[idx]], nil
}
