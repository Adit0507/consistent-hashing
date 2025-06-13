package consistenthash

// weighted node
type Node struct {
	Name string
	Weight int 
}

func NewNode(name string, weight int) *Node {
	if weight <= 0 {
		weight = 1
	}

	return &Node{
		Name: name,
		Weight: weight,	
	}
}

// returns no. of virtual noides for physical node
func (n *Node) VirtualNodeCount(baseReplicas int) int	{
	return baseReplicas * n.Weight
}

func (n *Node) VirtualNodeKey(index int) string {
	return n.Name+ "#" + string(rune(index +'0'))
}
