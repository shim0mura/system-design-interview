package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"

	"github.com/k0kubun/pp/v3"
)

type RealNode struct {
	Name         string
	Color        string
	hashNum      int
	VirtualNodes []*VirtualNode
}
type VirtualNode struct {
	Name     string
	RealNode *RealNode
	hashNum  int
	Value    map[int]map[string]string // hashNumをキーにしたhash キーがどのhashNumに割り当てられたのかをわかるようにしておくためにこの構造
}

type ConsistentHashing struct {
	RealNodes                  []RealNode
	VirtualNodes               []VirtualNode
	HashRingSize               int
	VirtualNodesPerRealNodeNum int
}

func NewConsistentHashing(size int, perNum int) *ConsistentHashing {
	return &ConsistentHashing{
		HashRingSize:               size,
		VirtualNodesPerRealNodeNum: perNum,
	}
}

func (c *ConsistentHashing) hash(str string) int {
	h := fnv.New32a()
	h.Write([]byte(str))
	return int(h.Sum32()) % c.HashRingSize
}
func (c *ConsistentHashing) nodeFromHashNum(num int) (int, *VirtualNode) {
	for i, n := range c.VirtualNodes {
		if n.hashNum == num {
			return i, &n
		}
	}
	return 0, nil
}
func (c *ConsistentHashing) prevNodeFromHashNum(num int) *VirtualNode {
	var nearestNode *VirtualNode
	for _, n := range c.VirtualNodes {
		if num > n.hashNum {
			np := n
			nearestNode = &np
		}
	}
	if nearestNode != nil {
		return nearestNode
	} else {
		return &c.VirtualNodes[len(c.VirtualNodes)-1]
	}
}
func (c *ConsistentHashing) nextNodeFromHashNum(num int) *VirtualNode {
	for _, n := range c.VirtualNodes {
		if num < n.hashNum {
			return &n
		}
	}
	return &c.VirtualNodes[0]
}
func (c *ConsistentHashing) AddNode(realNode *RealNode) {
	hashNum := c.hash(realNode.Name)
	realNode.hashNum = hashNum
	for i := 0; i < c.VirtualNodesPerRealNodeNum; i++ {
		v := VirtualNode{
			Name:     realNode.Name + "-v" + strconv.Itoa(i),
			RealNode: realNode,
			Value:    make(map[int]map[string]string),
			hashNum:  (hashNum + ((c.HashRingSize / 3) * i)) % c.HashRingSize,
		}

		// hashNumが被ってたらずらす
		for _, n := c.nodeFromHashNum(v.hashNum); n != nil; {
			v.hashNum = (v.hashNum + 3) % c.HashRingSize
		}

		// case A
		// 10 30 40
		// node30を追加 => 10-30のkeyをnode40からnode30に移動
		// node30を削除 => 10-30のkeyをnode30からnode40に移動
		//
		// case B
		// 3 20 95
		// node3を追加 => 95-3のkeyをnode20からnode3に移動
		// node3を削除 => 95-3のkeyをnode3からnode20に移動
		if len(c.VirtualNodes) != 0 {
			nextNode := c.nextNodeFromHashNum(v.hashNum)
			prevNode := c.prevNodeFromHashNum(v.hashNum)

			c.moveKeys(nextNode, &v, prevNode.hashNum, v.hashNum)
		}

		// hashNumで昇順ソート
		c.VirtualNodes = append(c.VirtualNodes, v)
		realNode.VirtualNodes = append(realNode.VirtualNodes, &v)
		sort.Slice(c.VirtualNodes, func(a, b int) bool {
			return c.VirtualNodes[a].hashNum < c.VirtualNodes[b].hashNum
		})
	}
	c.RealNodes = append(c.RealNodes, *realNode)
}

func (c *ConsistentHashing) removeNode(realNodeName string) {
	fmt.Println(c.RealNodes)
	var rn RealNode
	for _, n := range c.RealNodes {
		if n.Name == realNodeName {
			rn = n
		}
	}
	for _, v := range rn.VirtualNodes {
		vn := v

		nextNode := c.nextNodeFromHashNum(vn.hashNum)
		prevNode := c.prevNodeFromHashNum(vn.hashNum)

		c.moveKeys(vn, nextNode, prevNode.hashNum, vn.hashNum)

		if len(vn.Value) != 0 {
			pp.Println(vn)
			panic("key not moved")
		}

		i, _ := c.nodeFromHashNum(vn.hashNum)
		c.VirtualNodes = append(c.VirtualNodes[:i], c.VirtualNodes[i+1:]...)
	}
	for i, n := range c.RealNodes {
		if n.Name == realNodeName {
			c.RealNodes = append(c.RealNodes[:i], c.RealNodes[i+1:]...)
			return
		}
	}
}

func (c *ConsistentHashing) moveKeys(srcNode *VirtualNode, dstNode *VirtualNode, startIndex int, endIndex int) {
	if startIndex > endIndex {
		for i := startIndex; i < c.HashRingSize; i++ {
			if srcNode.Value[i] != nil {
				dstNode.Value[i] = srcNode.Value[i]
				delete(srcNode.Value, i)
			}
		}
		startIndex = 0
	}
	for i := startIndex; i <= endIndex; i++ {
		if srcNode.Value[i] != nil {
			dstNode.Value[i] = srcNode.Value[i]
			delete(srcNode.Value, i)
		}
	}
}

func (c *ConsistentHashing) AddKeyValue(key string, value string) {
	hashNum := c.hash(key)
	for _, n := range c.VirtualNodes {
		if n.hashNum >= hashNum {
			if n.Value[hashNum] == nil {
				n.Value[hashNum] = make(map[string]string)
			}
			n.Value[hashNum][key] = value
			return
		}
	}
	if c.VirtualNodes[0].Value[hashNum] == nil {
		c.VirtualNodes[0].Value[hashNum] = make(map[string]string)
	}
	c.VirtualNodes[0].Value[hashNum][key] = value
}

func (c *ConsistentHashing) printHashRing() {
	for _, v := range c.VirtualNodes {
		pp.Println(v.Name, v.hashNum)
	}
}

func main() {
	hashing := NewConsistentHashing(100, 3)
	hashing.AddNode(&RealNode{Name: "testtest"})
	hashing.AddNode(&RealNode{Name: "fdafafda"})

	hashing.AddKeyValue("1", "aaa")
	hashing.AddKeyValue("2", "bbb")
	hashing.AddKeyValue("3", "ccc")
	hashing.AddKeyValue("2", "ddd")
	hashing.AddKeyValue("4", "eee")
	hashing.AddKeyValue("5", "fff")
	hashing.AddKeyValue("6", "ggg")
	hashing.AddKeyValue("7", "hhh")
	hashing.AddKeyValue("8", "iii")
	// pp.Println(hashing)
	hashing.printHashRing()
	hashing.AddNode(&RealNode{Name: "uuuuu"})
	pp.Println(hashing)
	hashing.printHashRing()

	hashing.removeNode("uuuuu")
	pp.Println(hashing)
}
