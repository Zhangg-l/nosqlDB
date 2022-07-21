package art

import (
	goart "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	// 基数树
	tree goart.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
	}
}

// 如果存在旧值 返回旧值和true，否则nil,flase
func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldValue interface{}, update bool) {
	return art.tree.Insert(key, value)
}
func (art *AdaptiveRadixTree) Get(key []byte) (value interface{}) {
	value, _ = art.tree.Search(key)
	return
}

func (art *AdaptiveRadixTree) Delete(key []byte) (Value interface{}, update bool) {
	return art.tree.Delete(key)
}
func (art *AdaptiveRadixTree) Iterator() goart.Iterator {
	return art.tree.Iterator()
}

func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}
