package zset

import (
	"fmt"
	"math"
	"math/rand"
)

// zset is the implementation of sorted set

const (
	maxLevel  = 32
	probality = 0.25
)

type EncodeKey func(key, subKey []byte) []byte

type (
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	SortedSetNode struct {
		dict map[string]*sklNode
		skl  *skipList
	}

	sklLevel struct {
		forward *sklNode
		// 表示当前节点在当前层到达下一个节点的距离。
		// 后续查找节点的时候，每层会获取一个节点，这些节点的span加起来，就是节点的rank
		span uint64
	}

	sklNode struct {
		member   string //节点的值
		score    float64
		backward *sklNode
		level    []*sklLevel
	}
	skipList struct {
		head   *sklNode
		tail   *sklNode
		length int   //跳表的元素个数
		level  int16 //当前的实际层数
	}
)

func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}

// func(z *SortedSet)IterateAndSend()

// ZAdd Adds the specified member with the specified score to the sorted set stored at key.

func (z *SortedSet) ZAdd(key string, score float64, member string) {
	if !z.exist(key) {
		node := &SortedSetNode{
			skl:  NewSkipList(),
			dict: make(map[string]*sklNode),
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]
	var node *sklNode
	if exist {
		if score != v.score {
			item.skl.sklDelete(v.score, member)
			node = item.skl.sklInsert(score, member)
		}
	} else {
		node = item.skl.sklInsert(score, member)
	}
	if node != nil {
		item.dict[member] = node
	}
}

//  ZScore returns the score of member in the sorted set at key.
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}

	item, _ := z.record[key]

	if node, ok := item.dict[member]; ok {
		return ok, node.score
	}

	return
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}
	return len(z.record[key].dict)
}

func (z *SortedSet) exist(key string) bool {
	_, ok := z.record[key]

	return ok
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.

func (z *SortedSet) ZRank(key string, member string) int64 {
	if !z.exist(key) {
		return -1
	}
	v, exist := z.record[key].dict[member]

	if !exist {
		return -1
	}
	// 1开始
	rank := z.record[key].skl.sklGetRank(v.score, v.member)
	rank--
	return rank
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.

func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}
	v, exist := z.record[key].dict[member]

	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, v.member)

	return int64(z.record[key].skl.length) - rank
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment

}

// ZRange returns the specified range of elements in the sorted set stored at <key>.

func (z *SortedSet) ZRange(key string, start, end int) []interface{} {

	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, (start), (end), false, false)
}

func (z *SortedSet) ZRangeWithScore(key string, start, end int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, (start), (end), false, true)

}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRange(key string, start, end int) []interface{} {

	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, (start), (end), true, false)
}

func (z *SortedSet) ZRevRangeWithScore(key string, start, end int) []interface{} {

	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, (start), (end), true, true)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.

func (z *SortedSet) ZRem(key, member string) bool {

	if !z.exist(key) {
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}
	return false
}

// ZGetByRank get the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}
	score, member := z.getByRank(key, rank, false)

	return append(val,member,score  )
}

func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}
	score, member := z.getByRank(key, rank, true)

	return append(val, member,score )
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	skl := z.record[key].skl

	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := skl.tail.score

	if max > maxScore {
		max = maxScore
	}

	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward

	for p != nil {
		if p.score > max {
			break
		}
		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}
	return
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.

func (z *SortedSet) ZRevScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	skl := z.record[key].skl

	minScore := skl.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := skl.tail.score

	if max > maxScore {
		max = maxScore
	}

	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}
		val = append(val, p.member, p.score)
		p = p.backward
	}

	return
}
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// ZClear clear the key in zset.
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

func (z *SortedSet) getByRank(key string, rank int, reverse bool) (score float64, member string) {
	skl := z.record[key].skl

	if rank < 0 || rank > skl.length {
		return math.MinInt64, ""
	}
	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}
	var node *sklNode
	if node = skl.sklGetElementByRank(uint64(rank)); node == nil {
		return math.MinInt64, ""
	}

	if _, ok := z.record[key].dict[node.member]; !ok {
		return math.MinInt64, ""
	}

	return node.score, node.member
}

func (z *SortedSet) Ztraver(key string) {
	if !z.exist(key) {
		fmt.Println("key is not exist")
	}
	p := z.record[key].skl.head
	for p.level[0].forward != nil {
		fmt.Printf("%v\n", p.level[0].forward.score)
		p = p.level[0].forward
	}

}

func (z *SortedSet) findRange(key string, start, stop int, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl

	length := skl.length
	if start < 0 {
		start += (length)
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += (length)
	}
	if stop < start || start > (length) {
		return
	}
	if stop >= (length) {
		stop = length - 1
	}
	span := (stop - start) + 1
	var node *sklNode
	// 从后往前
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
	return
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(sklLevel)
	}
	return node
}
func NewSkipList() *skipList {
	return &skipList{
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	var level int16 = 1
	//rand.Float32() < 0.25 && level < DEFAULTMAX;
	for float32(rand.Int31()&0xFFFF) < (probality * 0xFFFF) {
		level++
	}
	if level < maxLevel {
		return level
	}

	return maxLevel
}

func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	// 记录插入节点的每一层的前一个节点
	updates := make([]*sklNode, maxLevel)
	rank := make([]uint64, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}

		if p.level[i] != nil {
			for p.level[i].forward != nil &&
				(p.level[i].forward.score < score ||
					(p.level[i].forward.score == score && p.level[i].forward.member < member)) {
				// 计算排位 && 找到这层的插入前驱
				rank[i] += p.level[i].span
				p = p.level[i].forward
			}
		}
		updates[i] = p

	} //for

	//
	level := randomLevel()

	if level > skl.level {
		for i := skl.level; i < level; i++ {
			rank[i] = 0
			updates[i] = skl.head
			updates[i].level[i].span = uint64(skl.length)
		}
		skl.level = level
	}

	p = sklNewNode(level, score, member)

	// 插入
	for i := int16(0); i < level; i++ {
		p.level[i].forward = updates[i].level[i].forward
		updates[i].level[i].forward = p

		p.level[i].span = updates[i].level[i].span - (rank[0] - rank[i])
		updates[i].level[i].span = rank[0] - rank[i] + 1
	}

	for i := level; i < skl.level; i++ {
		updates[i].level[i].span++
	}

	if updates[0] == skl.head {
		p.backward = nil
	} else {
		p.backward = updates[0]
	}

	if p.level[0].forward != nil {
		p.level[0].forward.backward = p
	} else {
		skl.tail = p
	}

	skl.length++
	return p
}

// p : 准备删除的节点
func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p {
			updates[i].level[i].span += p.level[i].span - 1
			updates[i].level[i].forward = p.level[i].forward

		} else {
			updates[i].level[i].span--
		}
	}
	if p.level[0].forward == nil {
		skl.tail = p.backward
	} else {
		p.level[0].forward.backward = p.backward
	}

	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil {
		skl.level--
	}
	skl.length--

}

func (skl *skipList) sklDelete(score float64, member string) {
	updates := make([]*sklNode, maxLevel)

	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score) && (p.level[i].forward.member < member)) {
			p = p.level[i].forward
		}
		updates[i] = p
	}

	p = p.level[0].forward
	if p != nil && score == p.score && p.member == member {
		skl.sklDeleteNode(p, updates)
		return
	}
}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0

	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {
			rank += p.level[i].span
			p = p.level[i].forward
		}
		if p.member == member {
			return int64(rank)
		}
	}
	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {

	var traverse uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (traverse+p.level[i].span) <= rank {
			traverse += p.level[i].span
			p = p.level[i].forward
		}
		if traverse == rank {
			return p
		}
	}
	return nil
}
