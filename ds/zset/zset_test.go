package zset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func InitZSet() *SortedSet {
	zSet := New()
	zSet.ZAdd("myzset", 19, "ced")
	zSet.ZAdd("myzset", 12, "acd")
	zSet.ZAdd("myzset", 17, "bcd")
	zSet.ZAdd("myzset", 32, "acc")
	zSet.ZAdd("myzset", 17, "mcd")
	zSet.ZAdd("myzset", 21, "ccd")
	zSet.ZAdd("myzset", 17, "ecd")
	return zSet

}

// func TestSortedSet_ZScore(t *testing.T) {
// 	t.Run("normal data", func(t *testing.T) {

// 		zset := InitZSet()
// 		zset.ZAdd("myzset", 13, "zhang")

// 		t.Log(zset.ZScore("myzset", "ccd"))
// 		t.Log(zset.ZScore("myzset", "zhang"))
// 		t.Log(zset.ZScore("myzset", "es"))
// 		t.Log(zset.ZScore("myzset", "asdasd"))

// 	})
// }

// func TestSortedSet_ZAdd(t *testing.T) {
// 	t.Run("normal data", func(t *testing.T) {

// 		zset := InitZSet()
// 		zset.ZAdd("myzset", 13, "zhang")
// 		t.Log(zset.ZCard("myzset"))
// 		t.Log(zset.ZScore("myzset", "ccd"))
// 		t.Log(zset.ZScore("myzset", "zhang"))

// 	})
// }

// func TestSortedSet_Rank(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	rank := zset.ZRank(key, "acd")

// 	t.Log(rank)
// 	t.Log(zset.ZRank(key, "acc"))
// 	t.Log(zset.ZRank(key, "mcd"))
// 	t.Log(zset.ZRank(key, "ecd"))
// 	t.Log(zset.ZRank(key, "bcd"))

// }

// func TestSortedSet_ZRevRank(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	rank := zset.ZRevRank(key, "acd")

// 	t.Log(rank)
// 	t.Log(zset.ZRevRank(key, "acc"))
// 	t.Log(zset.ZRevRank(key, "mcd"))
// 	t.Log(zset.ZRevRank(key, "ecd"))
// 	t.Log(zset.ZRevRank(key, "bcd"))

// }

// func TestSortedSet_ZIncrBy(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	zset.Ztraver(key)
// 	t.Log(zset.ZIncrBy(key, 300, "acd"))
// 	t.Log(zset.ZIncrBy(key, 100, "acc"))
// 	zset.Ztraver(key)

// }

// func TestSortedSet_ZRanges(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	ran := zset.ZRange(key, 0, -1)
// 	assert.Equal(t, 7, len(ran))
// 	for _, v := range ran {
// 		assert.NotNil(t, v)
// 	}
// }

// func TestSortedSet_ZRanges(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	ran := zset.ZRangeWithScore(key, 0, -1)
// 	// assert.Equal(t, 7, len(ran))
// 	for _, v := range ran {
// 		t.Logf("%+v", v)
// 	}
// }

// func TestSortedSet_ZRevRangeWithScore(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	ran := zset.ZRevRangeWithScore(key, 0, -1)
// 	// assert.Equal(t, 7, len(ran))
// 	for _, v := range ran {
// 		t.Logf("%+v", v)
// 	}
// }

// func TestSortedSet_ZRevRange(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	ran := zset.ZRevRange(key, 0, -1)
// 	// assert.Equal(t, 7, len(ran))
// 	for _, v := range ran {
// 		t.Logf("%+v", v)
// 	}
// }

// func TestSortedSet_ZRem(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()

// 	zset.Ztraver(key)
// 	ok := zset.ZRem(key,"acasc")
// 	fmt.Println(ok)
// 	_ = zset.ZRem(key,"acc")
// 	 zset.Ztraver(key)
// }

// func TestSortedSet_ZGetByRank(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()
// 	for i := 0;i <= 6 ;i++{
// 		val := zset.ZGetByRank(key,i)
// 		t.Logf("%#v\t",val)
// 	}
// 	t.Logf("%+v\n",zset.ZGetByRank(key,0))
// }

// func TestSortedSet_ZRevGetByRank(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()
// 	for i := 0;i <= 6 ;i++{
// 		val := zset.ZRevGetByRank(key,i)
// 		t.Logf("%#v\t",val)
// 	}
// 	t.Logf("%+v\n",zset.ZRevGetByRank(key,0))
// 	fmt.Println("========")
// 	zset.Ztraver(key)
// }

// func TestSortedSet_ZScoreRange(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()
// 	val := zset.ZScoreRange(key, -1, 33)
// 	t.Logf("%+v", val)
// }

// func TestSortedSet_ZRevRangeWithScore(t *testing.T) {
// 	key := "myzset"
// 	zset := InitZSet()
// 	val := zset.ZRevRangeWithScore(key, 0, 10)
// 	t.Logf("%+v", val)
// // }
// func TestSortedSet_ZCard(t *testing.T) {
// 	zSet := InitZSet()
// 	card := zSet.ZCard("myzset")
// 	assert.Equal(t, 7, card)
// // }
// func TestSortedSet_ZClear(t *testing.T) {
// 	zset := InitZSet()
// 	key := "myzset"
// 	zset.ZClear(key)

// 	card := zset.ZCard(key)
// 	assert.Equal(t, card, 0)
// }

func TestSortedSet_ZKeyExists(t *testing.T) {
	zset := InitZSet()
	key := "myzset"

	ok1 := zset.ZKeyExists(key)
	assert.Equal(t, ok1, true)

	zset.ZClear(key)
	ok2 := zset.ZKeyExists(key)
	assert.Equal(t, ok2, false)
}
