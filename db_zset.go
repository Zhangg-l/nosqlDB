package rosedb

import (
	"go_code/project13/rosedb/ds/zset"
	"go_code/project13/rosedb/storage"
	"go_code/project13/rosedb/utils"
	"sync"
	"time"
)

func newZsetIdx() *ZsetIdx {
	return &ZsetIdx{
		indexes: zset.New(),
		mu:      new(sync.RWMutex),
	}
}

func (db *RoseDB) ZAdd(key interface{}, score float64, member interface{}) (err error) {
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return err
	}
	if err = db.checkKeyValue(encKey, encMember); err != nil {
		return
	}

	// if the score corresponding to the key and member already exist, nothing will be done.
	if ok, oldScore := db.ZScore(encKey, encMember); ok && oldScore == score {
		return nil
	}
	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	extra := []byte(utils.Float64ToStr(score))
	e := storage.NewEntry(encKey, encMember, extra, uint16(ZSet), ZSetAdd)
	if err = db.store(e); err != nil {
		return
	}
	db.zsetIndex.indexes.ZAdd(string(encKey), score, string(encMember))
	return
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZScore(key interface{}, member interface{}) (ok bool, score float64) {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false, -1
	}

	if db.checkExpired(encKey, ZSet) {
		return false, -1
	}
	return db.zsetIndex.indexes.ZScore(string(encKey), string(encMember))
}

// ZScore returns the score of member in the sorted set at key.
func (db *RoseDB) ZCard(key interface{}) int {
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return 0
	}

	if db.checkExpired(encKey, ZSet) {
		return 0
	}
	return db.zsetIndex.indexes.ZCard(string(encKey))
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (db *RoseDB) ZRank(key, member interface{}) int64 {

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}

	return db.zsetIndex.indexes.ZRank(string(encKey), string(encMember))
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (db *RoseDB) ZRevRank(key, member interface{}) int64 {

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return -1
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return -1
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return -1
	}
	return db.zsetIndex.indexes.ZRevRank(string(encKey), string(encMember))

}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (db *RoseDB) ZIncrBy(key interface{}, increment float64, member interface{}) (float64, error) {

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return increment, err
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return increment, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	increment = db.zsetIndex.indexes.ZIncrBy(string(encKey), increment, string(encMember))

	extra := []byte(utils.Float64ToStr(increment))
	e := storage.NewEntry(encKey, encMember, extra, uint16(ZSet), ZSetAdd)
	if err = db.store(e); err != nil {
		return increment, err
	}
	return increment, nil

}

func (db *RoseDB) ZRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZRange(string(encKey), start, stop)
}

func (db *RoseDB) ZRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZRangeWithScore(string(encKey), start, stop)
}

func (db *RoseDB) ZRevRangeWithScores(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZRevRangeWithScore(string(encKey), start, stop)
}

func (db *RoseDB) ZRevRange(key interface{}, start, stop int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()
	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZRevRange(string(encKey), start, stop)
}

func (db *RoseDB) ZRem(key, member interface{}) (ok bool, err error) {

	encKey, encMember, err := db.encode(key, member)
	if err != nil {
		return false, err
	}

	if err := db.checkKeyValue(encKey, encMember); err != nil {
		return false, err
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	if db.checkExpired(encKey, ZSet) {
		return
	}

 

	if ok = db.zsetIndex.indexes.ZRem(string(encKey), string(encMember)); ok {
		e := storage.NewEntryNoExtra(encKey, encMember, uint16(ZSet), ZSetRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

func (db *RoseDB) ZGetByRank(key interface{}, rank int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZGetByRank(string(encKey), rank)
}

func (db *RoseDB) ZRevGetByRank(key interface{}, rank int) []interface{} {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}
	return db.zsetIndex.indexes.ZRevGetByRank(string(encKey), rank)
}

func (db *RoseDB) ZScoreRange(key interface{}, min, max float64) []interface{} {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZScoreRange(string(encKey), min, max)
}

func (db *RoseDB) ZRevScoreRange(key interface{}, min, max float64) []interface{} {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return nil
	}
	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return nil
	}

	return db.zsetIndex.indexes.ZRevScoreRange(string(encKey), min, max)
}

func (db *RoseDB) ZKeyExists(key interface{}) (ok bool) {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return false
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return false
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	if db.checkExpired(encKey, ZSet) {
		return false
	}

	return db.zsetIndex.indexes.ZKeyExists(string(encKey))
}

func (db *RoseDB) ZClear(key interface{}) (err error) {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(encKey, nil, uint16(ZSet), ZSetClear)
	if err = db.store(e); err != nil {
		return
	}
	db.zsetIndex.indexes.ZClear(string(encKey))
	return
}

func (db *RoseDB) ZExpire(key interface{}, duration int64) (err error) {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if !db.ZKeyExists(encKey) {
		return ErrKeyNotExist
	}
	if duration <= 0 {
		return ErrInvalidTTl
	}

	db.zsetIndex.mu.Lock()
	defer db.zsetIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, nil, deadline, uint16(ZSet), ZSetEXpire)
	if err = db.store(e); err != nil {
		return
	}
	db.expires[ZSet][string(encKey)] = deadline
	return
}

func (db *RoseDB) ZTTL(key interface{}) (ttl int64) {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}
	if !db.ZKeyExists(encKey) {
		return
	}

	db.zsetIndex.mu.RLock()
	defer db.zsetIndex.mu.RUnlock()

	deadline, exist := db.expires[ZSet][string(encKey)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}
