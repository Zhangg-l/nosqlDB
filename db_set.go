package rosedb

import (
	"go_code/project13/rosedb/ds/set"
	"go_code/project13/rosedb/storage"
	"sync"
	"time"
)

func newSetIdx() *SetIdx {
	return &SetIdx{
		&sync.RWMutex{},
		set.New(),
	}
}

// SAdd return element number in key 
func (db *RoseDB) SAdd(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key, members...); err != nil {
		return
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	for _, m := range members {
		if ok := db.setIndex.indexes.SIsMember(string(key), m); !ok {
			e := storage.NewEntryNoExtra(key, m, uint16(Set), SetSAdd)
			if err = db.store(e); err != nil {
				return
			}
			res = db.setIndex.indexes.SAdd(string(key), m)
		}
	}
	return
}

// SPop removes and returns one or more random members from the set value store at key.
func (db *RoseDB) SPop(key []byte, count int) (values [][]byte, err error) {

	if err = db.checkKeyValue(key); err != nil {
		return
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return
	}
	values = db.setIndex.indexes.SPop(string(key), count)
	for _, val := range values {
		e := storage.NewEntryNoExtra(key, val, uint16(Set), SetSRem)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

func (db *RoseDB) SIsMember(key, member []byte) bool {
	if err := db.checkKeyValue(key); err != nil {
		return false
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return false
	}

	return db.setIndex.indexes.SIsMember(string(key), member)
}

// SRandMember returns a random element from the set value stored at key.
// count > 0: if count less than set`s card, returns an array containing count different elements. if count greater than set`s card, the entire set will be returned.
// count < 0: the command is allowed to return the same element multiple times, and in this case, the number of returned elements is the absolute value of the specified count.
func (db *RoseDB) SRandMember(key []byte, count int) [][]byte {
	if err := db.checkKeyValue(key); err != nil {
		return nil
	}
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return nil
	}
	return db.setIndex.indexes.SRandMember(string(key), count)
}

// SRem remove the specified members from the set stored at key.
// Specified members that are not a member of this set are ignored.
// If key does not exist, it is treated as an empty set and this command returns 0.
func (db *RoseDB) SRem(key []byte, members ...[]byte) (res int, err error) {
	if err = db.checkKeyValue(key); err != nil {
		return
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return
	}
	for _, m := range members {
		if ok := db.setIndex.indexes.SRem(string(key), m); ok {
			e := storage.NewEntryNoExtra(key, m, uint16(Set), SetSRem)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

// SMove move member from the set at source to the set at destination.
func (db *RoseDB) SMove(src, dst, member []byte) (err error) {

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(src, Set) || db.checkExpired(dst, Set) {
		return ErrKeyExpired
	}
	if ok := db.setIndex.indexes.SMove(string(src), string(dst), member); ok {
		e := storage.NewEntry(src, member, dst, uint16(Set), SetSMove)
		if err = db.store(e); err != nil {
			return err
		}
	}

	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (db *RoseDB) SCard(key []byte) (res int) {

	if err := db.checkKeyValue(key); err != nil {
		return
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return
	}
	return db.setIndex.indexes.SCard(string(key))
}

// SMembers returns all the members of the set value stored at key.
func (db *RoseDB) SMembers(key []byte) (res [][]byte) {
	if err := db.checkKeyValue(key); err != nil {
		return
	}
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return
	}

	res = db.setIndex.indexes.SMembers(string(key))
	return
}

// SUnion returns the members of the set resulting from the union of all the given sets.
func (db *RoseDB) SUnion(keys ...[]byte) (res [][]byte) {

	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	var validKeys []string
	for _, k := range keys {
		if !db.checkExpired(k, Set) {
			validKeys = append(validKeys, string(k))
		}
	}
	return db.setIndex.indexes.SUnion(validKeys...)
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (db *RoseDB) SDiff(keys ...[]byte) (res [][]byte) {
	if keys == nil || len(keys) == 0 {
		return
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()
	var validKeys []string
	
	for _, k := range keys {
		if !db.checkExpired(k, Set) {
			validKeys = append(validKeys, string(k))
		}
	}

	return db.setIndex.indexes.SDiff(validKeys...)
}

// SKeyExists returns if the key exists.
func (db *RoseDB) SKeyExists(key []byte) (ok bool) {

	if err := db.checkKeyValue(key, nil);err != nil {
		return false
	}

	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return
	}

	return db.setIndex.indexes.SKeyExists(string(key))
}

func (db *RoseDB) SClear(key []byte) (err error) {

	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}

	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	if db.checkExpired(key, Set) {
		return ErrKeyExpired
	}

	e := storage.NewEntryNoExtra(key, nil, uint16(Set), SetSClear)
	if err = db.store(e); err != nil {
		return err
	}

	db.setIndex.indexes.SClear(string(key))
	return
}

func (db *RoseDB) SExpire(key []byte, duration int64) (err error) {

	if duration <= 0 {
		return ErrInvalidTTl
	}
	if !db.SKeyExists(key) {
		return ErrKeyNotExist
	}
	deadline := time.Now().Unix() + duration
	db.setIndex.mu.Lock()
	defer db.setIndex.mu.Unlock()

	e := storage.NewEntryWithExpire(key, nil, deadline, uint16(Set), SetSExpire)
	if err = db.store(e); err != nil {
		return
	}
	db.expires[Set][string(key)] = deadline

	db.setIndex.indexes.SClear(string(key))
	return
}



// STTL return time to live for the key in set.
func (db *RoseDB) STTL(key []byte) (ttl int64) {
	db.setIndex.mu.RLock()
	defer db.setIndex.mu.RUnlock()

	if db.checkExpired(key, Set) {
		return
	}

	deadline, exist := db.expires[Set][string(key)]
	if !exist {
		return
	}
	return deadline - time.Now().Unix()
}