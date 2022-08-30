package rosedb

import (
	"bytes"
	"go_code/project13/rosedb/ds/hash"
	"go_code/project13/rosedb/storage"
	"sync"
	"time"
)

// type HashIndex struct {
// 	mu      *sync.RWMutex
// 	indexes *hash.Hash
// }

func newHashIndex() *HashIdx {
	return &HashIdx{
		mu:      &sync.RWMutex{},
		indexes: hash.New(),
	}
}

// HSet sets field in the hash stored at key to value. If key does not exist, a new key holding a hash is created.
// If field already exists in the hash, it is overwritten.
// Return num of elements in hash of the specified filed of key.
func (db *RoseDB) HSet(key []byte, field []byte, value []byte) (res int, err error) {

	if err = db.checkKeyValue(key, value); err != nil {
		return
	}
	oldValue := db.HGet(key, field)
	if bytes.Compare(oldValue, value) == 0 {
		return
	}
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	e := storage.NewEntry(key, value, field, uint16(Hash), HashSet)
	if err = db.store(e); err != nil {
		return
	}
	res = db.hashIndex.indexes.HSet(string(key), string(field), value)
	return
}

func (db *RoseDB) HGet(key []byte, field []byte) (res []byte) {

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	if db.checkExpired(key, Hash) {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	return db.hashIndex.indexes.HGet(string(key), string(field))
}

// HSetNx Sets field in the hash stored at key to value, only if field does not yet exist.
// If key does not exist, a new key holding a hash is created. If field already exists, this operation has no effect.
// Return if the operation is successful.
func (db *RoseDB) HSetNx(key []byte, field []byte, value []byte) (res int, err error) {

	if err = db.checkKeyValue(key, value); err != nil {
		return
	}
	oldValue := db.HGet(key, field)
	if bytes.Compare(oldValue, value) == 0 {
		return
	}
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()

	if res = db.hashIndex.indexes.HSetNx(string(key), string(field), value); res == 1 {
		e := storage.NewEntry(key, value, field, uint16(Hash), HashSet)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

// HGetAll returns all fields and values of the hash stored at key.
// In the returned value, every field name is followed by its value, so the length of the reply is twice the size of the hash.
func (db *RoseDB) HGetAll(key []byte) [][]byte {
	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}
	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return nil
	}
	res := [][]byte{}
	res = db.hashIndex.indexes.HGetAll(string(key))
	return res
}

// HMSet set multiple hash fields to multiple values

func (db *RoseDB) HMSet(key []byte, values ...[]byte) error {

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	if len(values)%2 != 0 {
		return ErrWrongNumberArgs
	}

	fields := [][]byte{}

	for i := 0; i < len(values); i += 2 {
		fields = append(fields, values[i])
	}

	existVals := db.HMGet(key, fields...)

	insertVals := [][]byte{}
	if existVals == nil {
		insertVals = values
	} else {
		for i := 0; i < len(existVals); i++ {
			if bytes.Compare(values[i*2+1], existVals[i]) != 0 {
				insertVals = append(insertVals, fields[i], values[i*2+1])
			}
		}
	}

	// check all fields and values.
	if err := db.checkKeyValue(key, insertVals...); err != nil {
		return err
	}
	
	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	for i := 0; i < len(insertVals); i += 2 {
		e := storage.NewEntry(key, insertVals[i+1], insertVals[i], uint16(Hash), HashSet)
		if err := db.store(e); err != nil {
			return err
		}
		db.hashIndex.indexes.HSet(string(key), string(insertVals[i]), insertVals[i+1])
	}
	return nil
}

func (db *RoseDB) HMGet(key []byte, values ...[]byte) [][]byte {

	if err := db.checkKeyValue(key, nil); err != nil {
		return nil
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()

	if db.checkExpired(key, Hash) {
		return nil
	}
	vals := [][]byte{}
	for _, f := range values {
		tmpRes := db.hashIndex.indexes.HGet(string(key), string(f))

		vals = append(vals, tmpRes)
	}
	return vals
}

func (db *RoseDB) HDel(key []byte, fields ...[]byte) (res int, err error) {

	if err = db.checkKeyValue(key); err != nil {
		return
	}

	if fields == nil || len(fields) == 0 {
		return
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	for _, f := range fields {
		if ok := db.hashIndex.indexes.HDel(string(key), string(f)); ok == 1 {
			e := storage.NewEntry(key, nil, f, uint16(Hash), HashDel)
			if err = db.store(e); err != nil {
				return
			}
			res++
		}
	}
	return
}

func (db *RoseDB) HKeyExist(key []byte) bool {
	if err := db.checkKeyValue(key); err != nil {
		return false
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return false
	}
	return db.hashIndex.indexes.HKeyExists(string(key))
}

func (db *RoseDB) HExists(key []byte, field []byte) bool {
	if err := db.checkKeyValue(key); err != nil {
		return false
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return false
	}
	return db.hashIndex.indexes.HExists(string(key), string(field))
}

func (db *RoseDB) HLen(key []byte) int {
	if err := db.checkKeyValue(key); err != nil {
		return 0
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return 0
	}
	return db.hashIndex.indexes.HLen(string(key))
}

// HKeys returns all field names in the hash stored at key.
func (db *RoseDB) HKeys(key []byte) (val []string) {
	if err := db.checkKeyValue(key); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return
	}
	return db.hashIndex.indexes.HKeys(string(key))
}

// HVals returns all the value of field names in the hash stored at key.
func (db *RoseDB) HVals(key []byte) (vals [][]byte) {
	if err := db.checkKeyValue(key); err != nil {
		return
	}

	db.hashIndex.mu.RLock()
	defer db.hashIndex.mu.RUnlock()
	if db.checkExpired(key, Hash) {
		return
	}
	return db.hashIndex.indexes.HVals(string(key))
}

func (db *RoseDB) HClear(key []byte) (err error) {
	if err = db.checkKeyValue(key); err != nil {
		return
	}
	if !db.HKeyExist(key) {
		return ErrKeyNotExist
	}

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(key, nil, uint16(Hash), HashClear)
	if err = db.store(e); err != nil {
		return
	}
	db.hashIndex.indexes.HClear(string(key))
	delete(db.expires[Hash], string(key))
	return
}

// HExpire set expired time for a hash key.
func (db *RoseDB) HExpire(key []byte, duration int64) (err error) {
	if err = db.checkKeyValue(key); err != nil {
		return
	}
	if !db.HKeyExist(key) {
		return ErrKeyNotExist
	}
	deadline := duration + time.Now().Unix()

	db.hashIndex.mu.Lock()
	defer db.hashIndex.mu.Unlock()
	e := storage.NewEntryWithExpire(key, nil, deadline, uint16(Hash), HashClear)
	if err = db.store(e); err != nil {
		return
	}
	db.expires[Hash][string(key)] = deadline
	return
}

func (db *RoseDB) HTTL(key []byte) int64 {
	if err := db.checkKeyValue(key); err != nil {
		return 0
	}
	if !db.HKeyExist(key) {
		return 0
	}

	if _, ok := db.expires[Hash][string(key)]; !ok {
		return 0
	}
	ttl := db.expires[Hash][string(key)] - time.Now().Unix()

	return ttl
}
