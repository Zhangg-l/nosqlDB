package rosedb

import (
	"bytes"
	"go_code/project13/rosedb/index"
	"go_code/project13/rosedb/storage"
	"go_code/project13/rosedb/utils"
	"strings"
	"sync"
	"time"
)

func newStrIdx() *StrIdx {
	return &StrIdx{
		mu:      &sync.RWMutex{},
		idxList: index.NewSkipList(),
	}
}

// Set set key to hold the string value. If key already holds a value, it is overwritten.
// Any previous time to live associated with the key is discarded on successful Set operation.
func (db *RoseDB) Set(key, value interface{}) error {
	encKey, encValue, err := db.encode(key, value)

	if err != nil {
		return err
	}

	return db.setVal(encKey, encValue)
}

// SetNx is short for "Set if not exists", set key to hold string value if key does not exist.
// In that case, it is equal to Set. When key already holds a value, no operation is performed.
func (db *RoseDB) SetNx(key, value interface{}) (bool, error) {
	encKey, encValue, err := db.encode(key, value)
	if err != nil {
		return false, err
	}

	if db.StrExists(encKey) {
		return false, err
	}

	if err = db.Set(encKey, encValue); err != nil {
		return false, err
	}
	return true, nil
}

// SetEx set key to hold the string value and set key to timeout after a given number of seconds.
func (db *RoseDB) SetEx(key, value interface{}, duration int64) (err error) {
	if duration <= 0 {
		return ErrInvalidTTl
	}
	encKey, encValue, err := db.encode(key, value)
	if err != nil {
		return err
	}

	deadline := time.Now().Unix() + duration

	e := storage.NewEntryWithExpire(encKey, encValue, deadline, uint16(String), StringExpire)
	if err = db.store(e); err != nil {
		return
	}

	// set String index info, stored at skip list.
	if err = db.setIndexer(e); err != nil {
		return
	}
	db.expires[String][string(encKey)] = deadline
	return
}

//  Get get the value of key. If the key does not exist an error is returned.
func (db *RoseDB) Get(key, dest interface{}) (err error) {
	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	val, err := db.getVal(encKey)
	if err != nil {
		return err
	}
	if len(val) > 0 {
		err = utils.DecodeValue(val, dest)
	}
	return err
}

// GetSet set key to value and returns the old value stored at key.
// If the key not exist, return an err.
func (db *RoseDB) GetSet(key, value, dest interface{}) (err error) {
	err = db.Get(key, dest)
	if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
		return
	}
	return db.Set(key, value)
}

// MSet set multiple keys to multiple values
func (db *RoseDB) MSet(values ...interface{}) error {
	 
		if len(values)%2 != 0 {
			return ErrWrongNumberArgs
		}
	 

	keys := make([][]byte, 0)
	vals := make([][]byte, 0)
	if db.config.IdxMode == KeyValueMemMode {
		for i := 0; i < len(values); i += 2 {
			encKey, encValue, err := db.encode(values[i], values[i+1])
			if err != nil {
				return err
			}
			if err := db.checkKeyValue(encKey, encValue); err != nil {
				return err
			}
			existVal, err := db.getVal(encKey)
			if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
				return err
			}
			if bytes.Compare(existVal, encValue) != 0 {
				keys = append(keys, encKey)
				vals = append(vals, encValue)
			}

		}
	} else {
		for i := 0; i < len(values); i += 2 {
			encKey, encValue, err := db.encode(values[i], values[i+1])
			if err != nil {
				return err
			}
			if err := db.checkKeyValue(encKey, encValue); err != nil {
				return err
			}
			keys = append(keys, encKey)
			vals = append(vals, encValue)

		}
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	for i := 0; i < len(keys); i++ {
		e := storage.NewEntryNoExtra(keys[i], vals[i], uint16(String), StringSet)
		if err := db.store(e); err != nil {
			return err
		}
		delete(db.expires[String], string(keys[i]))
		if err := db.setIndexer(e); err != nil {
			return err
		}

	}
	return nil
}

func (db *RoseDB) MGet(keys ...interface{}) ([][]byte, error) {
	encKeys := [][]byte{}

	// 验证key
	for _, key := range keys {
		encKey, err := utils.EncodeKey(key)
		if err != nil {
			return nil, err
		}
		if err = db.checkKeyValue(encKey, nil); err != nil {
			return nil, err
		}
		encKeys = append(encKeys, encKey)
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	vals := [][]byte{}
	for _, enckey := range encKeys {
		if val, err := db.getVal(enckey); err != nil {
			return nil, err
		} else {
			vals = append(vals, val)
		}
	}
	return vals, nil
}

// Append if key already exists and is a string, this command appends the value at the end of the string.
// If key does not exist it is created and set as an empty string, so Append will be similar to Set in this special case.
func (db *RoseDB) Append(key interface{}, val string) (err error) {

	encKey, encVal, err := db.encode(key, val)
	if err != nil {
		return err
	}

	if err = db.checkKeyValue(encKey, encVal); err != nil {
		return err
	}

	var existVal []byte
	err = db.Get(key, &existVal)
	if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
		return err
	}

	existVal = append(existVal, []byte(val)...)

	return db.Set(encKey, existVal)
}

func (db *RoseDB) StrExists(key interface{}) bool {

	encKey, err := utils.EncodeKey(key)

	if err != nil {
		return false
	}

	if err := db.checkKeyValue(encKey, nil); err != nil {
		return false
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	exist := db.strIndex.idxList.Exist(encKey)
	if exist && !db.checkExpired(encKey, String) {
		return true
	}
	return false
}

func (db *RoseDB) Remove(key interface{}) error {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if err = db.checkKeyValue(encKey, nil); err != nil {
		return err
	}
	// checking  whether exists key 
	_, err = db.getVal(encKey)
	if err != nil {
		return err
	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(encKey, nil, uint16(String), StringRem)
	if err = db.store(e); err != nil {
		return err
	}
	db.strIndex.idxList.Remove(encKey)
	delete(db.expires[String], string(encKey))
	db.cache.Remove(encKey)
	return nil
}

// PrefixScan find the value corresponding to all matching keys based on the prefix.
// limit and offset control the range of value.
// if limit is negative, all matched values will return.
func (db *RoseDB) PrefixScan(prefix string, limit, offset int) (val []interface{}, err error) {

	if limit <= 0 {
		return
	}
	if offset < 0 {
		offset = 0
	}
	if err = db.checkKeyValue([]byte(prefix), nil); err != nil {
		return
	}

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()
	
	e := db.strIndex.idxList.FindPrefix([]byte(prefix))
	if limit > 0 {
		for i := 0; i < offset && e != nil && strings.HasPrefix(string(e.Key()), prefix); i++ {
			e = e.Next()
		}
	}

	for e != nil && strings.HasPrefix(string(e.Key()), prefix) && limit != 0 {
		item := e.Value().(*index.Indexer)
		var value []byte
		if db.config.IdxMode == KeyOnlyMemMode {
			if err = db.Get(e.Key(), &value); err != nil {
				return
			}
		} else {
			if item != nil {
				value = item.Meta.Value
			}
		}

		expired := db.checkExpired(e.Key(), String)
		if !expired {
			val = append(val, value)
		}
		if limit > 0 && !expired {
			limit--
		}
		e = e.Next()
	}
	return
}

// RangeScan find range of values from start to end.
func (db *RoseDB) RangeScan(start, end interface{}) (val []interface{}, err error) {
	startKey, err := utils.EncodeKey(start)
	if err != nil {
		return nil, err
	}
	endKey, err := utils.EncodeKey(end)
	if err != nil {
		return nil, err
	}

	node := db.strIndex.idxList.Get(startKey)

	db.strIndex.mu.RLock()
	defer db.strIndex.mu.RUnlock()

	for node != nil && bytes.Compare(node.Key(), endKey) <= 0 {
		if db.checkExpired(node.Key(), String) {
			node = node.Next()
			continue
		}
		var value []byte
		if db.config.IdxMode == KeyOnlyMemMode {
			err = db.Get(node.Key(), &value)
			if err != nil && err != ErrKeyNotExist && err != ErrKeyExpired {
				return nil, err
			}
		} else {
			value = node.Value().(*index.Indexer).Meta.Value
		}
		val = append(val, value)
		node = node.Next()
	}
	return
}

func (db *RoseDB) Expire(key interface{}, duration int64) (err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if duration <= 0 {
		return ErrInvalidTTl
	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	var value []byte
	if value, err = db.getVal(encKey); err != nil {
		return
	}
	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(encKey, value, deadline, uint16(String), StringExpire)

	if err = db.store(e); err != nil {
		return err
	}
	db.expires[String][string(encKey)] = deadline
	return
}

func (db *RoseDB) Persist(key interface{}) (err error) {

	var val interface{}
	if err = db.Get(key, &val); err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	enckey, encval, err := db.encode(key, val)
	if err != nil {
		return
	}

	e := storage.NewEntryNoExtra(enckey, encval, uint16(String), StringPersist)
	if err = db.store(e); err != nil {
		return err
	}
	delete(db.expires[String], string(enckey))
	return
}

// TTL Time to live.
func (db *RoseDB) TTL(key interface{}) (ttl int64) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}

	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()

	deadline, exist := db.expires[String][string(encKey)]
	if !exist {
		return
	}
	if expired := db.checkExpired(encKey, String); expired {
		return
	}

	return deadline - time.Now().Unix()
}

func (db *RoseDB) setVal(key, value []byte) error {
	var err error

	if err = db.checkKeyValue(key, value); err != nil {
		return err
	}

	if db.config.IdxMode == KeyValueMemMode {
		var existVal []byte
		existVal, err = db.getVal(key)

		if err != nil && err != ErrKeyExpired && err != ErrKeyNotExist {
			return err
		}

		if bytes.Compare(value, existVal) == 0 {
			return nil
		}

	}
	db.strIndex.mu.Lock()
	defer db.strIndex.mu.Unlock()
	e := storage.NewEntryNoExtra(key, value, uint16(String), StringSet)
	if err := db.store(e); err != nil {
		return err
	}
	delete(db.expires[String], string(key))
	if err = db.setIndexer(e); err != nil {
		return err
	}

	// set into cache if necessary.
	db.cache.Set(key, value)
	return nil
}

func (db *RoseDB) setIndexer(e *storage.Entry) error {
	activeFile, err := db.getActiveFile(uint16(String))
	if err != nil {
		return err
	}
	idx := &index.Indexer{
		Meta: &storage.Meta{
			Key: e.Meta.Key,
		},
		FieldId: activeFile.Id,
		Offset:  activeFile.Offset,
	}

	if db.config.IdxMode == KeyValueMemMode {
		idx.Meta.Value = e.Meta.Value
	}

	db.strIndex.idxList.Put(idx.Meta.Key, idx)
	return nil
}

func (db *RoseDB) getVal(key []byte) ([]byte, error) {

	node := db.strIndex.idxList.Get(key)

	if node == nil {
		return nil, ErrKeyNotExist
	}
	
	idx := node.Value().(*index.Indexer)
	if idx == nil {
		return nil, ErrNilIndexer
	}

	if db.checkExpired(key, String) {
		return nil, ErrKeyExpired
	}

	if db.config.IdxMode == KeyValueMemMode {
		return idx.Meta.Value, nil
	}

	if db.config.IdxMode != KeyOnlyMemMode {
		return nil, ErrKeyNotExist
	}

	// 先从缓存拿数据
	if value, ok := db.cache.Get(key); ok {
		return value, nil
	}

	// 数据不在索引中时 需要 去磁盘拿数据
	df, err := db.getActiveFile(uint16(String))
	if err != nil {
		return nil, err
	}

	if idx.FieldId != df.Id {
		df = db.archFiles[uint16(String)][idx.FieldId]
	}

	e, err := df.Read(idx.Offset)
	if err != nil {
		return nil, err
	}
	value := e.Meta.Value
	// set value into cache
	db.cache.Set(key, value)
	return value, nil
}
