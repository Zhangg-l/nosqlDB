package rosedb

// all the  list are string type (key & value)

import (
	"bytes"
	"go_code/project13/rosedb/ds/list"
	"go_code/project13/rosedb/storage"
	"go_code/project13/rosedb/utils"
	"strconv"
	"strings"
	"sync"
	"time"
)

func newListIdx() *ListIdx {
	return &ListIdx{
		mu:      &sync.RWMutex{},
		indexes: list.New(),
	}
}

// LPush return len of key ;; insert in head
func (db *RoseDB) LPush(key interface{}, values ...interface{}) (res int, err error) {
	enKey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}

	var encVals [][]byte
	
	for _, v := range values {
		res, err := utils.EncodeValue(v)
		if err != nil {
			return -1, err
		}

		if err := db.checkKeyValue(enKey, res); err != nil {

			return -1, err
		}
		encVals = append(encVals, res)

	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	for _, val := range encVals {
		e := storage.NewEntryNoExtra(enKey, val, uint16(List), ListLPush)
		if err = db.store(e); err != nil {
			return
		}
		res = db.listIndex.indexes.LPush(string(enKey), val)
	}
	return
}

func (db *RoseDB) RPush(key interface{}, values ...interface{}) (count int, err error) {
	enkey, err := utils.EncodeKey(key)
	if err != nil {
		return -1, err
	}
	var vals [][]byte
	for _, v := range values {
		res, err := utils.EncodeValue(v)
		if err != nil {
			return -1, err
		}
		if err := db.checkKeyValue(enkey, res); err != nil {
			return -1, err
		}
		vals = append(vals, res)
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()
	for _, val := range vals {
		entry := storage.NewEntryNoExtra(enkey, val, uint16(List), ListRPush)
		if err = db.store(entry); err != nil {
			return
		}
		count = db.listIndex.indexes.RPush(string(enkey), val)
	}
	return
}

// LPop removes and returns the first elements of the list stored at key.
func (db *RoseDB) LPop(key interface{}) ([]byte, error) {
	var (
		res    []byte
		encKey []byte
		err    error
	)
	encKey, err = utils.EncodeKey(key)

	if err != nil {
		return nil, err
	}

	if err = db.checkKeyValue(encKey, nil); err != nil {
		return nil, err
	}
	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return nil, ErrKeyExpired
	}

	res = db.listIndex.indexes.LPop(string(encKey))
	
	// 删除
	if res != nil {
		e := storage.NewEntryNoExtra(encKey, res, uint16(List), ListLPop)

		if err = db.store(e); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (db *RoseDB) RPop(key interface{}) ([]byte, error) {
	var (
		encKey []byte
		res    []byte
		err    error
	)
	encKey, err = utils.EncodeKey(key)
	if err != nil {
		goto ERR
	}

	if db.checkExpired(encKey, List) {
		err = ErrKeyExpired
		goto ERR
	}

	res = db.listIndex.indexes.RPop(string(encKey))
	if res != nil {
		e := storage.NewEntryNoExtra(encKey, res, uint16(List), ListRPop)
		if err = db.store(e); err != nil {
			goto ERR
		}
	}
	return res, nil
ERR:
	return nil, err
}

// LIndex returns the element at index index in the list stored at key.
// The index is zero-based, so 0 means the first element, 1 the second element and so on.
// Negative indices can be used to designate elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate and so forth.
func (db *RoseDB) LIndex(key interface{}, idx int) []byte {
	var (
		encKey []byte
		err    error
	)

	encKey, err = utils.EncodeKey(key)
	if err != nil {
		return nil
	}

	if db.checkExpired(encKey, List) {
		return nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RLock()

	return db.listIndex.indexes.LIndex(string(encKey), idx)
}

// LRem removes the first count occurrences of elements equal to element from the list stored at key.
// The count argument influences the operation in the following ways:
// count > 0: Remove elements equal to element moving from head to tail.
// count < 0: Remove elements equal to element moving from tail to head.
// count = 0: Remove all elements equal to element.
func (db *RoseDB) LRem(key, value interface{}, count int) (int, error) {
	var (
		encKey []byte
		encVal []byte
		err    error
		res    int
	)

	encKey, err = utils.EncodeKey(key)
	if err != nil {
		return res, nil
	}
	encVal, err = utils.EncodeValue(value)
	if err != nil {
		return res, nil
	}

	if db.checkExpired(encKey, List) {
		return res, nil
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RLock()

	res = db.listIndex.indexes.LRem(string(encKey), encVal, count)

	if res > 0 {
		c := strconv.Itoa(count)
		e := storage.NewEntry(encKey, encVal, []byte(c), uint16(List), ListRem)
		if err = db.store(e); err != nil {
			return res, err
		}
	}
	return res, nil
}

// LInsert inserts element in the list stored at key either before or after the reference value pivot.
func (db *RoseDB) LInsert(key string, option list.InsertOption, pivot, value interface{}) (count int, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	encValue, err := utils.EncodeValue(value)
	if err != nil {
		return
	}
	encPivot, err := utils.EncodeValue(pivot)
	if err != nil {
		return
	}

	if err = db.checkKeyValue(encKey, encValue); err != nil {
		return
	}

	if strings.Contains(string(encPivot), ExtraSeparator) {
		return 0, ErrExtraContainsSeparator
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RLock()

	count = db.listIndex.indexes.LInsert(string(encKey), option, encPivot, encValue)
	if count != -1 {
		var buf bytes.Buffer
		buf.Write(encPivot)
		buf.Write([]byte(ExtraSeparator))
		opt := strconv.Itoa(int(option))
		buf.Write([]byte(opt))
		e := storage.NewEntry(encKey, encValue, buf.Bytes(), uint16(List), ListLInsert)
		if err = db.store(e); err != nil {
			return
		}
	}
	return
}

func (db *RoseDB) LSet(key interface{}, idx int, val interface{}) (ok bool, err error) {
	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	encValue, err := utils.EncodeValue(val)
	if err != nil {
		return
	}
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RLock()
	if ok := db.listIndex.indexes.LSet(string(encKey), idx, encValue); ok {
		i := strconv.Itoa(idx)
		e := storage.NewEntry(encKey, encValue, []byte(i), uint16(List), ListLSet)
		if err := db.store(e); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// LTrim trim an existing list so that it will contain only the specified range of elements specified.
// Both start and stop are zero-based indexes, where 0 is the first element of the list (the head), 1 the next element and so on.
func (db *RoseDB) LTrim(key interface{}, start, end int) (err error) {

	encKey, err := utils.EncodeKey(key)
	if err != nil {
		return
	}
	if err := db.checkKeyValue(encKey, nil); err != nil {
		return err
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	if db.checkExpired(encKey, List) {
		return err
	}

	if ok := db.listIndex.indexes.LTrim(string(encKey), start, end); ok {
		var buf bytes.Buffer
		buf.Write([]byte(strconv.Itoa(start)))
		buf.Write([]byte(ExtraSeparator))
		buf.Write([]byte(strconv.Itoa(end)))
		e := storage.NewEntry(encKey, nil, buf.Bytes(), uint16(List), ListLTrim)
		if err = db.store(e); err != nil {
			return err
		}
		return nil
	}
	return ErrKeyNotExist
}

// LRange returns the specified elements of the list stored at key.
// The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
// These offsets can also be negative numbers indicating offsets starting at the end of the list.
// For example, -1 is the last element of the list, -2 the penultimate, and so on.
func (db *RoseDB) LRange(key interface{}, start, end int) ([][]byte, error) {
	var (
		// res   [][]byte
		err   error
		enkey []byte
	)
	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return nil, err
	}

	if err = db.checkKeyValue(enkey, nil); err != nil {
		return nil, err
	}
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.indexes.LRange(string(enkey), start, end), nil
}

func (db *RoseDB) LLen(key interface{}) int {
	var (
		err   error
		enkey []byte
	)
	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return 0
	}

	if err = db.checkKeyValue(enkey, nil); err != nil {
		return 0
	}
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()
	return db.listIndex.indexes.LLen(string(enkey))
}

func (db *RoseDB) LKeyExists(key interface{}) bool {
	var (
		err   error
		enkey []byte
	)
	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return false
	}

	if err = db.checkKeyValue(enkey, nil); err != nil {
		return false
	}
	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(enkey, List) {
		return false
	}

	return db.listIndex.indexes.LKeyExists(string(enkey))
}

func (db *RoseDB) LValExists(key interface{}, val interface{}) bool {
	var (
		err   error
		enkey []byte
		enval []byte
	)
	enkey, enval, err = db.encode(key, val)

	if err != nil {
		return false
	}

	if err = db.checkKeyValue(enkey, enval); err != nil {
		return false
	}

	db.listIndex.mu.RLock()
	defer db.listIndex.mu.RUnlock()

	if db.checkExpired(enkey, List) {
		return false
	}

	return db.listIndex.indexes.LValExists(string(enkey), enval)
}

func (db *RoseDB) LClear(key interface{}) error {

	var (
		err   error
		enkey []byte
	)
	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return err
	}

	if err = db.checkKeyValue(enkey, nil); err != nil {
		return err
	}

	if !db.LKeyExists(enkey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	e := storage.NewEntryNoExtra(enkey, nil, uint16(List), ListLClear)

	if err = db.store(e); err != nil {
		return err
	}
	db.listIndex.indexes.LClear(string(enkey))
	delete(db.expires[List], string(enkey))
	return nil
}

// set expire time
func (db *RoseDB) LExpire(key interface{}, duration int64) (err error) {
	var (
		enkey []byte
		enval []byte
	)
	if duration <= 0 {
		return ErrInvalidTTl
	}

	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return err
	}
	if !db.LKeyExists(enkey) {
		return ErrKeyNotExist
	}

	db.listIndex.mu.Lock()
	defer db.listIndex.mu.Unlock()

	deadline := time.Now().Unix() + duration
	e := storage.NewEntryWithExpire(enkey, enval, deadline, uint16(List), ListLExpire)
	if err = db.store(e); err != nil {
		return err
	}

	db.expires[List][string(enkey)] = deadline
	return err
}

func (db *RoseDB) LTTL(key interface{}) int64 {
	var (
		enkey []byte

		err error
	)
	db.listIndex.mu.RLock()

	defer db.listIndex.mu.RUnlock()
	enkey, err = utils.EncodeKey(key)
	if err != nil {
		return 0
	}
	if !db.LKeyExists(enkey) {
		return 0
	}
	if db.checkExpired(enkey, List) {
		return 0
	}

	if deadline, exist := db.expires[List][string(enkey)]; exist {
		if deadline > time.Now().Unix() {
			return deadline - time.Now().Unix()
		}
	}
	return 0
}
