package rosedb

import "sync"

// LockMgr is a lock manager that manages read and write operations of different data structures.
// It is also be used to manage transaction.
type lockMgr struct {
	locks map[DataType]*sync.RWMutex
}

func newLockMgr(db *RoseDB) *lockMgr {
	locks := make(map[DataType]*sync.RWMutex, 0)
	locks[String] = db.strIndex.mu
	locks[Hash] = db.hashIndex.mu
	locks[Set] = db.zsetIndex.mu
	locks[List] = db.listIndex.mu
	locks[ZSet] = db.zsetIndex.mu
	return &lockMgr{locks}
}

func (lm *lockMgr) Lock(dTypes ...uint16) func() {

	for _, t := range dTypes {
		lm.locks[DataType(t)].Lock()
	}

	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[DataType(t)].Unlock()
		}
	}
	return unLockFunc
}

// RLock locks the rw of dTypes for reading.

func (lm *lockMgr) RLock(dTypes ...DataType) func() {
	for _, t := range dTypes {
		lm.locks[t].RLock()
	}
	unLockFunc := func() {
		for _, t := range dTypes {
			lm.locks[t].RUnlock()
		}
	}
	return unLockFunc
}
