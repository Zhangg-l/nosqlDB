 package rosedb

// import (
// 	"go_code/project13/rosedb/ds/art"
// 	"go_code/project13/rosedb/logfile"
// 	"time"
// )

// type DataType = int8

// const (
// 	String DataType = iota
// 	List
// 	Hash
// 	Set
// 	ZSet
// )

// func (db *RoseDB) buildIndex(dataType DataType, ent *logfile.LogEntry, pos *valuePos) {

// 	switch dataType {
// 	case String:
// 	case List:
// 	case Hash:
// 	case Set:
// 	case ZSet:

// 	}
// }

// func (db *RoseDB) buildStrsIndex(ent *logfile.LogEntry, pos *valuePos) {
// 	ts := time.Now().Unix()
// 	if ent.Type == logfile.TypeDelete || (ent.ExpiredAt != 0 && ent.ExpiredAt < ts) {
// 		db.strIndex.idxTree.Delete(ent.Key)
// 		return
// 	}

// 	_, size := logfile.EncodeEntery(ent)

// 	idxNode := &indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
// 	// 如果支持内存直接查找，把value挂在索引上
// 	if db.opts.IndexMode == KeyValueMemMode {
// 		idxNode.value = ent.Value
// 	}

// 	if ent.ExpiredAt != 0 {
// 		idxNode.expiredAt = ent.ExpiredAt
// 	}
// 	// 存在索引上
// 	db.strIndex.idxTree.Put(ent.Key, idxNode)
// }

// func (db *RoseDB) buildListIndex(ent *logfile.LogEntry, pos *valuePos) {
// 	var listKey = ent.Key
// 	/*
// 		if ent.Type != logfile.TypeListMeta{
// 			listKey,_ = db.decodeListKey(ent.Key)
// 		}
// 	*/

// 	if db.listIndex.trees[string(listKey)] == nil {
// 		db.listIndex.trees[string(listKey)] = art.NewART()
// 	}

// 	db.listIndex.idxTree = db.listIndex.trees[string(listKey)]

// 	if ent.Type == logfile.TypeDelete {
// 		db.listIndex.idxTree.Delete(ent.Key)
// 		return
// 	}
// 	_, size := logfile.EncodeEntery(ent)
// 	idxNode := indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}

// 	if db.opts.IndexMode == KeyValueMemMode {
// 		idxNode.value = ent.Value
// 	}
// 	if ent.ExpiredAt != 0 {
// 		idxNode.expiredAt = ent.ExpiredAt
// 	}
// 	db.listIndex.idxTree.Put(ent.Key, idxNode)
// }

// func (db *RoseDB) buildHashIndex(ent *logfile.LogEntry, pos valuePos) {
// 	var key string = string(ent.Key)

// 	if db.hashIndex.trees[key] == nil {
// 		db.hashIndex.trees[(key)] = art.NewART()
// 	}
// 	db.hashIndex.idxTree = db.hashIndex.trees[(key)]
// 	if ent.Type == logfile.TypeDelete {
// 		db.hashIndex.idxTree.Delete(ent.Key)
// 		return
// 	}

// 	_, size := logfile.EncodeEntery(ent)

// 	idxNode := indexNode{fid: pos.fid, offset: pos.offset, entrySize: size}
// 	if db.opts.IndexMode == KeyValueMemMode {
// 		idxNode.value = ent.Value
// 	}
// 	if ent.ExpiredAt != 0 {
// 		idxNode.expiredAt = ent.ExpiredAt
// 	}
// 	db.hashIndex.idxTree.Put([]byte(""),idxNode)
// }
