package rosedb

import (
	"go_code/project13/rosedb/ds/list"
	"go_code/project13/rosedb/index"
	"go_code/project13/rosedb/storage"
	"go_code/project13/rosedb/utils"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DataType uint16

const (
	String DataType = iota
	List
	Hash
	Set
	ZSet
)

// The operations of a String Type, will be a part of Entry, the same for the other four types.

const (
	StringSet uint16 = iota
	StringRem
	StringExpire
	StringPersist
)
const (
	ListLPush uint16 = iota
	ListRPush
	ListLPop
	ListRPop
	ListRem
	ListLInsert
	ListLSet
	ListLTrim
	ListLClear
	ListLExpire
)

const (
	HashSet uint16 = iota
	HashDel
	HashClear
	HashExpire
)

const (
	ZSetAdd uint16 = iota
	ZSetRem
	ZSetClear
	ZSetEXpire
)

const (
	SetSAdd uint16 = iota
	SetSRem
	SetSMove
	SetSClear
	SetSExpire
)

func (db *RoseDB) buildStringIndex(idx *index.Indexer, entry *storage.Entry) {
	if db.strIndex == nil || idx == nil {
		return
	}
	switch entry.GetMark() {
	case ListLPush:
		db.strIndex.idxList.Put(entry.Meta.Key, idx)
	case StringRem:
		db.strIndex.idxList.Remove(entry.Meta.Key)
	case StringExpire:
		if entry.TimeStamp < uint64(time.Now().Unix()) {
			db.strIndex.idxList.Remove(idx.Meta.Key)
		} else {
			db.expires[String][string(idx.Meta.Key)] = int64(entry.TimeStamp)
			db.strIndex.idxList.Put(idx.Meta.Key, idx)
		}
	case StringPersist:
		db.strIndex.idxList.Put(entry.Meta.Key, idx)
	}
}

func (db *RoseDB) buildListIndex(entry *storage.Entry) {
	if db.strIndex == nil || entry == nil {
		return
	}
	key := string(entry.Meta.Key)
	switch entry.GetMark() {
	case ListLPush:
		db.listIndex.indexes.LPush(key, entry.Meta.Value)
	case ListLPop:
		db.listIndex.indexes.LPop(key)
	case ListRPush:
		db.listIndex.indexes.RPush(key, entry.Meta.Value)
	case ListRPop:
		db.listIndex.indexes.RPop(key)
	case ListRem:
		if count, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LRem(key, entry.Meta.Value, count)
		}
	case ListLInsert:
		extra := string(entry.Meta.Extra)

		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			pivot := []byte(s[0])
			if opt, err := strconv.Atoi(s[1]); err == nil {
				db.listIndex.indexes.LInsert(key, list.InsertOption(opt), pivot, entry.Meta.Value)
			}
		}
	case ListLSet:
		if i, err := strconv.Atoi(string(entry.Meta.Extra)); err == nil {
			db.listIndex.indexes.LSet(key, i, entry.Meta.Value)
		}
	case ListLTrim:
		extra := string(entry.Meta.Extra)
		s := strings.Split(extra, ExtraSeparator)
		if len(s) == 2 {
			start, _ := strconv.Atoi(s[0])
			end, _ := strconv.Atoi(s[1])
			db.listIndex.indexes.LTrim(key, start, end)
		}
	case ListLClear:

		db.listIndex.indexes.LClear(key)
	case ListLExpire:
		if entry.TimeStamp < uint64(time.Now().Unix()) {
			db.listIndex.indexes.LClear(key)
		} else {
			db.expires[List][key] = int64(entry.TimeStamp)
		}

	}
}

func (db *RoseDB) buildHashIndex(entry *storage.Entry) {
	if db.strIndex == nil || entry == nil {
		return
	}
	key := string(entry.Meta.Key)
	val := entry.Meta.Value

	switch entry.GetMark() {
	case HashSet:
		db.hashIndex.indexes.HSet(key, string(entry.Meta.Extra), val)
	case HashDel:
		db.hashIndex.indexes.HDel(key, string(entry.Meta.Extra))

	case HashClear:
		db.hashIndex.indexes.HClear(key)
	case HashExpire:
		if entry.TimeStamp < uint64(time.Now().Unix()) {
			db.hashIndex.indexes.HClear(key)
		} else {
			db.expires[Hash][key] = int64(entry.TimeStamp)

		}
	}
}

func (db *RoseDB) buildSetIndex(entry *storage.Entry) {
	if db.strIndex == nil || entry == nil {
		return
	}
	key := string(entry.Meta.Key)
	val := entry.Meta.Value

	switch entry.GetMark() {
	case SetSAdd:
		db.setIndex.indexes.SAdd(key, val)
	case SetSRem:
		db.setIndex.indexes.SRem(key, val)
	case SetSMove:
		extra := entry.Meta.Extra
		db.setIndex.indexes.SMove(key, string(extra), val)
	case SetSClear:
		db.setIndex.indexes.SClear(key)
	case SetSExpire:
		if entry.TimeStamp < uint64(time.Now().Unix()) {
			db.setIndex.indexes.SClear(key)
		} else {
			db.expires[Set][key] = int64(entry.TimeStamp)

		}
	}
}

// ZSetAdd
// ZSetRem
// ZSetClear
// ZSetEXpire

func (db *RoseDB) buildZsetIndex(entry *storage.Entry) {
	if db.strIndex == nil || entry == nil {
		return
	}
	key := string(entry.Meta.Key)
	val := entry.Meta.Value

	switch entry.GetMark() {
	case ZSetAdd:
		if score, err := utils.StrToFloat64(string(entry.Meta.Extra)); err == nil {
			db.zsetIndex.indexes.ZAdd(key, score, string(val))
		}
	case ZSetRem:
		db.zsetIndex.indexes.ZRem(key, string(val))
	case ZSetClear:
		db.zsetIndex.indexes.ZClear(key)
	case ZSetEXpire:
		if entry.TimeStamp < uint64(time.Now().Unix()) {
			db.zsetIndex.indexes.ZClear(key)
		} else {
			db.expires[ZSet][key] = int64(entry.TimeStamp)
		}
	}
}

// load String、List、Hash、Set、ZSet indexes from db files.
func (db *RoseDB) loadIdxFromFile() error {
	if db.archFiles == nil && db.activeFile == nil {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructNum)
	for datatype := 0; datatype < DataStructNum; datatype++ {
		go func(datatype uint16) {
			defer wg.Done()

			var fileIds []int
			dbFile := make(map[uint32]*storage.DBFile)

			for k, v := range db.archFiles[(datatype)] {
				dbFile[k] = v
				fileIds = append(fileIds, int(k))
			}
			// active file

			activeFile, err := db.getActiveFile((datatype))
			if err != nil {
				log.Fatalf("active file is nil, the db can not open.[%+v]", err)
				return
			}

			dbFile[activeFile.Id] = activeFile
			fileIds = append(fileIds, int(activeFile.Id))
			sort.Ints(fileIds)
			for i := 0; i < len(fileIds); i++ {
				fid := uint32(fileIds[i])
				df, ok := dbFile[uint32(i)]; 
				if !ok {
					continue
				}

				var offset int64 = 0
				for offset < db.config.BlockSize {
					if e, err := df.Read(offset); err == nil {
						idx := &index.Indexer{
							Meta:    e.Meta,
							FieldId: fid,
							Offset:  offset,
						}

						offset += int64(e.Size())
						if len(e.Meta.Key) > 0 {
							if err := db.buildIndex(e, idx, true); err != nil {
								log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
							}
						}

						// if i == len(fileIds)-1 {

						// }

					} else {
						if err == io.EOF {
							break
						}
						log.Fatalf("a fatal err occurred, the db can not open.[%+v]", err)
					}

				}
			}
		}(uint16(datatype)) // go

	}
	wg.Wait()
	return nil
}
