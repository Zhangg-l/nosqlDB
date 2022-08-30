package rosedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go_code/project13/rosedb/cache"
	"go_code/project13/rosedb/ds/hash"
	"go_code/project13/rosedb/ds/list"
	"go_code/project13/rosedb/ds/set"
	"go_code/project13/rosedb/ds/zset"
	"go_code/project13/rosedb/index"
	"go_code/project13/rosedb/storage"
	"go_code/project13/rosedb/utils"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrEmptyKey    = errors.New("rosedb: the key is empty")
	ErrKeyNotExist = errors.New("rosedb: key not exist")

	ErrKeyTooLarge            = errors.New("rosedb: key exceeded the max length")
	ErrValueTooLarge          = errors.New("rosedb: value exceeded the max length")
	ErrNilIndexer             = errors.New("rosedb: indexer is nil")
	ErrMergeUnreached         = errors.New("rosedb: unused space not reach the threshold")
	ErrExtraContainsSeparator = errors.New("rosedb: extra contains separator \\0")
	ErrInvalidTTl             = errors.New("rosedb: invalid ttl")

	ErrKeyExpired  = errors.New("rosedb: key is expired")
	ErrDBisMerging = errors.New("rosedb: can`t do reclaim and single reclaim at the same time")
	ErrDBIsClosed  = errors.New("rosedb: db is closed, reopen it")
	// ErrInvalidTTL ttl is invalid
	ErrTxIsFinished = errors.New("rosedb: transaction is finished, create a new one")

	ErrActiveFieIsNil = errors.New("rosedb: active file is nil")

	ErrWrongNumberArgs = errors.New("rosedb: wrong number of arguments")
)

const (
	configSaveFile   = string(os.PathSeparator) + "DB.CFG"
	dbTxMetaSaveFile = string(os.PathSeparator) + "DB.TX.META"
	MergePath        = string(os.PathSeparator) + "rosedb_merge"

	ExtraSeparator = "\\0"
	DataStructNum  = 5
)

type (
	StrIdx struct {
		mu      *sync.RWMutex
		idxList *index.SkipList
	}
	ListIdx struct {
		mu      *sync.RWMutex
		indexes *list.List
	}
	HashIdx struct {
		mu      *sync.RWMutex
		indexes *hash.Hash
	}

	SetIdx struct {
		mu      *sync.RWMutex
		indexes *set.Set
	}
	ZsetIdx struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
	}
)

type (
	RoseDB struct {
		// Current active files of different data types, stored like this: map[DataType]*storage.DBFile.
		activeFile      *sync.Map
		archFiles       ArchivedFiles
		strIndex        *StrIdx
		listIndex       *ListIdx
		hashIndex       *HashIdx
		setIndex        *SetIdx
		zsetIndex       *ZsetIdx
		config          *Config
		mu              sync.RWMutex
		expires         Expires
		isMerging       bool
		isSingleMerging bool
		lockMgr         *lockMgr
		// txnMeta         TxnMeta
		closed   uint32
		mergeChn chan struct{}
		cache    *cache.LruCache // lru cache for db_str.

	}
	// Expires saves the expire info of different keys.
	Expires       map[DataType]map[string]int64
	ArchivedFiles map[uint16]map[uint32]*storage.DBFile
)

// Open a rosedb instance. You must call Close after using it.

func Open(config Config) (*RoseDB, error) {
	//
	if !utils.Exist(config.DirPath) {
		if err := os.MkdirAll(config.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// load the db files from disk.
	archFiles, activeFileIds, err := storage.Build(config.DirPath, config.RwMethod, config.BlockSize)

	if err != nil {
		return nil, err
	}
	// set active file for writing

	activeFiles := new(sync.Map)

	for dataType, fileId := range activeFileIds {
		file, err := storage.NewDBFile(config.DirPath, fileId, config.RwMethod, config.BlockSize, dataType)
		if err != nil {
			return nil, err
		}
		activeFiles.Store(dataType, file)
	}

	db := &RoseDB{
		activeFile: activeFiles,
		archFiles:  archFiles,
		config:     &config,
		strIndex:   newStrIdx(),
		listIndex:  newListIdx(),
		hashIndex:  newHashIndex(),
		setIndex:   newSetIdx(),
		zsetIndex:  newZsetIdx(),
		expires:    make(Expires),
		cache:      cache.NewLruCache(config.CacheCapacity),
	}
	for i := 0; i < DataStructNum; i++ {
		db.expires[DataType(i)] = make(map[string]int64)
	}

	db.lockMgr = newLockMgr(db)

	if err := db.loadIdxFromFile(); err != nil {
		return nil, err
	}

	// handle db merge
	go func() {
		timer := time.NewTimer(config.MergeCheckInterval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				timer.Reset(config.MergeCheckInterval)
				err := db.StartMerge()
				if err != nil && err != ErrDBisMerging && err != ErrMergeUnreached {
					log.Println("rosedb: merge err: ", err)
					return
				}
			}
		}
	}()
	return db, nil
}

func (db *RoseDB) StartMerge() (err error) {
	if db.isMerging || db.isSingleMerging {
		return ErrDBisMerging
	}
	var mergeTypes int
	for _, archFiles := range db.archFiles {
		if len(archFiles) >= db.config.MergeThreshold {
			mergeTypes++
		}
	}

	if mergeTypes == 0 {
		return ErrMergeUnreached
	}

	// create a temporary directory for storing the new db files.
	mergePath := db.config.DirPath + MergePath
	if !utils.Exist(mergePath) {
		if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
			return err
		}
	}
	db.mu.Lock()
	//
	defer func() {
		db.isMerging = false
		db.mu.Unlock()
	}()

	db.isMerging = true
	// processing the different types of files in different goroutines.
	newArchivedFiles := sync.Map{}
	reclaimedTypes := sync.Map{}
	// 加载合并过得文件
	mergedFiles, _, err := storage.Build(mergePath, db.config.RwMethod, db.config.BlockSize)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(DataStructNum)
	for i := 0; i < DataStructNum; i++ {
		go func(dType uint16) {
			defer wg.Done()
			if len(db.archFiles[dType]) < db.config.MergeThreshold {
				newArchivedFiles.Store(dType, db.archFiles[dType])
				return
			}

			var (
				df      *storage.DBFile
				fileId  uint32
				fileIds []int
			)

			archFiles := mergedFiles[dType]

			if archFiles == nil {
				archFiles = make(map[uint32]*storage.DBFile)
			}

			// find the greatest file id in merged files.
			for id := range archFiles {
				if id > fileId {
					fileId = id
				}
			}

			//skip merged file
			for _, file := range db.archFiles[dType] {
				if _, exist := archFiles[file.Id]; !exist {
					fileIds = append(fileIds, int(file.Id))
				}
			}

			// need to  merge file
			sort.Ints(fileIds)

			for _, fid := range fileIds {
				select {
				case <-db.mergeChn:
					log.Printf("receive a stop signal, merge stop, data type:%d\n", dType)
					return
				default:
					file := db.archFiles[dType][uint32(fid)]
					var offset int64 = 0
					var reclaimEntries []*storage.Entry
					// read add entries in db file ,and find the valid entry

					for {
						if e, err := file.Read(offset); err == nil {
							if db.validEntry(e, offset, file.Id) {
								reclaimEntries = append(reclaimEntries, e)
							}
							offset += int64(e.Size())
						} else {
							if err == io.EOF {
								break
							}
							log.Fatalf("err occurred when read the entry: %+v", err)
							return
						}
					}

					// rewrite the valid entry to new file
					for _, entry := range reclaimEntries {

						if df == nil || int64(entry.Size())+df.Offset > db.config.BlockSize {
							df, err = storage.NewDBFile(mergePath, fileId, db.config.RwMethod, db.config.BlockSize, dType)
							if err != nil {
								log.Fatalf("err occurred when create new db file: %+v", err)

								return
							}
							archFiles[fileId] = df

							db.archFiles[dType][fileId] = df
							fileId += 1
						}

						if err = df.Write(entry); err != nil {
							log.Fatalf("err occurred when write the entry: %+v", err)
							return
						}

						// since the str type value will read from db file,so should update the index info
						if DataType(dType) == String {
							item := db.strIndex.idxList.Get(entry.Meta.Key)
							idx := item.Value().(*index.Indexer)
							idx.Offset = df.Offset - int64(entry.Size())
							idx.FieldId = df.Id
							db.strIndex.idxList.Put(idx.Meta.Key, idx)
						}
					}

					if err = file.Close(false); err != nil {
						log.Println("close old db file err: ", err)
						return
					}

					if err = os.Remove(file.File.Name()); err != nil {
						log.Println("remove old db file err: ", err)
						return
					}

				} // default
			}

			reclaimedTypes.Store(dType, struct{}{})
			newArchivedFiles.Store(dType, archFiles)

		}(uint16(i))
	}
	wg.Wait()

	var mergedCount int
	reclaimedTypes.Range(func(key, value interface{}) bool {
		mergedCount++
		return true
	})
	if mergedCount < mergeTypes {
		log.Printf("rosedb: merge stopped(total:%d, finished:%d), it will continue in next interval.\n", mergeTypes, mergedCount)
		return
	}
	dbArchivedFiles := make(ArchivedFiles)

	for i := 0; i < DataStructNum; i++ {
		dType := uint16(i)
		value, ok := newArchivedFiles.Load(dType)
		if !ok {
			log.Printf("one type of data(%d) is missed after merge.", dType)
			return
		}
		dbArchivedFiles[dType] = value.(map[uint32]*storage.DBFile)
	}

	// copy the temporary reclaim directory as new db files.
	for dataType, files := range dbArchivedFiles {
		if _, exist := reclaimedTypes.Load(dataType); exist {
			for _, f := range files {
				name := storage.PathSeparator + fmt.Sprintf(storage.DBFileFormNames[dataType], f.Id)
				// move
				os.Rename(mergePath+name, db.config.DirPath+name)
			}
		}
	}

	if err = os.RemoveAll(mergePath); err != nil {
		return
	}

	db.archFiles = dbArchivedFiles
	return

}

// StopMerge send a stop signal to merge process.
// Then the merge operation will quit.
func (db *RoseDB) StopMerge() {
	if db.mergeChn == nil {
		db.mergeChn = make(chan struct{}, DataStructNum)
	}
	go func() {
		for i := 0; i < DataStructNum; i++ {
			db.mergeChn <- struct{}{}
		}
	}()
}

// SingleMerge reclaim a single db file`s space according to the param fileId.
// File id is the non-zero part of a db file`s name prefix, such as 000000000.data.str (fileId is 0), 000000101.data.str (fileId is 101), etc.
// Only support String type now.

// Backup copy the database directory for backup.
func (db *RoseDB) Backup(dir string) (err error) {
	if utils.Exist(db.config.DirPath) {
		err = utils.CopyDir(db.config.DirPath, dir)
	}
	return
}

func (db *RoseDB) buildIndex(entry *storage.Entry, idx *index.Indexer, isOpen bool) (err error) {
	if db.config.IdxMode == KeyValueMemMode && entry.GetType() == uint16(String) {
		idx.Meta.Value = entry.Meta.Value
		idx.Meta.ValueSize = uint32(len(entry.Meta.Value))
	}

	switch entry.GetType() {
	case storage.String:
		db.buildStringIndex(idx, entry)
	case storage.List:
		db.buildListIndex(entry)
	case storage.Hash:
		db.buildHashIndex(entry)
	case storage.Set:
		db.buildSetIndex(entry)
	case storage.ZSet:
		db.buildZsetIndex(entry)

	}
	return
}
func (db *RoseDB) Sync() (err error) {

	if db == nil || db.activeFile == nil {
		return nil
	}

	db.activeFile.Range(func(key, value interface{}) bool {
		if dbFile, ok := value.(*storage.DBFile); ok {
			if err = dbFile.Sync(); err != nil {
				return false
			}
		}
		return true
	})
	if err != nil {
		return
	}
	return
}
func (db *RoseDB) Close() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err = db.saveConfig(); err != nil {
		return err
	}
	db.activeFile.Range(func(key, value interface{}) bool {
		if dbfile, ok := value.(*storage.DBFile); ok {
			if err = dbfile.Close(true); err != nil {
				return false
			}
		}
		return true
	})

	if err != nil {
		return
	}
	// close the archived files.
	for _, archFile := range db.archFiles {
		for _, file := range archFile {
			if err = file.Sync(); err != nil {
				return err
			}
		}
	}
	atomic.StoreUint32(&db.closed, 1)
	return
}

func (db *RoseDB) saveConfig() (err error) {
	path := db.config.DirPath + configSaveFile

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)

	b, err := json.Marshal(db.config)
	_, err = file.Write(b)
	err = file.Close()
	return

}

func (db *RoseDB) checkKeyValue(key []byte, value ...[]byte) error {
	keySize := uint32(len(key))
	if keySize == 0 {
		return ErrEmptyKey
	}

	config := db.config

	if keySize > config.MaxKeySize {
		return ErrKeyTooLarge
	}

	for _, v := range value {
		if uint32(len(v)) > config.MaxValueSize {
			return ErrValueTooLarge
		}
	}

	return nil
}

// validEntry check whether entry is valid(contains add and update types of operations).
// expired entry will be filtered.
func (db *RoseDB) validEntry(e *storage.Entry, offset int64, fileId uint32) bool {
	// isValid := false
	if e == nil {
		return false
	}
	// uncommitted entry is invalid.
	// if e.TxId != 0 {
	// 	if _, ok := db.txnMeta.committedTxIds[e.TxId]; !ok {
	// 		return false
	// 	}
	// 	e.TxId = 0
	// }
	mark := e.GetMark()
	switch e.GetType() {
	case uint16(String):
		deadline, exist := db.expires[String][string(e.Meta.Key)]
		now := time.Now().Unix()

		if mark == StringExpire {
			if exist && deadline > now {
				return true
			}
		}

		if mark == StringSet || mark == StringPersist {
			if exist && deadline <= now {
				return false
			}
			// check the data position.
			node := db.strIndex.idxList.Get(e.Meta.Key)
			if node == nil {
				return false
			}
			indexer := node.Value().(*index.Indexer)
			if bytes.Compare(indexer.Meta.Key, e.Meta.Key) == 0 {
				if indexer != nil && indexer.FieldId == fileId && indexer.Offset == offset {
					return true
				}
			}

		}

	case uint16(List):
		if mark == ListLExpire {
			deadline, exist := db.expires[List][string(e.Meta.Key)]

			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == ListLPush || mark == ListRPush || mark == ListLInsert || mark == ListLSet {
			if db.LValExists(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case uint16(Hash):
		if mark == HashExpire {
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]

			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		if mark == HashSet {
			if val := db.HGet(e.Meta.Key, e.Meta.Extra); string(val) == string(e.Meta.Value) {
				return true
			}
		}

	case uint16(Set):
		if mark == SetSExpire {
			deadline, exist := db.expires[Hash][string(e.Meta.Key)]

			if exist && deadline > time.Now().Unix() {
				return true
			}
		}
		// in setsmove , extra  is dst position
		if mark == SetSMove {
			if db.SIsMember(e.Meta.Extra, e.Meta.Value) {
				return true
			}
		}

		if mark == SetSAdd {
			if db.SIsMember(e.Meta.Key, e.Meta.Value) {
				return true
			}
		}
	case uint16(ZSet):
		if mark == ZSetEXpire {
			deadline, exist := db.expires[ZSet][string(e.Meta.Key)]
			if exist && deadline > time.Now().Unix() {
				return true
			}
		}

		if mark == ZSetAdd {
			if val, err := utils.StrToFloat64(string(e.Meta.Extra)); err == nil {
				ok, score := db.ZScore(e.Meta.Key, e.Meta.Value)
				if ok && score == val {
					return true
				}
			}
		}
	}
	return false
}

func (db *RoseDB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) == 1
}

func (db *RoseDB) getActiveFile(dType uint16) (file *storage.DBFile, err error) {
	value, ok := db.activeFile.Load(dType)

	if !ok || value == nil {
		return nil, ErrActiveFieIsNil
	}

	var typeOk bool

	if file, typeOk = value.(*storage.DBFile); !typeOk {
		return nil, ErrActiveFieIsNil
	}
	return
}

func (db *RoseDB) encode(key, value interface{}) (encKey, encValue []byte, err error) {
	if encKey, err = utils.EncodeKey(key); err != nil {
		return
	}

	if encValue, err = utils.EncodeValue(value); err != nil {
		return
	}
	return
}

func (db *RoseDB) checkExpired(key []byte, dType DataType) (expired bool) {
	deadline, exist := db.expires[dType][string(key)]
	if !exist {
		return
	}
	if time.Now().Unix() > deadline {
		expired = true
		// 过期了 得到对应的数据 添加多起标志
		var e *storage.Entry
		switch dType {
		case String:
			e = storage.NewEntryNoExtra(key, nil, uint16(String), StringRem)
			db.strIndex.idxList.Remove(key)
		case Hash:
			e = storage.NewEntryNoExtra(key, nil, uint16(Hash), HashClear)
			db.hashIndex.indexes.HClear(string(key))
		case List:
			e = storage.NewEntryNoExtra(key, nil, uint16(List), ListLClear)
			db.listIndex.indexes.LClear(string(key))
		case Set:
			e = storage.NewEntryNoExtra(key, nil, uint16(Set), SetSClear)
			db.setIndex.indexes.SClear(string(key))
		case ZSet:
			e = storage.NewEntryNoExtra(key, nil, uint16(ZSet), ZSetClear)
			db.zsetIndex.indexes.ZClear(string(key))
		}

		if err := db.store(e); err != nil {
			log.Println("checkExpired: store entry err: ", err)
			return
		}

		delete(db.expires[dType], string(key))

	}

	return
}

func (db *RoseDB) store(e *storage.Entry) error {
	config := db.config
	activeFile, err := db.getActiveFile((e.GetType()))

	if err != nil {
		return err
	}
	// sync the db file if file size is not enough, and open a new db file.
	if activeFile.Offset+int64(e.Size()) > config.BlockSize {
		if err = activeFile.Sync(); err != nil {
			return err
		}

		// save old file & open new file
		activeFileId := activeFile.Id
		db.archFiles[(e.GetType())][activeFileId] = activeFile
		newDBFile, err := storage.NewDBFile(config.DirPath, activeFileId+1, config.RwMethod, config.BlockSize, e.GetType())
		if err != nil {
			return err
		}
		activeFile = newDBFile
	}
	// write entry to db file.
	if err := activeFile.Write(e); err != nil {
		return err
	}

	db.activeFile.Store(e.GetType(), activeFile)

	if config.Sync {
		if err = activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}
