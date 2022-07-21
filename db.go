package rosedb

import (
	"errors"
	"go_code/project13/rosedb/ds/art"
	"go_code/project13/rosedb/ds/zset"
	"go_code/project13/rosedb/ioselector"
	"go_code/project13/rosedb/logfile"
	"go_code/project13/rosedb/utils"
	"math"
	"sync"
)

var (
	ErrKeyFound     = errors.New("key not found")
	ErrLogFileFound = errors.New("log file not found")

	ErrWrongNumberOfArgs = errors.New("wrong number of arguments")
	ErrIntegerOverflow   = errors.New("increment or decrement overflow")
	ErrWrongValueType    = errors.New("value is not an integer")
	ErrGCRunning         = errors.New("log file gc isrunning retry later")
)

const (
	logFileTypeNum = 5

	encodeHeaderSize = 10
	initialListReq   = math.MaxUint32 / 2
	discardFilePath  = "DISCARD"
	lockFileName     = "FLOCK"
)

type (
	RoseDB struct {
		activeLogFiles map[DataType]*logfile.LogFile
		// archivedLogFiles map[DataType]archivedFiles

		fidMap map[DataType][]uint32 //only used at startup, never update even though log files changed
		// discard map[DateType]*discard
		dumpState ioselector.IOSelector
		opts Options
		strIndex  *strIndex
		listIndex *listIndex
		hashIndex *hashIndex
		setIndex  *setIndex
		zsetIndex *zsetIndex
		mu        sync.RWMutex
		// fileLock *flock.fileLockGuard
		closed  uint32
		gcState int32
	}

	// archivedFiles map[uint32]*logfile.LogFile
	valuePos struct {
		fid       uint32
		offset    int64
		entrySize int
	}

	// 存在内存的不同结构的索引
	strIndex struct {
		mu      *sync.RWMutex
		idxTree *art.AdaptiveRadixTree
	}
	indexNode struct {
		value     []byte
		fid       uint32
		offset    int64
		entrySize int
		expiredAt int64
	}

	listIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	hashIndex struct {
		mu      *sync.RWMutex
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	setIndex struct {
		mu      *sync.RWMutex
		murhash *utils.Murmur128
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
	}

	zsetIndex struct {
		mu      *sync.RWMutex
		indexes *zset.SortedSet
		trees   map[string]*art.AdaptiveRadixTree
		idxTree *art.AdaptiveRadixTree
		murhash *utils.Murmur128
	}
)

func newStrsIndex() *strIndex {
	return &strIndex{idxTree: art.NewART(), mu: new(sync.RWMutex)}
}

func newListIdx() *listIndex {
	return &listIndex{
		idxTree: art.NewART(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      &sync.RWMutex{},
	}
}

func newHashIdx() *hashIndex {
	return &hashIndex{
		idxTree: art.NewART(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
	}
}

func newSetIdx() *setIndex {
	return &setIndex{
		idxTree: art.NewART(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
		murhash: utils.NewMurmur128(),
	}
}

func newZSetIdx() *zsetIndex {
	return &zsetIndex{
		indexes: zset.New(),
		trees:   make(map[string]*art.AdaptiveRadixTree),
		mu:      new(sync.RWMutex),
		murhash: utils.NewMurmur128(),
	}
}
