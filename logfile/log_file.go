package logfile

import (
	"errors"
	"fmt"
	"go_code/project13/rosedb/ioselector"
	"hash/crc32"
	"path/filepath"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidCrc             = errors.New("logfile: invalid crc")
	ErrWriteSizeNotEqual      = errors.New("logfile:  size is not equal to entry size")
	ErrEndOfEntry             = errors.New("logfile: end of entry in log file")
	ErrUnsupportedIoType      = errors.New("unsupported io type")
	ErrUnsupportedLogFileType = errors.New("unsupported log file type")
)

const (
	// InitialLogFileId initial log file id: 0.
	InitialLogFile = 0
	FilePrefix     = "log."
)

type FileType int8

const (
	Strs FileType = iota
	List
	Hash
	Sets
	ZSet
)

var (
	FileNamesMap = map[FileType]string{
		Strs: "log.strs",
		List: "log.list",
		Hash: "log.hash",
		Sets: "log.sets",
		ZSet: "log.zset",
	}
	FileTypesMap = map[string]FileType{
		"strs": Strs,
		"list": List,
		"hash": Hash,
		"sets": Sets,
		"zset": ZSet,
	}
)

type IOType int8

const (
	// FileIO standard file io.
	FileIO IOType = iota
	// MMap Memory Map.
	MMap
)

// LogFile is an abstraction of a disk file, entry`s read and write will go through it.
type LogFile struct {
	sync.RWMutex
	Fid        uint32
	WriteAt    int64
	IoSelector ioselector.IOSelector
}

// OpenLogFile open an existing or create a new log file.
// fsize must be a postitive number.And we will create io selector according to ioType.

func OpenLogFile(path string, fid uint32, fsize int64, ftype FileType, ioType IOType) (lf *LogFile, err error) {
	lf = &LogFile{
		Fid: fid,
	}
	// path + xx.log + fid
	fileName, err := lf.getLofFileName(path, fid, ftype)
	if err != nil {
		return nil, err
	}
	var selector ioselector.IOSelector
	switch ioType {
	case FileIO:
		if selector, err = ioselector.NewFileIOSelector(fileName, fsize); err != nil {
			return
		}
	case MMap:
		if selector, err = ioselector.NewMMapSelector(fileName, fsize); err != nil {
			return
		}
	default:
		return nil, ErrUnsupportedIoType
	}
	lf.IoSelector = selector
	return
}

// ReadLogEntry read a LogEntry from log file at offset.
// It returns a LogEntry, entry size and an error, if any.
// If offset is invalid, the err is io.EOF.
func (lf *LogFile) ReadLogEntry(offset int64) (*LogEntry, int64, error) {
	headerBuf, err := lf.readBytes(offset, MaxHeaderSize)
	if err != nil {
		return nil, 0, err
	}
	// size is real length of header
	header, size := decodeHeader(headerBuf)
	if header.crc32 == 0 && header.kSize == 0 && header.vSize == 0 {
		return nil, 0, ErrEndOfEntry
	}
	e := &LogEntry{
		Type:      header.typ,
		ExpiredAt: header.expiredAt,
	}

	kSize, vSize := int64(header.kSize), int64(header.vSize)

	var enterSize = size + kSize + vSize

	if kSize > 0 || vSize > 0 {
		kvBuf, err := lf.readBytes(offset+size, kSize+vSize)
		if err != nil {
			return nil, 0, err
		}
		e.Key = kvBuf[:header.kSize]
		e.Value = kvBuf[header.kSize:]
	}
	if crc := getEntryCrc(e, headerBuf[crc32.Size:size]); crc != header.crc32 {
		return nil, 0, ErrInvalidCrc
	}
	return e, enterSize, nil
}

func (lf *LogFile) Read(offset int64, size uint32) ([]byte, error) {
	if size < 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	if _, err := lf.IoSelector.Read(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil

}

// Write a byte slice at the end of log file.
// Returns an error, if any.
func (lf *LogFile) Write(buf []byte) error {
	if len(buf) <= 0 {
		return nil
	}

	offset := atomic.LoadInt64(&lf.WriteAt)
	n, err := lf.IoSelector.Write(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return ErrWriteSizeNotEqual
	}
	atomic.AddInt64(&lf.WriteAt, int64(n))
	return nil
}

func (lf *LogFile) Sync() error {
	return lf.IoSelector.Sync()
}

func (lf *LogFile) Close() error {
	return lf.IoSelector.Close()
}

// Delete delete current log file.
// File can`t be retrieved if do this, so use it carefully.

func (lf *LogFile) Delete() error {
	return lf.IoSelector.Delete()
}

func (lf *LogFile) readBytes(offset, n int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := lf.IoSelector.Read(buf, offset)
	return buf, err
}

func (lf *LogFile) getLofFileName(path string, fid uint32, ftype FileType) (name string, err error) {
	if _, ok := FileNamesMap[ftype]; !ok {
		return "", ErrUnsupportedLogFileType
	}
	fname := FileNamesMap[ftype] + fmt.Sprintf("%09d", fid)
	name = filepath.Join(path, fname)
	return
}
