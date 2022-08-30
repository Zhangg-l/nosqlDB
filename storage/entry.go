package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

var (
	ErrInvalidEntry = errors.New("storage/entry: invalid entry")
	ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
	// KeySize, ValueSize, ExtraSize, crc32 is uint32 type，4 bytes each.
	// Timestamp 8 bytes, tx 8 bytes, state 2 bytes.
	// 4 * 4 + 8 + 8 + 2 = 34
	entryHeaderSize = 34
)

// data structure type of value, support five types now.
const (
	String uint16 = iota
	List
	Hash
	Set
	ZSet
)

type (
	Entry struct {
		Meta *Meta
		// state represents two fields, high 8 bits is the data type, low 8 bits is operation mark.
		state     uint16
		crc32     uint32
		TimeStamp uint64
		// TxId represents transaction id of an Entry.
		// TxId uint64
	}
	Meta struct {
		Key   []byte
		Value []byte
		// Extra info that operates the entry.
		Extra     []byte
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {

	return &Entry{
		state: state, TimeStamp: timestamp,
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
	}
}
func NewEntry(key, value, extra []byte, t, mark uint16) *Entry {
	var state uint16 = 0

	// set type & mark
	// 类型  low 8 最高 1 1 1 1 1 1 1 1
	state = state | (t << 8)

	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

// NewEntryNoExtra create a new entry without extra info.
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

// NewEntryWithExpire create a new Entry with expired info.
func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry {
	var state uint16 = 0

	// set type & mark
	// 类型    最高 1 1 1 1 1 1 1 1
	state = state | (t << 8)

	state = state | mark
	return newInternal(key, value, nil, state, uint64(deadline))
}

// func NewEntryWithTxn(key, value, extra []byte, t, mark uint16, txId uint64) *Entry {
// 	e := NewEntry(key, value, extra, t, mark)

// 	e.TxId = txId
// 	return e
// }

// Size the entry`s total size.
func (e *Entry) Size() uint32 {
	return entryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

// Encode encode the entry and returns a byte array.
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, ErrInvalidEntry
	}

	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize
	buf := make([]byte, e.Size())
	binary.LittleEndian.PutUint32(buf[4:8], ks)
	binary.LittleEndian.PutUint32(buf[8:12], vs)
	binary.LittleEndian.PutUint32(buf[12:16], es)
	binary.LittleEndian.PutUint16(buf[16:18], e.state)
	binary.LittleEndian.PutUint64(buf[18:26], (e.TimeStamp))
	// binary.LittleEndian.PutUint64(buf[26:34], (e.TxId))

	copy(buf[entryHeaderSize:entryHeaderSize+ks], e.Meta.Key)
	copy(buf[entryHeaderSize+ks:entryHeaderSize+ks+vs], e.Meta.Value)

	if es > 0 {
		copy(buf[entryHeaderSize+ks+vs:entryHeaderSize+ks+vs+es], e.Meta.Extra)
	}
	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// 读头
// Decode decode the byte array and return the entry.
func Decode(buf []byte) (*Entry, error) {
	if buf == nil || len(buf) == 0 {
		return nil, errors.New("invalid buf of decode")
	}
	entry := &Entry{
		Meta: &Meta{},
	}

	entry.crc32 = binary.LittleEndian.Uint32(buf[0:4])
	entry.Meta.KeySize = binary.LittleEndian.Uint32(buf[4:8])
	entry.Meta.ValueSize = binary.LittleEndian.Uint32(buf[8:12])
	entry.Meta.ExtraSize = binary.LittleEndian.Uint32(buf[12:16])
	entry.state = binary.LittleEndian.Uint16(buf[16:18])
	entry.TimeStamp = binary.LittleEndian.Uint64(buf[18:26])
	// entry.TxId = binary.LittleEndian.Uint64(buf[26:34])
	return entry, nil
}

// GetType state high 8 bits is data type.
func (e *Entry) GetType() uint16 {

	return e.state >> 8
}

// GetMark state low 8 bits is operation mark.
func (e *Entry) GetMark() uint16 {
	return e.state & (2<<7 - 1)
}
