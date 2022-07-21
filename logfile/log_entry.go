package logfile

import (
	"encoding/binary"
	"hash/crc32"
)

// MaxHeaderSize max entry header size.
// crc32	typ    kSize	vSize	expiredAt
//  4    +   1   +   5   +   5    +    10      = 25 (refer to binary.MaxVarintLen32 and binary.MaxVarintLen64)

const MaxHeaderSize = 25

type EntryType byte

const (
	// TypeDelete represents entry type is delete.
	TypeDelete EntryType = iota + 1
	// TypeListMeta represents entry is list
	TypeListMeta
)

type LogEntry struct {
	Key       []byte
	Value     []byte
	ExpiredAt int64
	Type      EntryType
}
type entryHeader struct {
	crc32     uint32
	typ       EntryType
	kSize     uint32
	vSize     uint32
	expiredAt int64 // time.Unix
}

// EncodeEntry will encode entry into a byte slice.
// The encoded Entry looks like:
// +-------+--------+----------+------------+-----------+-------+---------+
// |  crc  |  type  | key size | value size | expiresAt |  key  |  value  |
// +-------+--------+----------+------------+-----------+-------+---------+
// |------------------------HEADER----------------------|
//         |--------------------------crc check---------------------------|

func EncodeEntery(e *LogEntry) ([]byte, int) {
	if e == nil {
		return nil, 0
	}

	header := make([]byte, MaxHeaderSize)
	header[4] = byte(e.Type)
	var index = 5
	// 写变长度的编码
	index += binary.PutVarint(header[index:], int64(len(e.Key)))
	index += binary.PutVarint(header[index:], int64(len(e.Value)))
	index += binary.PutVarint(header[index:], e.ExpiredAt)

	var size = index + len(e.Key) + len(e.Value)
	buf := make([]byte, size)
	copy(buf[:index], header[:])
	// key and value

	copy(buf[index:], e.Key)
	copy(buf[index+len(e.Key):], e.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	// 小endian适合内存 big适合网络传输
	binary.LittleEndian.PutUint32(buf[:4], crc)
	return buf, size
}

func decodeHeader(buf []byte) (*entryHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	h := &entryHeader{
		crc32: binary.LittleEndian.Uint32(buf[:4]),
		typ:   EntryType(buf[4]),
	}
	var index = 5

	ksize, n := binary.Varint(buf[index:])

	h.kSize = uint32(ksize)
	index += n

	vsize, n := binary.Varint(buf[index:])
	h.vSize = uint32(vsize)
	index += n

	expiredAt, n := binary.Varint(buf[index:])

	h.expiredAt += expiredAt
	return h, int64(index + n)
}

func getEntryCrc(e *LogEntry, h []byte) uint32 {
	if e == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(h[:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc

}
