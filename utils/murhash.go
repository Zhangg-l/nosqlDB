package utils

import (
	"encoding/binary"
	"io"

	"github.com/spaolacci/murmur3"
)

// hash算法
type Murmur128 struct {
	mur murmur3.Hash128
}

func NewMurmur128() *Murmur128 {
	return &Murmur128{mur: murmur3.New128()}
}

// 把写入mur，再通过 EncodeSum128的好哈希值
func (m *Murmur128) Write(p []byte) error {
	n, err := m.mur.Write(p)
	if n != len(p) {
		return io.ErrShortWrite
	}
	return err
}

func (m *Murmur128) EncodeSum128() []byte {

	buf := make([]byte, binary.MaxVarintLen64*2)

	s1, s2 := m.mur.Sum128()

	var index int
	// 对s1,s2 进行uint64编码，存入buf中并返回字节数量
	index += binary.PutUvarint(buf[index:], s1)
	index += binary.PutUvarint(buf[index:], s2)
	return buf[:index]
}

func (m *Murmur128) Reset() {
	m.mur.Reset()
}
