package ioselector

import (
	"go_code/project13/rosedb/mmap"
	"io"
	"os"
)

// MMapSelector represents using memory-mapped file I/O.

type MMapselector struct {
	fd     *os.File
	buf    []byte
	bufLen int64
}

func NewMMapSelector(fName string, fSize int64) (IOSelector, error) {

	if fSize <= 0 {
		return nil, ErrInvalidFsize
	}
	file, err := openFile(fName, fSize)
	if err != nil {
		return nil, err
	}

	buf, err := mmap.Mmap(file, true, fSize)
	if err != nil {
		return nil, err
	}
	return &MMapselector{
		fd:    file,
		buf:   buf,
		bufLen: fSize,
	}, nil

}

func (lm *MMapselector) Write(b []byte, offset int64) (int, error) {
	length := int64(len(b))
	if length <= 0 {
		return 0, nil
	}

	if offset < 0 || length+offset > lm.bufLen {
		return 0, io.EOF
	}

	return copy(lm.buf[offset:], b), nil

}

func (lm *MMapselector) Read(b []byte, offset int64) (int, error) {
	if offset < 0 || offset > lm.bufLen {
		return 0, io.EOF
	}

	if offset+int64(len(b)) >= lm.bufLen {
		return 0, io.EOF
	}

	return copy(b, lm.buf[offset:]), nil

}

func (lm *MMapselector) Sync() error {
	return mmap.Msync(lm.buf)
}

func (lm *MMapselector) Close() error {
	if err := mmap.Msync(lm.buf); err != nil {
		return err
	}
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	return lm.fd.Close()
}
func (lm *MMapselector) Delete() error {
	if err := mmap.Munmap(lm.buf); err != nil {
		return err
	}
	lm.buf = nil

	if err := lm.fd.Truncate(0); err != nil {
		return err
	}
	if err := lm.fd.Close(); err != nil {
		return err
	}
	return os.Remove(lm.fd.Name())
}
