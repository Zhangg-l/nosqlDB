package ioselector

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestNewFileSelector(t *testing.T) {
// 	testNewIOSelector(t, 1)
// }

// func TestFileSelector_writer(t *testing.T) {
// 	testIOSelectorWriter(t, 1)
// }
func TestFileSelector_Read(t *testing.T) {
	testIOSelectorRead(t, 1)
}
func testNewIOSelector(t *testing.T, ioType uint8) {
	type args struct {
		fName string
		fsize int64
	}
	tests := []struct {
		name string
		args args
	}{
		{"size-zero", args{fName: "000000001.wal", fsize: 0}},
		{
			"size-negative", args{fName: "000000002.wal", fsize: -1},
		},
		{
			"size-big", args{fName: "000000003.wal", fsize: 1024 << 20},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			absPath, err := filepath.Abs(filepath.Join("/tmp", tt.args.fName))
			assert.Nil(t, err)
			var got IOSelector

			if ioType == 0 {
				got, err = NewFileIOSelector(absPath, tt.args.fsize)
			}
			if ioType == 1 {
				got, err = NewMMapSelector(absPath, tt.args.fsize)
			}
			defer func() {
				if got != nil {
					err = got.Delete()
					assert.Nil(t, err)
				}
			}()

			if tt.args.fsize > 0 {
				assert.Nil(t, err)
				assert.NotNil(t, got)
			} else {

				assert.Equal(t, err, ErrInvalidFsize)
			}
		})
	}

}

func testIOSelectorWriter(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "ss000001.vlog"))
	assert.Nil(t, err)
	var size int64 = 1048576

	var selector IOSelector

	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, size)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, size)
	}

	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()

	type fields struct {
		selector IOSelector
	}

	type args struct {
		b      []byte
		offset int64
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil-byte", fields{selector: selector}, args{b: nil, offset: 0}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: []byte("0"), offset: 0}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: []byte("lotusdb"), offset: 0}, 7, false,
		},
		{
			"bigvalue-byte", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048576d", 123)), offset: 0}, 1048576, false,
		},
		{
			"exceed-size", fields{selector: selector}, args{b: []byte(fmt.Sprintf("%01048577d", 123)), offset: 0}, 1048577, false,
		},
		{
			"EOF-error", fields{selector: selector}, args{b: []byte("lotusdb"), offset: -1}, 0, true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.selector.Write(tt.args.b, tt.args.offset)
			if tt.want == 1048577 && ioType == 1 {
				tt.wantErr = true
				tt.want = 0
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Write() got = %v, want %v", got, tt.want)
			}
		})
	}

}

func testIOSelectorRead(t *testing.T, ioType uint8) {
	absPath, err := filepath.Abs(filepath.Join("/tmp", "ss001.wal"))
	assert.Nil(t, err)
	var selector IOSelector
	if ioType == 0 {
		selector, err = NewFileIOSelector(absPath, 100)
	}
	if ioType == 1 {
		selector, err = NewMMapSelector(absPath, 100)
	}

	defer func() {
		if selector != nil {
			_ = selector.Delete()
		}
	}()
	offsets := writeSomeDate(selector, t)
	results := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}

	type fields struct {
		selector IOSelector
	}

	type args struct {
		b      []byte
		offset int64
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{
			"nil", fields{selector: selector}, args{b: make([]byte, 0), offset: offsets[0]}, 0, false,
		},
		{
			"one-byte", fields{selector: selector}, args{b: make([]byte, 1), offset: offsets[1]}, 1, false,
		},
		{
			"many-bytes", fields{selector: selector}, args{b: make([]byte, 7), offset: offsets[2]}, 7, false,
		},
		{
			"EOF-1", fields{selector: selector}, args{b: make([]byte, 100), offset: -1}, 0, true,
		},
		{
			"EOF-2", fields{selector: selector}, args{b: make([]byte, 100), offset: 1024}, 0, true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.selector.Read(tt.args.b, tt.args.offset)

			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
			if !tt.wantErr {
				assert.Equal(t, tt.args.b, results[i])
			}
		})
	}

}

func writeSomeDate(selector IOSelector, t *testing.T) []int64 {
	tests := [][]byte{
		[]byte(""),
		[]byte("1"),
		[]byte("lotusdb"),
	}
	var offsets []int64
	var offset int64

	for _, tt := range tests {
		offsets = append(offsets, offset)
		n, err := selector.Write(tt, offset)
		assert.Nil(t, err)
		offset += int64(n)
	}

	return offsets
}
