package logfile

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestOpenLogFile(t *testing.T) {
// 	testOpenLogFile(t, FileIO)
// 	testOpenLogFile(t, MMap)
// }

func testOpenLogFile(t *testing.T, iotype IOType) {

	type args struct {
		path   string
		fid    uint32
		fsize  int64
		ftype  FileType
		ioType IOType
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"zero-size", args{"/tmp", 0, 0, List, iotype}, true},
		{"zero-normal", args{"/tmp", 1, 100, List, iotype}, false},
		{"zero-big", args{"/tmp", 2, 64 << 1, List, iotype}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lFile, err := OpenLogFile(tt.args.path, tt.args.fid, tt.args.fsize, tt.args.ftype, tt.args.ioType)
			if err != nil && !tt.wantErr {
				t.Errorf("OpenLogFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer func() {
				if lFile != nil && lFile.IoSelector != nil {
					_ = lFile.Delete()
				}
			}()
			if !tt.wantErr && lFile == nil {
				t.Errorf("OpenLogFile() gotLf =nil, want not nil")
			}
		})
	}
}

// func TestLogFileWrite(t *testing.T) {
// 	// testLogFileWrite(t,FileIO)
// 	testLogFileWrite(t,MMap)
// }

func testLogFileWrite(t *testing.T, iotype IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, List, iotype)
	assert.Nil(t, err)
	assert.NotNil(t, lf)

	type args struct {
		buf []byte
	}
	type fields struct {
		lf *LogFile
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"nil", fields{lf: lf}, args{buf: nil}, false},
		{"no-value", fields{lf: lf}, args{buf: []byte{}}, false},
		{"normal-1", fields{lf: lf}, args{buf: []byte("lotusdb")}, false},
		{"normal-2", fields{lf: lf}, args{buf: []byte("some data")}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fields.lf.Write(tt.args.buf); err != nil && !tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// func TestLogFile_Read(t *testing.T) {
// 	t.Run("fileio", func(t *testing.T) {
// 		testLogFileRead(t, FileIO)
// 	})
// 	t.Run("mmap", func(t *testing.T) {
// 		testLogFileRead(t, MMap)
// 	})
// }

func testLogFileRead(t *testing.T, iotype IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, List, iotype)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()

	data := [][]byte{
		[]byte("0"),
		[]byte("some data"),
		[]byte("some data 1"),
		[]byte("some data 2"),
		[]byte("some data 3"),
		[]byte("lotoutdb"),
	}

	offset := writeSomeData(lf, data)
	type fields struct {
		lf *LogFile
	}

	type args struct {
		offset int64
		size   uint32
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"read-0", fields{lf: lf}, args{offset[0], uint32(len(data[0]))}, data[0], false},
		{"read-1", fields{lf: lf}, args{offset[1], uint32(len(data[1]))}, data[1], false},
		{"read-2", fields{lf: lf}, args{offset[2], uint32(len(data[2]))}, data[2], false},
		{"read-3", fields{lf: lf}, args{offset[3], uint32(len(data[3]))}, data[3], false},
		{"read-4", fields{lf: lf}, args{offset[4], uint32(len(data[4]))}, data[4], false},
		{"read-5", fields{lf: lf}, args{offset[5], uint32(len(data[5]))}, data[5], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fields.lf.Read(tt.args.offset, tt.args.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Read() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func writeSomeData(lf *LogFile, data [][]byte) []int64 {
	var offset []int64
	for _, v := range data {
		off := atomic.LoadInt64(&lf.WriteAt)
		offset = append(offset, off)
		if err := lf.Write(v); err != nil {
			panic(fmt.Sprintf("write data err.%+v", err))
		}
	}

	return offset
}

func TestLogFile_ReadLogEntry(t *testing.T) {
	t.Run("fileio",func(t *testing.T) {
		testLogFileReadLogEntry(t,FileIO)
	})
	t.Run("mmap",func(t *testing.T) {
		testLogFileReadLogEntry(t,MMap)
	})
}

func testLogFileReadLogEntry(t *testing.T, iotype IOType) {
	lf, err := OpenLogFile("/tmp", 1, 1<<20, Strs, iotype)
	assert.Nil(t, err)
	defer func() {
		if lf != nil {
			_ = lf.Delete()
		}
	}()
	entries := []*LogEntry{
		{ExpiredAt: 123332, Type: 0},
		{ExpiredAt: 123332, Type: TypeDelete},
		{Key: []byte(""), Value: []byte(""), ExpiredAt: 994332343, Type: TypeDelete},
		{Key: []byte("k1"), Value: nil, ExpiredAt: 7844332343},
		{Key: nil, Value: []byte("lotusdb"), ExpiredAt: 99400542343},
		{Key: []byte("k2"), Value: []byte("lotusdb"), ExpiredAt: 8847333912},
		{Key: []byte("k3"), Value: []byte("some data"), ExpiredAt: 8847333912, Type: TypeDelete},
	}

	var vals [][]byte

	for _, e := range entries {
		v, _ := EncodeEntery(e)
		vals = append(vals, v)
	}
	offsets := writeSomeData(lf, vals)

	type fields struct {
		lf *LogFile
	}

	type args struct {
		offset int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *LogEntry
		want1   int64
		wantErr bool
	}{
		{
			"read-entry-0", fields{lf: lf}, args{offset: offsets[0]}, entries[0], int64(len(vals[0])), false,
		},
		{
			"read-entry-1", fields{lf: lf}, args{offset: offsets[1]}, entries[1], int64(len(vals[1])), false,
		},
		{
			"read-entry-2", fields{lf: lf}, args{offset: offsets[2]}, &LogEntry{ExpiredAt: 994332343, Type: TypeDelete}, int64(len(vals[2])), false,
		},
		{
			"read-entry-3", fields{lf: lf}, args{offset: offsets[3]}, &LogEntry{Key: []byte("k1"), Value: []byte{}, ExpiredAt: 7844332343}, int64(len(vals[3])), false,
		},
		{
			"read-entry-4", fields{lf: lf}, args{offset: offsets[4]}, &LogEntry{Key: []byte{}, Value: []byte("lotusdb"), ExpiredAt: 99400542343}, int64(len(vals[4])), false,
		},
		{
			"read-entry-5", fields{lf: lf}, args{offset: offsets[5]}, entries[5], int64(len(vals[5])), false,
		},
		{
			"read-entry-6", fields{lf: lf}, args{offset: offsets[6]}, entries[6], int64(len(vals[6])), false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.fields.lf.ReadLogEntry(tt.args.offset)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadLogEntry() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("ReadLogEntry() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
