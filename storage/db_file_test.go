package storage

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// func TestBuild(t *testing.T) {
// 	type args struct {
// 		path      string
// 		method    FileRWMethod
// 		blockSize int64
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		want    map[uint16]map[uint32]*DBFile
// 		want1   map[uint16]uint32
// 		wantErr bool
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, got1, err := Build(tt.args.path, tt.args.method, tt.args.blockSize)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("Build() got = %v, want %v", got, tt.want)
// 			}
// 			if !reflect.DeepEqual(got1, tt.want1) {
// 				t.Errorf("Build() got1 = %v, want %v", got1, tt.want1)
// 			}
// 		})
// 	}
// }

// func TestDBFile_Sync(t *testing.T) {
// 	type fields struct {
// 		Id     uint32
// 		Path   string
// 		File   *os.File
// 		mmap   mmap.MMap
// 		Offset int64
// 		method FileRWMethod
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		wantErr bool
// 	}{
// 		// TODO: Add test cases.
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			df := &DBFile{
// 				Id:     tt.fields.Id,
// 				Path:   tt.fields.Path,
// 				File:   tt.fields.File,
// 				mmap:   tt.fields.mmap,
// 				Offset: tt.fields.Offset,
// 				method: tt.fields.method,
// 			}
// 			if err := df.Sync(); (err != nil) != tt.wantErr {
// 				t.Errorf("Sync() error = %v, wantErr %v", err, tt.wantErr)
// 			}
// 		})
// 	}
// }

// func TestDBFile_Write(t *testing.T) {
// 	path := "/tmp/rosedb"
// 	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
// 	defer os.RemoveAll(path)

// 	var dbfile *DBFile
// 	var err error

// 	deadline := time.Now().Add(time.Second * 100).Unix()
// 	tests := []*Entry{
// 		NewEntryNoExtra([]byte("key-1"), []byte("val-1"), String, 0),
// 		NewEntry([]byte("key-2"), []byte("val-2"), []byte("extra-something"), String, 0),
// 		NewEntryWithExpire([]byte("key-3"), []byte("val-3"), deadline, String, 0),
// 		NewEntryWithTxn([]byte("key-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0),
// 	}

// 	t.Run("file-io", func(t *testing.T) {
// 		dbfile, err = NewDBFile(path, 0, FileIO, 1*1024*1024, String)
// 		if err != nil {
// 			panic(err)
// 		}
// 		for _, tt := range tests {
// 			err := dbfile.Write(tt)
// 			assert.Nil(t, err)
// 		}
// 	})

// 	t.Run("mmap", func(t *testing.T) {
// 		dbfile, err = NewDBFile(path, 1, MMap, 1024 , String)
// 		if err != nil {
// 			panic(err)
// 		}
// 		for _, tt := range tests {
// 			err := dbfile.Write(tt)
// 			assert.Nil(t, err)
// 		}
// 	})
// }

func TestNewDBFile(t *testing.T) {
	path := "/tmp/rosedb"
	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
	// defer os.RemoveAll(path)
	deadline := time.Now().Add(100 * time.Second).Unix()
	type args struct {
		path      string
		fileId    uint32
		method    FileRWMethod
		blockSize int64
		eType     uint16
		entrys    *Entry
	}

	tests := []struct {
		name    string
		args    args
		want    *DBFile
		wantErr bool
	}{
		{"f-string", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, String, NewEntryNoExtra([]byte("FileIOkey-1"), []byte("val-1"), String, 0)}, nil, false},
		{"f-list", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, List, NewEntry([]byte("FileIOkey-2"), []byte("val-2"), []byte("extra-something"), String, 0)}, nil, false},
		{"f-hash", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Hash, NewEntryWithExpire([]byte("FileIOkey-3"), []byte("val-3"), deadline, String, 0)}, nil, false},
		// {"f-set", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, Set, NewEntryWithTxn([]byte("FileIOkey-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0)}, nil, false},
		{"f-zset", args{"/tmp/rosedb", 0, FileIO, 1 * 1024 * 1024, ZSet, NewEntryNoExtra([]byte("FileIOkey-5"), []byte("val-5"), String, 0)}, nil, false},

		{"m-string", args{"/tmp/rosedb", 1, MMap, 1 * 1024 * 1024, String, NewEntryNoExtra([]byte("MMapkey-1"), []byte("val-1"), String, 0)}, nil, false},
		{"m-list", args{"/tmp/rosedb", 2, MMap, 2 * 1024 * 1024, List, NewEntry([]byte("MMapkey-2"), []byte("val-2"), []byte("extra-something"), String, 0)}, nil, false},
		{"m-hash", args{"/tmp/rosedb", 3, MMap, 3 * 1024 * 1024, Hash, NewEntryWithExpire([]byte("MMapkey-3"), []byte("val-3"), deadline, String, 0)}, nil, false},
		// {"m-set", args{"/tmp/rosedb", 4, MMap, 4 * 1024 * 1024, Set, NewEntryWithTxn([]byte("MMapkey-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0)}, nil, false},
		{"m-zset", args{"/tmp/rosedb", 5, MMap, 5 * 1024 * 1024, ZSet, NewEntryNoExtra([]byte("MMapkey-5"), []byte("val-5"), String, 0)}, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDBFile(tt.args.path, tt.args.fileId, tt.args.method, tt.args.blockSize, tt.args.eType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDBFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.NotNil(t, got)

			// 写入数据
			got.Write(tt.args.entrys)

			err = got.Close(true)
			if err != nil {
				log.Println(err)
			}

		})
	}

	t.Run("build-test", func(t *testing.T) {
		m1, m2, err := Build(path, FileIO, 1024*1024)
		if err != nil {
			log.Println(err)
		}

		for mm1, v := range m1 {
			for k, vv := range v {
				fmt.Printf("%d-->%d--%v\t", mm1, k, vv)
			}
			fmt.Println()
		}
		for k, v := range m2 {
			fmt.Printf("%d-->%d\n", k, v)
		}
	})

	
}

// func TestDBFile_Read(t *testing.T) {
// 	path := "/tmp/rosedb"
// 	_ = os.MkdirAll("/tmp/rosedb", os.ModePerm)
// 	defer os.RemoveAll(path)

// 	tt := func(method FileRWMethod, fileId uint32) {
// 		writeForRead(path, method)
// 		file, err := NewDBFile(path, fileId, method, 1024, String)
// 		if err != nil {
// 			panic(err)
// 		}
// 		offset := []int64{0, 44, 103, 147}
// 		for _, off := range offset {
// 			e, err := file.Read(off)
// 			assert.Nil(t, err)

// 			t.Logf("%+v\n", e)
// 			t.Logf("%+v\n", string(e.Meta.Key))
// 			assert.NotNil(t, e)
// 		}
// 	}

// 	t.Run("fileio", func(t *testing.T) {
// 		tt(FileIO, 0)
// 	})

// 	t.Run("mmap", func(t *testing.T) {
// 		tt(MMap, 0)
// 	})
// }

// func writeForRead(path string, method FileRWMethod) {
// 	deadline := time.Now().Add(time.Second * 100).Unix()
// 	tests := []*Entry{
// 		NewEntryNoExtra([]byte("key-1"), []byte("val-1"), String, 0),
// 		NewEntry([]byte("key-2"), []byte("val-2"), []byte("extra-something"), String, 0),
// 		NewEntryWithExpire([]byte("key-3"), []byte("val-3"), deadline, String, 0),
// 		NewEntryWithTxn([]byte("key-4"), []byte("val-4"), []byte("extra-something"), 101, String, 0),
// 	}

// 	file, err := NewDBFile(path, 0, method, 1024, String)
// 	if err != nil {
// 		panic(err)
// 	}

// 	for _, e := range tests {
// 		if err := file.Write(e); err != nil {
// 			panic(err)
// 		}
// 	}
// }
