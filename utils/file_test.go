package utils

import (
	"log"
	"testing"
)

// func TestExist1(t *testing.T) {
// 	t.Log(os.TempDir() + "ssds")

// 	exist := Exist(os.TempDir() + "ssds")
// 	t.Log(exist)

// 	if err := os.MkdirAll(os.TempDir()+storage.PathSeparator+"abcd", storage.FilePerm); err != nil {
// 		t.Error(err)
// 	}
// }

// func TestExist2(t *testing.T) {
// 	//目录是否存在
// 	path := "/tmp/rosedb"

// 	t.Log(Exist(path))

// 	//文件是否存在
// 	t.Log(Exist(path + "/000w000000.data"))

// 	t.Log(os.TempDir())
// }

// func TestCopyFile(t *testing.T) {
// 	src := "/tmp/dbtest1/text.txt"
// 	dst := "/tmp/dbtest2/text.txt"

// 	err := CopyFile(src, dst)
// 	if err != nil {
// 		t.Error(err)
// 	}
// }
func TestCopyDir(t *testing.T) {
	src := "/tmp/dbtest1"
	dst := "/tmp/dbtest2"

	err := CopyDir(src, dst)
	if err != nil {
		log.Fatal(err)
	}
}
