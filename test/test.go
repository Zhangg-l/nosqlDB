package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	buf := make([]byte, 25)
	n := binary.PutVarint(buf, int64(259))
	fmt.Println(n)
	fmt.Println(binary.MaxVarintLen16)
	
}
func tTruncate_1() {
	f, err := os.OpenFile(`haha.txt`, os.O_RDWR, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	s, err := f.Seek(0, io.SeekEnd)
	if err != nil {

		log.Fatalf(" f.Seek(0, io.SeekEnd):%v", err)
	}
	fmt.Println(s)

	_, err = f.WriteString("a32")
	if err != nil {
		log.Fatalf(" f.WriteString():%v", err)
	}
	// Truncate方法截取长度为size，即删除后面的内容，不管当前的偏移量在哪儿，都是从头开始截取
	// 但是其不会影响当前的偏移量
	err = f.Truncate(s)
	if err != nil {
		log.Fatalf(" f.Truncate:%v", err)
	}
	_, _ = f.Seek(0, io.SeekEnd)
	_, err = f.WriteString("sse")
	if err != nil {
		log.Fatalf(" f.WriteString:%v", err)
	}

}
