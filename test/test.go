package main

import (
	"fmt"
	"strings"
)

type Call struct {
	Num1 int
	Num2 int
}

func f1(c interface{}) {
	cc := c.(Call)
	fmt.Println(cc.Num1 + cc.Num2)
}

func main() {
	str := " set hah 2a "
	str = strings.TrimSpace(str)
	cc := strings.Split(str," ")
	fmt.Println(cc)
}
