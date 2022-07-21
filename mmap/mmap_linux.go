package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	// lenght 代表将文件中多大的部分映射到内存
	// MAP_SHARED对映射区域的写入数据会复制回文件内，而且允许其他映射该文件的进程共享。
	// offset 文件映射的偏移量，通常设置为0，代表从文件最前方开始对应，offset必须是分页大小的整数倍。 文件映射的偏移量，通常设置为0，代表从文件最前方开始对应，offset必须是分页大小的整数倍。
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// mremap is a Linux-specific system call to remap pages in memory. This can be used in place of munmap + mmap.

// func mremap(data []byte,size int)([]byte,error){

// }
// 取消data指向的映射
func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
 
	if errno != 0 {
		return errno
	}
	return nil
}

func madvise(b []byte, readahead bool) error {
	// 无建议，按照一般方式处理内核的行为不变，执行适当数量的预读操作
	flags := unix.MADV_NORMAL
	if !readahead {
		// 以随机（非顺序）方式访问指定页面内核禁用预读，每次实际读取操作时会进行最少量的读数据
		flags = unix.MADV_RANDOM
	}
	return unix.Madvise(b, flags)
}
// 数据同步到磁盘
// msync writes any modified data to persistent storage.
func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
