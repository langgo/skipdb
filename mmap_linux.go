package skipdb

import (
	"fmt"
	"syscall"
	"unsafe"
	"reflect"
)

// mmap memory maps a DB's data file.
func mmap(sl *SkipList, sz int) (data uintptr, err error) {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(sl.file.Fd()), 0, sz, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return 0, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := syscall.Madvise(b, syscall.MADV_RANDOM); err != nil {
		return 0, fmt.Errorf("madvise: %s", err)
	}


	return uintptr(unsafe.Pointer(&b[0])), nil
}

func munmap(sl *SkipList) (err error) {
	if sl.mmapAddr == 0 {
		return nil
	}

	b := *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: sl.mmapAddr,
		Len:  int(sl.mmapSize),
		Cap:  int(sl.mmapSize),
	}))

	sl.mmapAddr = 0
	sl.mmapSize = 0
	return syscall.Munmap(b)
}

func mremap(sl *SkipList, sz int) (data uintptr, err error){
	if err := munmap(sl); err != nil {
		return 0, err
	}

	return mmap(sl, sz)
}

// TODO go 中没有找到 MREMAP_MAYMOVE 这个常量
func _mremap(old_address uintptr, old_size int, new_size int, flags int) (xaddr uintptr, err error) {
	r0, _, e1 := syscall.Syscall6(syscall.SYS_MREMAP, uintptr(old_address), uintptr(old_size), uintptr(new_size), uintptr(flags), 0, 0)
	xaddr = uintptr(r0)
	if e1 != 0 {
		err = fmt.Errorf("syscall err: %d", 0)
	}
	return
}
