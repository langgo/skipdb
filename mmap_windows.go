package skipdb

import (
	"syscall"
	"os"
)

func mmap(sl *SkipList, sz int) (data uintptr, err error) {
	// Open a file mapping handle.
	sizelo := uint32(sz >> 32)
	sizehi := uint32(sz) & 0xffffffff
	h, errno := syscall.CreateFileMapping(syscall.Handle(sl.file.Fd()), nil, syscall.PAGE_READWRITE, sizelo, sizehi, nil)
	if h == 0 {
		return 0, os.NewSyscallError("CreateFileMapping", errno)
	}

	// Create the memory map.
	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ|syscall.FILE_MAP_WRITE, 0, 0, uintptr(sz))
	if addr == 0 {
		return 0, os.NewSyscallError("MapViewOfFile", errno)
	}

	// Close mapping handle.
	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return 0, os.NewSyscallError("CloseHandle", err)
	}

	return addr, nil
}

func munmap(sl *SkipList) (err error) {
	if sl.mmapAddr == 0 {
		return nil
	}

	if err := syscall.UnmapViewOfFile(sl.mmapAddr); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}

	sl.mmapAddr = 0
	sl.mmapSize = 0
	return nil
}

// TODO windows 没有找到 remap
func mremap(sl *SkipList, new_size int) (addr uintptr, err error) {
	if err := munmap(sl); err != nil {
		return 0, err
	}

	return mmap(sl, new_size)
}
