package skipdb

// TODO 多线程的意义何在。 哪些地方可以充分利用并发。

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"unsafe"
)

// maxMapSize represents the largest mmap size supported by Bolt.
const maxMapSize = 0xFFFFFFFFFFFF // 256TB // TODO 和硬件平台有关系

// The largest step that can be taken when remapping the mmap.
const maxMmapStep = 1 << 30 // 1GB

const skipListHeaderSize = 1024

const headerNodeOffset = skipListHeaderSize

const nodeHeaderSize = int(unsafe.Offsetof(node{}.ptr))

const uintptrSize = int(unsafe.Sizeof(uintptr(0)))

var pageSize = os.Getpagesize()
var minFileSize = pageSize

const forwardSize = uintptrSize

const maxAllocSize = 0x7FFFFFFF

const nodeDelFlag = 0x00000001

const MaxKeySize = 0xFFFF

const MaxLevel = 64

const magic = 0x0706050403020100 // 魔数

var (
	errKeyNotFound = errors.New("key not found")
	errKeyTooLong = errors.New("key too long")
	errKeyNil = errors.New("key must not be a nil value")
	errNotDBFile = errors.New("not db file")
)

type skiplist struct {
	magic      uint64
	maxLevel   uint16
	level      uint16
	p          float32 // must less 1/2
	lastOffset uintptr // TODO 插入失败的时候，仍然会加上失败时的错误值
	count      int64

	//
	// skipIndex       uint64 // 插入导致的乱序指数 a->b->c->d = 0 a->c->d->b = 2
	linkDeleteCount uint64
	flagDeleteCount uint64
	// TODO 前几个文件
}

type SkipList struct {
	*skiplist

	mmapSize int64 // TODO int
	mmapAddr uintptr
	file     *os.File
	header   *node
	compare  func([]byte, []byte) int
}

// less db 的序列化和less函数有关系，所以必须保证同样的less函数，才可以使用
func Open(path string) (*SkipList, error) {
	var sl = &SkipList{
		compare: bytes.Compare,
	}

	var err error
	sl.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	if info, err := sl.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		if err := sl.init(); err != nil {
			return nil, nil
		}
	} else {
		if err := sl.mmap(); err != nil {
			return nil, err
		}

		if sl.magic != magic {
			return nil, errNotDBFile
		}

		sl.header = sl.node(headerNodeOffset)
	}

	return sl, nil
}

func (s *SkipList) init() error {
	// 初始化 一个页的数据
	if err := s.file.Truncate(int64(minFileSize)); err != nil {
		return err
	}

	if err := s.mmap(); err != nil {
		return err
	}

	s.skiplist.magic = magic
	s.skiplist.maxLevel = 32
	s.skiplist.level = 1
	s.skiplist.p = 0.25
	s.skiplist.lastOffset = headerNodeOffset //
	s.skiplist.skipIndex = 0
	s.skiplist.linkDeleteCount = 0
	s.skiplist.flagDeleteCount = 0

	var err error
	s.header, _, err = s.createNode(MaxLevel, []byte("header"), 0)
	if err != nil {
		return err
	}

	return nil
}

func (s *SkipList) Sync() error {
	return s.file.Sync()
}

func (s *SkipList) Close() error {
	if err := s.munmap(); err != nil {
		return err
	}

	// TODO 什么时候刷新内容到磁盘
	if err := s.file.Sync(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	return nil
}

func (s *SkipList) Get(key []byte) []byte {
	n, equal := s.find(key)
	if equal {
		n.key()
		fmt.Printf("key: %s, value: %s\n", n.key(), n.value) // TODO debug
		// getValue
		return nil // TODO find value
	}
	fmt.Printf("key: %s, not found\n", n.key()) // TODO debug
	return nil
}

func (s *SkipList) Del(key []byte) error {
	var (
		cur     *node
		next    *node
		updates = make([]*node, s.maxLevel)
	)

	cur = s.header
	for i := int(s.level - 1); i >= 0; i-- {
		for {
			next = s.node(cur.forwards()[i])
			if next == nil {
				break
			}

			if s.compare(key, next.key()) <= 0 {
				break
			}

			cur = next
		}
		updates[i] = cur
	}
	// 循环之后
	// cur 指向距离key最右的位置
	// next 执行距离key最左的位置，有可能等于key

	if next == nil || s.compare(key, next.key()) != 0 {
		return errKeyNotFound
	}

	s.linkDeleteCount++
	// s.flagDeleteCount++
	// next.del()

	for i := 0; i < int(s.level); i++ {
		updates[i].forwards()[i] = next.forwards()[i] // TODO: 暂时不考虑 临时存储 forwards. 需要测试
	}

	// TODO: 当存在多个 跳表 的时候，有可能会设置标记位

	s.count--
	return nil
}

func (s *SkipList) Put(key, value []byte) error {
	if key == nil {
		return errKeyNil
	}
	if len(key) > MaxKeySize {
		return errKeyTooLong
	}

	var (
		cur     *node
		next    *node
		updates = make([]uintptr, s.maxLevel)
	)

	cur = s.header
	for i := int(s.level - 1); i >= 0; i-- {
		for {
			next = s.node(cur.forwards()[i])
			if next == nil {
				break
			}

			diff := s.compare(key, next.key())
			if diff > 0 {
				cur = next
				continue
			}
			if diff < 0 {
				break
			}
			// fmt.Println("old node: ", s.DumpNode(next))
			next.setValue(uintptr(len(value))) // TODO value
			return nil
		}
		updates[i] = s.offset(cur)
	}

	level := s.randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			updates[i] = s.offset(s.header)
		}
		s.level = level
	}

	n, off, err := s.createNode(level, key, uintptr(len(value))) // TODO value
	if err != nil {
		return err
	}
	// fmt.Println("new node: ", s.DumpNode(n))
	for i := 0; i < int(level); i++ {
		n.forwards()[i] = s.node(updates[i]).forwards()[i] // NOTE: remap 会导致的 updates的指针失效
		s.node(updates[i]).forwards()[i] = off
	}

	//if n.forwards()[0] != 0 {
	//	s.skipIndex++
	//}

	s.count++
	return nil
}

func (s *SkipList) createNode(level uint16, key []byte, value uintptr) (*node, uintptr, error) {
	size := nodeHeaderSize + forwardSize*int(level) + len(key)
	off, err := s.allocate(size)
	if err != nil {
		return nil, 0, err
	}

	n := s.node(off)
	n.init(level, key, value)
	return n, off, nil
}

func (s *SkipList) find(key []byte) (n *node, equal bool) {
	var (
		cur  *node
		next *node
	)

	cur = s.header
	for i := int(s.level - 1); i >= 0; i-- {
		for {
			next = s.node(cur.forwards()[i])
			if next == nil {
				break
			}

			diff := s.compare(key, next.key())
			if diff > 0 {
				cur = next
				continue
			}
			if diff < 0 {
				break
			}
			return next, true
		}
	}

	return cur, false
}

func (s *SkipList) node(offset uintptr) *node {
	if offset <= 0 {
		return nil
	}

	return (*node)(unsafe.Pointer(s.mmapAddr + offset))
}

func (s *SkipList) offset(n *node) uintptr {
	return uintptr(unsafe.Pointer(n)) - s.mmapAddr
}

func (s *SkipList) adjustMmapSize(size int64) (int64, error) {
	// Double the size from 32KB until 1GB.
	for i := uint(12); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// Verify the requested size is not above the maximum allowed.
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// If larger than 1GB then grow by 1GB at a time.
	sz := size
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder // TODO 正好也不够
	}

	// Ensure that the mmap size is a multiple of the page size.
	// This should always be true since we're incrementing in MBs.
	pageSize := int64(pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// If we've exceeded the max size then only grow up to the max size.
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return sz, nil
}

// 优化代码。这里的代码功能不是很内聚耦合
func (s *SkipList) mmap() error {
	info, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if info.Size() < int64(minFileSize) {
		return errors.New("file size too small")
	}

	size, err := s.adjustMmapSize(info.Size())
	if err != nil {
		return err
	}

	addr, err := mmap(s, int(size))
	if err != nil {
		return err
	}

	s.mmapAddr = addr
	s.mmapSize = int64(size) // TODO int64
	s.skiplist = (*skiplist)(unsafe.Pointer(s.mmapAddr))
	s.header = s.node(headerNodeOffset)
	return nil
}

func (s *SkipList) munmap() error {
	return munmap(s)
}

func (s *SkipList) mremap() error {
	size, err := s.adjustMmapSize(s.mmapSize + 1)
	if err != nil {
		return err
	}
	if err := s.file.Truncate(int64(size)); err != nil {
		return err
	}
	addr, err := mremap(s, int(size))
	if err != nil {
		return err
	}

	s.mmapAddr = addr
	s.mmapSize = int64(size)
	s.skiplist = (*skiplist)(unsafe.Pointer(s.mmapAddr))
	s.header = s.node(headerNodeOffset)
	return nil
}

func (s *SkipList) DumpNode(n *node) string {
	return fmt.Sprintf(
		"node(%d){flag: %d, level: %d, keyLen: %d, value: %d, forwards: %v, key: %s}",
		uintptr(unsafe.Pointer(n))-s.mmapAddr,
		n.flag,
		n.level,
		n.keyLen,
		n.value,
		n.forwards(),
		string(n.key()),
	)
}

func (s *SkipList) Dump(dumpNode bool) {
	fmt.Println("=========== dump ===========")
	fmt.Printf(
		"SkipList{level: %d, lastOffset: %d, count: %d, skipIndex: %d, linkDeleteCount: %d, flagDeleteCount: %d, mmapAddr: %d, mmapSize: %d}\n",
		s.level,
		s.lastOffset,
		s.count,
		s.skipIndex,
		s.linkDeleteCount,
		s.flagDeleteCount,
		s.mmapAddr,
		s.mmapSize,
	)

	fmt.Println()
	if dumpNode {
		for cur := s.header; cur != nil; cur = s.node(cur.forwards()[0]) {
			fmt.Println(s.DumpNode(cur))
		}
	}
	fmt.Println("=========== dump over ===========")
}

func (s *SkipList) randomLevel() uint16 {
	var level uint16 = 1

	a := int64(s.p * 0xFFFF)
	for rand.Int63()&0xFFFF < a {
		level++
	}

	if level > s.maxLevel {
		return s.maxLevel
	}
	return level
}

// 从 mmap 最后 分配足够的字节数
func (s *SkipList) allocate(cnt int) (uintptr, error) {
	addr := s.lastOffset

	for int64(s.lastOffset)+int64(cnt) > s.mmapSize {
		if err := s.mremap(); err != nil {
			return 0, err
		}
	}

	s.lastOffset += uintptr(cnt)
	return addr, nil
}

type node struct {
	flag   uint16
	level  uint16
	keyLen uint32
	value  uintptr
	ptr    uintptr
}

func (n *node) String() string {
	return fmt.Sprintf(
		`node{flag: %d, level: %d, keyLen: %d, value: %d, forwards: %v, key: %s}\n`,
		n.flag,
		n.level,
		n.keyLen,
		n.value,
		n.forwards(),
		string(n.key()),
	)
}

func (n *node) forwards() []uintptr {
	return *(*[]uintptr)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&n.ptr)),
		Len:  int(n.level),
		Cap:  int(n.level),
	}))
}

func (n *node) key() []byte {
	keyAddr := uintptr(unsafe.Pointer(&n.ptr)) + uintptr(forwardSize)*uintptr(n.level)

	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: keyAddr,
		Len:  int(n.keyLen),
		Cap:  int(n.keyLen),
	}))
}

// 还是说直接赋值
func (n *node) setValue(value uintptr) {
	n.value = value
}

func (n *node) setKey(key []byte) {
	n.keyLen = uint32(len(key))
	copy(n.key(), key)
}

func (n *node) del() {
	n.flag = nodeDelFlag
}

func (n *node) init(level uint16, key []byte, value uintptr) {
	n.flag = 0
	n.level = level
	n.keyLen = uint32(len(key))
	n.value = value

	forwards := n.forwards()
	for i := range forwards {
		forwards[i] = 0
	}

	copy(n.key(), key)
}
