package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	copy2()
}

func copy1() {
	src := make([]byte, 1024*128*8)
	dst := make([]byte, 1024*128*8)

	st := time.Now()
	g := 0
	for i := 0; i < 100*1024; i++ {
		src[0] = byte(i)
		src[1024] = byte(i + 1)
		g++
		copy(dst, src)
	}
	fmt.Println("单线程：", time.Now().Sub(st), g)
}

func copy2() {
	st := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			src := make([]byte, 1024*128*8)
			dst := make([]byte, 1024*128*8)
			for i := 0; i < 100*1024/4; i++ {
				src[0] = byte(i)
				src[1024] = byte(i + 1)
				copy(dst, src)
			}
		}()
	}
	wg.Wait()
	fmt.Println("4线程：", time.Now().Sub(st))
}
