package main

import (
	"fmt"
	"skipdb"
	"strconv"
	"time"
)

var cpuprofile = "cpuprofile.data"
var memprofile = "memprofile.data"

func main() {
	//{
	//	f, err := os.Create(cpuprofile)
	//	if err != nil {
	//		log.Fatal("could not create CPU profile: ", err)
	//	}
	//	if err := pprof.StartCPUProfile(f); err != nil {
	//		log.Fatal("could not start CPU profile: ", err)
	//	}
	//	defer pprof.StopCPUProfile()
	//}
	//{
	//	f, err := os.Create(memprofile)
	//	if err != nil {
	//		log.Fatal("could not create memory profile: ", err)
	//	}
	//	runtime.GC() // get up-to-date statistics
	//	if err := pprof.WriteHeapProfile(f); err != nil {
	//		log.Fatal("could not write memory profile: ", err)
	//	}
	//	f.Close()
	//}

	var sl *skipdb.SkipList
	var err error

	st := time.Now()
	sl, err = skipdb.Open("db.data")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 13; i++ {
		err = sl.Put([]byte("key_"+strconv.Itoa(i)), []byte("value_"+strconv.Itoa(i)))
		if err != nil {
			panic(err)
		}
	}

	err = sl.Put([]byte("key_test"), []byte("value_test"))
	if err != nil {
		panic(err)
	}

	sl.Dump(false)
	fmt.Println(time.Now().Sub(st))

	if err := sl.Close(); err != nil {
		panic(err)
	}
	fmt.Println(time.Now().Sub(st))
}
