package mr

import (
	"encoding/json"
	"fmt"
	// "github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	writeMutex sync.Mutex
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// var wg sync.WaitGroup
	// uuidW := uuid.New()

	args := ExampleArgs{}
	reduce := ExampleReply{}
	ok := call("Coordinator.GetReduceN", &args, &reduce)
	if !ok {
		log.Fatal("Coordinator.GetReduceN failed")
	}
	for {
		// Your worker implementation here.
		args := GetFileArgs{}
		// args.Uid = uuidW
		reply := GetFileReply{}
		ok := call("Coordinator.GetFileName", &args, &reply)
		// log.Printf("%+v", rep)
		if !ok {
			log.Fatal("Coordinator.GetFileName failed")
		} else if reply.IfProcessEnd {
			break
		} else if reply.IfSendEnd {
			time.Sleep(time.Second)
		} else {
			//wg.Add(1)
			//go func(reply GetFileReply) {
			// defer wg.Done()
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			defer file.Close()
			kva := mapf(reply.FileName, string(content))

			sort.Sort(ByKey(kva))

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			writeMutex.Lock()
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				oname := "mr-" + strconv.Itoa(reply.MapNum) + "-" + strconv.Itoa(ihash(kva[i].Key)%reduce.Y)
				// log.Printf("%v", oname)
				file, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("Failed to open or create file: %v", err)
				}
				enc := json.NewEncoder(file)
				for k := i; k < j; k++ {
					err := enc.Encode(&kva[i])
					if err != nil {
						log.Fatalf("Failed to encode %v", err)
					}
				}
				err = file.Close()
				if err != nil {
					return
				}
				// this is the correct format for each line of Reduce output.
				// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			writeMutex.Unlock()
			rargs := ReadArgs{}
			rargs.FileName = reply.FileName
			rargs.MapNum = reply.MapNum
			rrep := ReadReply{}
			ok := call("Coordinator.ReadSuccess", &rargs, &rrep)
			if !ok {
				log.Fatal("Coordinator.ReadSuccess failed")
			} else {
				// log.Printf("%v read %v success", rep.MapNum, rep.FileName)
			}
			//}(rep)
		}
	}
	// wg.Wait()

	for {
		args := GetReduceArgs{}
		rep := GetReduceReply{}
		ok := call("Coordinator.GetReduceNo", &args, &rep)
		// log.Printf("%+v", rep)
		if !ok {
			log.Fatal("Coordinator.GetReduceNo failed")
		} else if rep.IfProcessEnd {
			break
		} else if rep.IfSendEnd {
			time.Sleep(time.Second)
		} else {
			// wg.Add(1)
			// go func(i int) {
			//defer wg.Done()
			i := rep.ReduceNum
			// log.Printf("%v", i)
			files, err := filepath.Glob("mr-*-" + strconv.Itoa(i))
			if err != nil {
				fmt.Printf("Error finding files: %v\n", err)
				continue
			}
			// log.Printf("%v", files)

			// 检查是否有符合条件的文件
			if len(files) == 0 {
				// fmt.Println("No matching files found")
				// return
			}

			// 遍历并读取符合条件的文件
			kva := []KeyValue{}
			for _, file := range files {
				file, err := os.OpenFile(file, os.O_RDONLY, 0644)
				if err != nil {
					log.Fatalf("Failed to open file: %v", err)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(i)
			ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("Failed to open or create file: %v", err)
			}
			q := 0
			for q < len(kva) {
				j := q + 1
				for j < len(kva) && kva[j].Key == kva[q].Key {
					j++
				}
				values := []string{}
				for k := q; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[q].Key, values)
				// log.Printf("%v %v %v", kva[q].Key, values, output)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[q].Key, output)
				q = j
			}
			rargs := ReduceArgs{}
			rargs.ReduceNum = i
			rrep := ReduceReply{}
			ok := call("Coordinator.ReduceSuccess", &rargs, &rrep)
			if !ok {
				log.Fatal("Coordinator.ReduceSuccess failed")
			} else {
				// log.Printf("%v reduce success", i)
			}
			// }(rep.ReduceNum)
		}
	}
	// wg.Wait()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//arg := ExampleArgs{}
	//reps := ExampleReply{}
	//err := call("Coordinator.Handler", &arg, &reps)
	//for !err {
	//	err = call("Coordinator.Handler", &arg, &reps)
	//}

	// 匹配所有 mr-* 文件
	//files, errrm := filepath.Glob("mr-*-*")
	//if errrm != nil {
	//	log.Fatalf("Failed to list files: %v", errrm)
	//}

	// 使用正则表达式筛选出符合 "mr-数字-数字" 格式的文件
	//pattern := regexp.MustCompile(`^mr-\d+-\d+$`)
	//for _, file := range files {
	//	if pattern.MatchString(file) {
	//		// 删除符合条件的文件
	//		err := os.Remove(file)
	//		if err != nil {
	//			log.Printf("Failed to delete file %s: %v", file, err)
	//		} else {
	//			// log.Printf("Deleted file: %s", file)
	//		}
	//	}
	//}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
