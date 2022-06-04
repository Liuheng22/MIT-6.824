package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

var nReduce int

const TaskInterval = 200

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//得到reduce数量
func getReduceCount() (int, bool) {
	args := GetReduceArgs{}
	reply := GetReduceReply{}
	succ := call("Coordinator.GetReduceCount", &args, &reply)
	return reply.NReduce, succ
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	n, succ := getReduceCount()
	if succ == false {
		fmt.Println("Failed to get reduce task count,worker exiting")
		return
	}
	nReduce = n
	for {
		reply, succ := requestTask()
		// rpc调用出错
		if succ == false {
			fmt.Println("Failed to contact coordinator,worker exiting")
			return
		}
		if reply.TaskType == ExitTask {
			fmt.Println("All tasks are done,workder exiting.")
			return
		}

		//申请到了任务，就来做任务
		exit, succ := false, true
		if reply.TaskType == NoTask {
			//申请任务的时候，被申请完了，应该不会出现这种情况
		} else if reply.TaskType == MapTask {
			//做map任务
			doMap(mapf, reply.TaskFile, reply.TaskId)
			//完成之后，通知coodinator
			exit, succ = reportTask(MapTask, reply.TaskId)
		} else if reply.TaskType == ReduceTask {
			//做reduce任务
			doReduce(reducef, reply.TaskId)
			//完成之后，通知coodinator
			exit, succ = reportTask(ReduceTask, reply.TaskId)
		}

		// 全部完成，或者任务失败都结束
		if exit || !succ {
			fmt.Println("Master exited or all tasks done,worker exiting.")
			return
		}
		//任务完成之后，需要一些时间来间隔
		time.Sleep(TaskInterval * time.Millisecond)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMap(mapf func(string, string) []KeyValue, filePath string, mapId int) {
	//打开文件
	file, err := os.Open(filePath)
	checkError(err, "cannot open file %v\n", filePath)

	content, err := ioutil.ReadAll(file)
	checkError(err, "cannot read file %v\n", filePath)
	//读完之后关上文件
	file.Close()

	//得到处理结果
	kva := mapf(filePath, string(content))
	//将结果输出
	writeMapOutput(kva, mapId)
}

func writeMapOutput(kva []KeyValue, mapId int) {
	//
	prefix := fmt.Sprintf("%v/mr-%v", TempDir, mapId)
	//size = 0,cap = nreduce
	files := make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encoders := make([]*json.Encoder, 0, nReduce)

	//创建文件
	for i := 0; i < nReduce; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, os.Getpid())
		file, err := os.Create(filePath)
		checkError(err, "Cannt create file %v\n", filePath)
		buf := bufio.NewWriter(file)
		files = append(files, file)
		buffers = append(buffers, buf)
		encoders = append(encoders, json.NewEncoder(buf))
	}

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		err := encoders[idx].Encode(&kv)
		checkError(err, "cannt encode %v to file\n", kv)
	}

	for i, buf := range buffers {
		//刷到file
		err := buf.Flush()
		checkError(err, "can't flush buffer for file:%v\n", files[i].Name())
	}

	//原子改名字
	for i, file := range files {
		//写完关闭
		file.Close()
		newPath := fmt.Sprintf("%v-%v", prefix, i)
		err := os.Rename(file.Name(), newPath)
		checkError(err, "Cannot rename file%v\n", file.Name())
	}
}

func doReduce(reducef func(string, []string) string, reduceId int) {
	//将所有的文件都找到
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", TempDir, "*", reduceId))
	if err != nil {
		checkError(err, "Cannt list reduce files")
	}

	kvMap := make(map[string][]string)
	var kv KeyValue

	for _, filePath := range files {
		file, err := os.Open(filePath)
		checkError(err, "Cannt open file %v\n", filePath)

		//不知道数量的解析
		dec := json.NewDecoder(file)
		for dec.More() {
			err = dec.Decode(&kv)
			checkError(err, "Cannt decode from file %v\n", filePath)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	writeReduceOutput(reducef, kvMap, reduceId)
}

func writeReduceOutput(reducef func(string, []string) string, kvMap map[string][]string, reduceId int) {
	//排序
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	//创建tempfile
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", TempDir, reduceId, os.Getpid())
	file, err := os.Create(filePath)
	checkError(err, "cannot create file %v\n", filePath)

	//调用reduce
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		checkError(err, "cannot write mr output %v,%v to file\n", k, v)
	}

	//原子性改名字
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
	checkError(err, "cannot rename file %v\n", filePath)
}

func requestTask() (*RequestReply, bool) {
	args := RequestTaskArgs{os.Getpid()}
	reply := RequestReply{}
	succ := call("Coordinator.RequestTask", &args, &reply)
	return &reply, succ
}

func reportTask(taskType TaskType, taskId int) (bool, bool) {
	args := ReportTaskArgs{os.Getpid(), taskType, taskId}
	reply := ReportTaskReply{}
	succ := call("Coordinator.ReportTask", &args, &reply)

	return reply.CanExit, succ
}

func checkError(err error, format string, v ...interface{}) {
	if err != nil {
		log.Fatalf(format, v)
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
