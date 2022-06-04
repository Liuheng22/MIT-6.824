package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const TempDir = "tmp"
const TaskTimeout = 10

type TaskType int
type TaskStatus int

//任务类型
const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	ExitTask
)

//任务状态
const (
	NotStarted TaskStatus = iota
	Executing
	Finished
)

type Task struct {
	Type     TaskType
	Status   TaskStatus
	Index    int    //任务编号
	File     string //
	WorkerId int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nMap        int
	nReduce     int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceCount(args *GetReduceArgs, reply *GetReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.NReduce = len(c.reduceTasks)
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestReply) error {
	c.mu.Lock()

	var task *Task
	if c.nMap > 0 {
		task = c.getTask(c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
		task = c.getTask(c.reduceTasks, args.WorkerId)
	} else {
		// 没有相应的任务,给结束任务
		task = &Task{ExitTask, Finished, -1, "", -1}
	}

	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.TaskFile = task.File

	// 解锁然后返回
	c.mu.Unlock()
	go c.waitForTask(task)

	return nil
}

// 找任务来做
func (c *Coordinator) getTask(tasks []Task, workerId int) *Task {
	var task *Task
	for i := 0; i < len(tasks); i++ {
		if tasks[i].Status == NotStarted {
			//如果某个任务没有做，那么就分给这个worker
			//用的地址
			task = &tasks[i]
			task.Status = Executing
			task.WorkerId = workerId
			return task
		}
	}
	// 没有任务需要做
	return &Task{NoTask, Finished, -1, "", -1}
}

// 等待任务回复
func (c *Coordinator) waitForTask(task *Task) {
	if task.Type != MapTask && task.Type != ReduceTask {
		//非这两类任务，则返回
		return
	}

	//总的执行时间，如果10s还在执行，那么就重新置回未做，给别人做
	<-time.After(time.Second * TaskTimeout)

	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Status == Executing {
		task.Status = NotStarted
		task.WorkerId = -1
	}
}

// callback,回调任务完成
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	// 找到任务
	if args.Type == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.Type == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type to report:%v\n", args.Type)
	}

	// 只能回复，如果没有超时
	if args.WorkerId == task.WorkerId && task.Status == Executing {
		//只有对应上worker且目前还是executing，才修改状态
		task.Status = Finished
		// 任务完成，相应的任务计数修改
		if args.Type == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.Type == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}
	reply.CanExit = c.nMap == 0 && c.nReduce == 0
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	ret = c.nMap == 0 && c.nReduce == 0

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, c.nMap)
	c.reduceTasks = make([]Task, 0, c.nReduce)

	// 初始化map任务
	for i := 0; i < c.nMap; i++ {
		mtask := Task{MapTask, NotStarted, i, files[i], -1}
		c.mapTasks = append(c.mapTasks, mtask)
	}

	// 初始化reduce任务
	for i := 0; i < c.nReduce; i++ {
		rtask := Task{ReduceTask, NotStarted, i, "", -1}
		c.reduceTasks = append(c.reduceTasks, rtask)
	}

	c.server()

	// 清空相关文件
	//找到所有mr-out文件
	outFiles, _ := filepath.Glob("mr-out*")
	//清除
	for _, f := range outFiles {
		if err := os.Remove(f); err != nil {
			//出错了
			log.Fatalf("Cannot remove file %v\n", f)
		}
	}
	// 递归移除
	if err := os.RemoveAll(TempDir); err != nil {
		log.Fatalf("Cannt remove temporary directory %v\n", TempDir)
	}
	// 移除原先的，创建新的,以及dir模式
	err := os.Mkdir(TempDir, 0755)
	if err != nil {
		log.Fatalf("cannot create temporary directory%v\n", TempDir)
	}
	return &c
}
