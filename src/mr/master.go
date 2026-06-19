package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


type Master struct {
	// Your definitions here.
	files []string
	taskStates []int
	reduceStates []int
	nReduce int
	mu sync.Mutex
	
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *WorkerArgs, reply *WorkerReply) error {
	reply.Nreduce = m.nReduce
	reply.TaskType = 2


	reply.NMap = len(m.files)
	m.mu.Lock()
	defer m.mu.Unlock()
	check := true

	for i:=0; i<len(m.taskStates);i++{
		if m.taskStates[i] != 2 {
        	check = false
        	
   		}
	}

	if check == false {
		for i:=0; i<len(m.taskStates);i++{
			
			if m.taskStates[i]==0{
				m.taskStates[i]=1
				go m.monitorTask(0,i)
				reply.TaskNumber = i
				reply.FileName = m.files[i]
				reply.TaskType= 0
				
				return nil
			}
			

		}
	}else {
		for i:=0; i<m.nReduce;i++{
			if m.reduceStates[i]==0{
				m.reduceStates[i]=1
				go m.monitorTask(1,i)
				reply.TaskNumber=i
				
				reply.TaskType = 1
				return nil
			}
		}
	}

	return nil
}


func (m* Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	Taskid := args.TaskId
	if args.TaskType == 0{
		m.taskStates[Taskid] = 2

	}else if args.TaskType == 1{
		m.reduceStates[Taskid] = 2
	}
	return nil
}


func (m *Master) monitorTask(taskType int, taskNumber int){
	time.Sleep(10*time.Second)

	m.mu.Lock()
	defer m.mu.Unlock()

	if taskType==0{
		if m.taskStates[taskNumber] ==1{
			m.taskStates[taskNumber]=0
		}

	}else if taskType==1{
		if m.reduceStates[taskNumber]==1{
			m.reduceStates[taskNumber] =0
		}
	
	}
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}



//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := false

	
	for i:=0;i<len(m.reduceStates);i++{
		if m.reduceStates[i]!=2 {
			ret = false
			return ret
		}
	}
	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.files = files
	m.taskStates = make([]int, len(files))
	m.reduceStates = make([]int, nReduce)
	


	m.server()
	return &m
}
