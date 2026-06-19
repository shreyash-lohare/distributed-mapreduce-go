package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {



	// Your worker implementation here.


	


	for {
		reply, ok := CallMaster()
		if !ok {
			break
		}
		if reply.TaskType == 0 {
			filename := reply.FileName
			file , err := os.Open(filename)
				if err != nil{
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
			kva := mapf(filename, string(content))

			files := make([]*os.File, reply.Nreduce)
			encoders := make([]*json.Encoder, reply.Nreduce)
			fileNames := make([]string, reply.Nreduce)

			for i:=0; i<reply.Nreduce; i++{
				outFileName := fmt.Sprintf("mr-%v-%v", reply.TaskNumber, i)
				fileNames[i]= outFileName
				
				file, err := ioutil.TempFile(".", "mr-tmp-")
				if err != nil {
					log.Fatalf("cannot create temporary file")
				}
				files[i]= file
				encoders[i] = json.NewEncoder(file)
			}
			for _, kv:= range kva{
				bucket := ihash(kv.Key) % reply.Nreduce

				encoders[bucket].Encode(&kv)
			}

			for i := 0; i < reply.Nreduce; i++ {
				tempName := files[i].Name()

				if err := files[i].Close(); err != nil {
					log.Fatalf("cannot close temporary file")
				}

				if err := os.Rename(tempName, fileNames[i]); err != nil {
					log.Fatalf("cannot rename %v to %v", tempName, fileNames[i])
				}
			}

			master_args := TaskDoneArgs{}
			master_reply := TaskDoneReply{}
			master_args.TaskId = reply.TaskNumber
			master_args.TaskType = reply.TaskType
			call("Master.TaskDone", &master_args, &master_reply)

			
		} else if reply.TaskType ==1 {
			var kva []KeyValue
			for i:=0;i<reply.NMap;i++{
				fileName := fmt.Sprintf("mr-%v-%v", i, reply.TaskNumber)
				file , err := os.Open(fileName)
				if err != nil{
					log.Fatalf("cannot open %v", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err!= nil{
						break
					}
					kva = append(kva,kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(kva))
			
			ReducedFileName := fmt.Sprintf("mr-out-%d", reply.TaskNumber)
			ofile, err := ioutil.TempFile(".", "mr-out-tmp-")
			if err != nil{
				log.Fatalf("cannot create temporary reduce file")
			}

			i:=0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			tempName := ofile.Name()

			if err := ofile.Close(); err != nil {
				log.Fatalf("cannot close temporary reduce file")
			}

			if err := os.Rename(tempName, ReducedFileName); err != nil {
				log.Fatalf("cannot rename %v to %v", tempName, ReducedFileName)
			}
			Doneargs := TaskDoneArgs{}
			Donereply := TaskDoneReply{}
			Doneargs.TaskId = reply.TaskNumber
			Doneargs.TaskType = 1
			call("Master.TaskDone", &Doneargs, &Donereply)
		} else if reply.TaskType==2{
			time.Sleep(time.Second)
		}
		
	}

	
	
	

	// uncomment to send the Example RPC to the master.
	// CallExample()
	

}

func CallMaster() (WorkerReply, bool){
	args := WorkerArgs{}
	args.WorkerId = 69
	reply := WorkerReply{}

	ok := call("Master.GetTask", &args ,&reply)

	
	return reply, ok
	
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
