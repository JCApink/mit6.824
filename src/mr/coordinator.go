package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mtx *sync.Mutex

	workerNum     int
	workerMonitor []WorkerMonitor

	mapTasks    []MapTask
	reduceTasks []ReduceTask

	mapDone *sync.Cond
}

const (
	IDLE      = 0
	ALLOCATED = 1
	DONE      = 2
)

type MapTask struct {
	fileName         string
	status           int
	allocateWorkerId int
}

type ReduceTask struct {
	status           int
	allocateWorkerId int
}

type WorkerMonitor struct {
	workerId int
	ch       chan bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequireTask(args *RequireTaskArgs, reply *RequireTaskReply) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var workerId int
	if args.WorkerId == -1 {
		workerId = c.workerNum
		c.workerNum += 1
	} else {
		workerId = args.WorkerId
	}
	reply.WorkerId = workerId
	reply.MapTask = nil
	reply.ReduceTask = nil
	reply.WaitTask = false

	if args.LastDone {
		err := c.completeTask(workerId)
		if err != nil {
			panic(err)
		}
		//fmt.Printf("worker %d already completed his task\n", workerId)
	}

	newMonitor := make([]WorkerMonitor, 0)
	for i := range c.workerMonitor {
		if c.workerMonitor[i].workerId == workerId {
			c.workerMonitor[i].ch <- true
		} else {
			newMonitor = append(newMonitor, c.workerMonitor[i])
		}
	}
	c.workerMonitor = newMonitor

	for c.noMapTaskToAllocate() && !c.allMapTaskDone() {
		c.mapDone.Wait()
	}

	if c.allMapTaskDone() {
		task := c.allocateReduceTask(workerId)
		if task != nil {
			reply.ReduceTask = task
			//fmt.Printf("allocated reduce task %d to worker %d\n", task.ReduceTaskId, workerId)

			ch := make(chan bool)
			c.workerMonitor = append(c.workerMonitor, WorkerMonitor{
				workerId: workerId,
				ch:       ch,
			})
			c.monitorWorker(workerId, ch)
		} else {
			reply.WaitTask = true
		}

		return nil
	} else {
		task := c.allocateMapTask(workerId)
		reply.MapTask = task
		//fmt.Printf("allocated map task %d to worker %d\n", task.MapTaskId, workerId)

		ch := make(chan bool)
		c.workerMonitor = append(c.workerMonitor, WorkerMonitor{
			workerId: workerId,
			ch:       ch,
		})
		c.monitorWorker(workerId, ch)
		return nil
	}
}

func (c *Coordinator) noMapTaskToAllocate() bool {
	for i := range c.mapTasks {
		if c.mapTasks[i].status == IDLE {
			return false
		}
	}
	return true
}

func (c *Coordinator) allMapTaskDone() bool {
	for i := range c.mapTasks {
		if c.mapTasks[i].status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) allocateMapTask(workerId int) *MapTaskReply {
	for i := range c.mapTasks {
		if c.mapTasks[i].status == IDLE {
			c.mapTasks[i].status = ALLOCATED
			c.mapTasks[i].allocateWorkerId = workerId
			return &MapTaskReply{
				FilePath:  c.mapTasks[i].fileName,
				MapTaskId: i,
				NumReduce: len(c.reduceTasks),
			}
		}
	}
	return nil
}

func (c *Coordinator) allocateReduceTask(workerId int) *ReduceTaskReply {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].status == IDLE {
			c.reduceTasks[i].status = ALLOCATED
			c.reduceTasks[i].allocateWorkerId = workerId
			return &ReduceTaskReply{
				ReduceTaskId: i,
				NumMap:       len(c.mapTasks),
			}
		}
	}
	return nil
}

func (c *Coordinator) completeTask(workerId int) error {
	for i := range c.mapTasks {
		if c.mapTasks[i].status == ALLOCATED && c.mapTasks[i].allocateWorkerId == workerId {
			c.mapTasks[i].status = DONE

			mapTasksDone := true
			for _, task := range c.mapTasks {
				if task.status != DONE {
					mapTasksDone = false
				}
			}
			if mapTasksDone {
				c.mapDone.Broadcast()
			}
			return nil
		}
	}

	for i := range c.reduceTasks {
		if c.reduceTasks[i].status == ALLOCATED && c.reduceTasks[i].allocateWorkerId == workerId {
			c.reduceTasks[i].status = DONE
			return nil
		}
	}

	return fmt.Errorf("no allocated task for taskid %d", workerId)
}

func (c *Coordinator) monitorWorker(workerId int, ch chan bool) {
	go func() {
		time.Sleep(10 * time.Second)
		ch <- false
		close(ch)
	}()

	go func() {
		data := <-ch
		if !data {
			//fmt.Printf("oops worker %d crashed\n", workerId)
			c.mtx.Lock()
			defer c.mtx.Unlock()

			for i := range c.mapTasks {
				if c.mapTasks[i].status == ALLOCATED && c.mapTasks[i].allocateWorkerId == workerId {
					c.mapTasks[i].status = IDLE
				}
			}
			for i := range c.reduceTasks {
				if c.reduceTasks[i].status == ALLOCATED && c.reduceTasks[i].allocateWorkerId == workerId {
					c.reduceTasks[i].status = IDLE
				}
			}

			for i := range c.workerMonitor {
				if c.workerMonitor[i].ch == ch {
					c.workerMonitor = append(c.workerMonitor[:i], c.workerMonitor[i+1:]...)
					break
				}
			}
			c.mapDone.Broadcast() // IMPORTANT
		}
	}()
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
	ret := false

	// Your code here.
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if !c.allMapTaskDone() {
		ret = false
	} else {
		ret = true
		for i := range c.reduceTasks {
			if c.reduceTasks[i].status != DONE {
				ret = false
				break
			}
		}
	}

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
	if nReduce <= 0 {
		panic("wrong nReduce")
	}

	c.mtx = &sync.Mutex{}
	c.mapDone = sync.NewCond(c.mtx)

	c.workerNum = 0
	c.workerMonitor = make([]WorkerMonitor, 0)

	c.mapTasks = make([]MapTask, len(files))
	for i := range files {
		c.mapTasks[i] = MapTask{
			fileName: files[i],
			status:   IDLE,
		}
	}

	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := range c.reduceTasks {
		c.reduceTasks[i] = ReduceTask{
			status: IDLE,
		}
	}

	c.server()
	return &c
}
