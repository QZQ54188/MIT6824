package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var uniqueJobID int //用于分配工作ID
var mutex sync.Mutex

// 协调器进行管理需要的字段
type Coordinator struct {
	JobChannelMap        chan *Job
	JobChannelReduce     chan *Job
	ReducerNum           int
	MapNum               int
	jobMetaHolder        JobMetaHolder
	CoordinatorCondition Condition
}

// 其中reducerNum和Cooedinator结构体中一样，但是worker在requestJob这一rpc
// 调用时只获取到Job结构体，所以要在job中加入reducerNum
type Job struct {
	JobType    JobType
	InputFile  []string
	JobId      int
	ReducerNum int
}

type JobMetaHolder struct {
	// jobID到元数据的映射
	MetaMap map[int]*JobMetaInfo
}

func (j *JobMetaHolder) putJob(jobInfo *JobMetaInfo) bool {
	jobId := jobInfo.JobPtr.JobId
	meta := j.MetaMap[jobId]
	if meta != nil {
		// fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = jobInfo
	}
	return true
}

func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo) {
	res, ok := j.MetaMap[jobId]
	return ok, res
}

func (j *JobMetaHolder) fireTheJob(jobId int) bool {
	ok, jobInfo := j.getJobMetaInfo(jobId)
	if !ok || jobInfo.condition != JobWaiting {
		return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true
}

func (j *JobMetaHolder) checkJobDone(phase Condition) bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0

	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum += 1
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum += 1
			} else {
				reduceUndoneNum += 1
			}
		}
	}

	// fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
	// 	mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	if phase == MapPhase {
		return mapDoneNum > 0 && mapUndoneNum == 0
	} else if phase == ReducePhase {
		return reduceDoneNum > 0 && reduceUndoneNum == 0
	}

	return false
}

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	mutex.Lock()
	defer mutex.Unlock()
	// fmt.Println("coordinator get a request from worker :")

	if c.CoordinatorCondition == MapPhase {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				// fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone(c.CoordinatorCondition) {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				// fmt.Printf("job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone(c.CoordinatorCondition) {
				c.nextPhase()
			}
			return nil
		}
	} else {
		reply.JobType = KillJob
	}
	return nil
}

func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	switch args.JobType {
	// 防止job被worker重复运行
	case MapJob:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			// fmt.Printf("Map task on %d complete\n", args.JobId)
		} else {
			// fmt.Println("[duplicated] job done", args.JobId)
		}
	case ReduceJob:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			// fmt.Printf("Reduce task on %d complete\n", args.JobId)
		} else {
			// fmt.Println("[duplicated] job done", args.JobId)
		}
	default:
		panic("wrong job done")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mutex.Lock()
	defer mutex.Unlock()
	// fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)), // 初始化Map任务通道
		JobChannelReduce: make(chan *Job, nReduce),    // 初始化Reduce任务通道
		ReducerNum:       nReduce,
		MapNum:           len(files),
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo), // 初始化map
		},
		CoordinatorCondition: MapPhase,
	}

	// 创建Map任务
	c.makeMapJobs(files)

	go c.CrashHandler()

	c.server()
	return &c
}

func (c *Coordinator) generateJobId() int {
	res := uniqueJobID
	uniqueJobID++
	return res
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		c.CoordinatorCondition = AllDone
	}
}

// 制作map任务，在程序一开始执行时就运行
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		fmt.Println("making map job :", id)
		// 初始化map阶段任务相关信息
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}

		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		// fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job
	}
	// fmt.Println("done making map jobs")
	c.jobMetaHolder.checkJobDone(c.CoordinatorCondition)
}

func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		fmt.Println("making reduce job :", id)
		jobToDo := Job{
			JobType:   ReduceJob,
			JobId:     id,
			InputFile: TmpFileAssignHelper(i, "mr-tmp"),
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &jobToDo,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		// fmt.Println("making reduce job :", &jobToDo)
		c.JobChannelReduce <- &jobToDo
	}
	// fmt.Println("done making reduce jobs")
	c.jobMetaHolder.checkJobDone(c.CoordinatorCondition)
}

func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-temp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mutex.Lock()
		if c.CoordinatorCondition == AllDone {
			mutex.Unlock()
			continue
		}
		for _, v := range c.jobMetaHolder.MetaMap {
			fmt.Println(v)
			if v.condition == JobWorking {
				// fmt.Println("job", v.JobPtr.JobId, " working for ", time.Since(v.StartTime))
			}
			if v.condition == JobWorking && time.Since(v.StartTime) > 8*time.Second {
				// fmt.Println("detect a crash on job ", v.JobPtr.JobId)
				switch v.JobPtr.JobType {
				case MapJob:
					c.JobChannelMap <- v.JobPtr
					v.condition = JobWaiting
				case ReduceJob:
					c.JobChannelReduce <- v.JobPtr
					v.condition = JobWaiting
				}
			}
		}
		mutex.Unlock()
	}
}
