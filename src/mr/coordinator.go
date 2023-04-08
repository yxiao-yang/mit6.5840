package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义为全局，worker之间访问coordinator时加锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	taskMetaHolder    TaskMetaHolder // 存着task
	files             []string       // 传入的文件数组
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	StartTime time.Time // 任务的开始时间，为crash做准备
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 保存全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // 任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTasks(files)
	c.server()
	go c.CrashDetector()
	return &c
}

// CrashDetector crash检测
func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Working {
				//fmt.Println("task[", v.TaskAdr.TaskId, "] is working: ", time.Since(v.StartTime), "s")
			}

			if v.state == Working && time.Since(v.StartTime) > 9*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Waiting

				}
			}
		}
		mu.Unlock()
	}

}

// 对map任务进行处理,初始化map任务
func (c *Coordinator) makeMapTasks(files []string) {
	// 文件->任务->map队列
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		// 新生成的任务加入到map队列
		c.MapTaskChannel <- &task
	}
}

// 初始化reduce队列
func (c *Coordinator) makeReduceTasks() {
	// map任务->reduce队列
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		// 新生成的任务加入到map队列
		c.ReduceTaskChannel <- &task
	}
}

func selectReduceName(reduceNum int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		//fmt.Println("meta contains task which id = ", taskId)
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}

// 分发任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 分发任务应该上锁，防止多个worker竞争，并用defer回退解锁
	mu.Lock()
	defer mu.Unlock()

	// 判断任务类型存任务
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				//fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}

	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				//fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}

	case AllDone:
		{

			reply.TaskType = ExitTask
		}
	default:
		panic("The phase undefined ! ! !")

	}

	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {

	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成,下同
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	//fmt.Printf("map tasks  are finished %d/%d, reduce task are finished %d/%d \n",
	//	mapDoneNum, mapDoneNum+mapUnDoneNum, reduceDoneNum, reduceDoneNum+reduceUnDoneNum)

	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true

	// Map
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}

	return false

}

// 判断给定任务是否在工作，并修正其目前任务信息状态,如果任务不在工作的话返回true
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int {

	res := c.TaskId
	c.TaskId++
	return res
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	newReply := ExampleReply{
		1000,
	}
	reply = &newReply
	fmt.Println("example")
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

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break
	case ReduceTask:
		meta, ok := c.taskMetaHolder.MetaMap[args.TaskId]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break

	default:
		panic("The task type undefined ! ! !")
	}
	return nil

}

// Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}

}
