package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"encoding/csv"
)

// constants

const RAINSTORM_RPC_PORT = 9200
const RAINSTORM_DATA_PORT_START = 9201
const STAT_REPORT_INTERVAL = 1 * time.Second
const ACK_TIMEOUT = 2 * time.Second
const MAX_RETRIES = 5
const END_DETECT_TIMEOUT = 10 * time.Second

// structs

type Tuple struct {
	ID         string // SourceID:SeqNum
	Key        string
	Value      string
	SenderID   string // To know who to Ack
	SenderAddr string // Host:RPCPort for ACK
}

type RainStormConfig struct {
	NumStages        int
	NumTasksPerStage int
	Operators        []OperatorInfo
	HydfsSrc         string
	HydfsDest        string
	ExactlyOnce      bool
	Autoscale        bool
	InputRate        int
	LowWatermark     int
	HighWatermark    int
	JobID            string
}

type OperatorInfo struct {
	Exe  string
	Args []string
}

type TaskMetadata struct {
	TaskID     string
	StageIndex int
	TaskIndex  int // fixed index for WAL
	Op         OperatorInfo
	WorkerID   string
	Pid        int
	Port       int
	State      string // "RUNNING", "KILLED"
	WALFile    string
	StateFile  string
	LogFile    string
}

type StartTaskReply struct {
	Pid int
	Ok  bool
}

type RoutingTable map[int][]string // StageIndex -> List of "Host:Port"

type KillTaskArgs struct {
	TaskID string
}

// RPC Arguments

type StartTaskArgs struct {
	TaskID     string
	StageIndex int
	Op         OperatorInfo
	Config     RainStormConfig
	Routing    RoutingTable
	TotalTasks int
	TaskIndex  int
	LeaderAddr string
}

type UpdateRoutingArgs struct {
	Routing RoutingTable
}

type AckArgs struct {
	TupleID string
	TaskID  string // ID of the task sending the ack (receiver)
}

type StatsReportArgs struct {
	TaskID     string
	StageIndex int
	Rate       float64
}

type Checkpoint struct {
	State     map[string]int
	Processed map[string]bool
}

// Server Structs

type RainStormServer struct {
	mu             sync.RWMutex
	me             string
	isLeader       bool
	leaderAddr     string
	membershipList *MembershipList
	hydfs          *HyDFS
	logger         *log.Logger
	monitorRunning bool

	// Leader State
	jobConfig   *RainStormConfig
	tasks       map[string]*TaskMetadata
	stageTasks  map[int][]string // StageIdx -> []TaskID
	routing     RoutingTable
	resourceMgr *ResourceManager
	lastRates   time.Time // for end detection

	// Worker State
	rpcListener net.Listener
	activeTasks map[string]*RainStormTask
	taskLoggers map[string]*log.Logger
}

type RainStormTask struct {
	meta        TaskMetadata
	config      RainStormConfig
	routing     RoutingTable
	mu          sync.Mutex
	rs          *RainStormServer

	// Channels
	incoming    chan Tuple
	stopChan    chan struct{}
	ackChan     chan string // Receive Acks from downstream

	// State
	processed   map[string]bool // Dedup: TupleID -> bool
	pendingAcks map[string]Tuple // Retry Buffer: TupleID -> Tuple
	state       map[string]int  // Aggregation State: Key -> Count

	logger      *log.Logger

	// Stats
	count       int
	lastStat    time.Time
}

// Initialization

func NewRainStormServer(mem *MembershipList, h *HyDFS, logger *log.Logger) *RainStormServer {
    rs := &RainStormServer{
        me:             mem.selfId,
        membershipList: mem,
        hydfs:          h,
        logger:         logger,
        tasks:          make(map[string]*TaskMetadata),
        stageTasks:     make(map[int][]string),
        routing:        make(RoutingTable),
        resourceMgr:    NewResourceManager(),
        activeTasks:    make(map[string]*RainStormTask),
        taskLoggers:    make(map[string]*log.Logger),
        lastRates:      time.Now(),
    }

    rs.StartRPCServer()

    go rs.resourceMgr.Run(rs)
    go rs.MonitorFailures()

    return rs
}

func (rs *RainStormServer) StartRPCServer() {
	rpc.Register(rs)
	laddr, _ := net.ResolveTCPAddr("tcp", ":"+strconv.Itoa(RAINSTORM_RPC_PORT))
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		rs.logger.Fatal("[FATAL] RainStorm RPC listen error:", err)
	}

	rs.rpcListener = l

	go func() {
		for {
			conn, err := rs.rpcListener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}

// Leader Logic

func (rs *RainStormServer) StartJob(config RainStormConfig) {
	rs.mu.Lock()

	oldTasks := make([]*TaskMetadata, 0)
	for _, t := range rs.tasks {
		oldTasks = append(oldTasks, t)
	}

	rs.tasks = make(map[string]*TaskMetadata)
	rs.stageTasks = make(map[int][]string)
	rs.routing = make(RoutingTable)

	rs.resourceMgr.mu.Lock()
	rs.resourceMgr.stats = make(map[string]float64)
	rs.resourceMgr.mu.Unlock()

	rs.isLeader = true

	jobID := strconv.FormatInt(time.Now().UnixNano(), 10)
	config.JobID = jobID

	newConfig := config
	rs.jobConfig = &newConfig

	rs.mu.Unlock()

	// Kill old tasks and wait for cleanup
	if len(oldTasks) > 0 {
		rs.logger.Printf("[LEADER] Killing %d old tasks from previous job", len(oldTasks))
		var wg sync.WaitGroup
		for _, meta := range oldTasks {
			wg.Add(1)
			go func(m *TaskMetadata) {
				defer wg.Done()
				client, err := rpc.Dial("tcp", StripPortFromAddr(m.WorkerID, true)+":"+strconv.Itoa(RAINSTORM_RPC_PORT))
				if err == nil {
					var reply bool
					client.Call("RainStormServer.KillTask", KillTaskArgs{TaskID: m.TaskID}, &reply)
					client.Close()
				}
			}(meta)
		}
		wg.Wait()

		rs.logger.Println("[LEADER] Waiting for resource cleanup...")
		time.Sleep(5 * time.Second)
	}

	rs.logger.Printf("[LEADER] Starting Job %s...", jobID)

	rs.logger.Printf("[LEADER] Creating output file %s in HyDFS", config.HydfsDest)
	emptyData := []byte("")

	primary := rs.membershipList.GetPrimary(config.HydfsDest)
	rs.logger.Printf("[LEADER] Primary for %s is %s", config.HydfsDest, primary)

	var createErr error
	if primary == rs.me {
		rs.logger.Printf("[LEADER] Creating output file locally (I am primary)")
		// Try to create locally, but if it exists from previous job, overwrite it
		createErr = rs.hydfs.LocalCreate(config.HydfsDest, emptyData)
		if createErr != nil && strings.Contains(createErr.Error(), "already exists") {
			rs.logger.Printf("[LEADER] Output file exists, overwriting with empty content")
			createErr = rs.hydfs.LocalOverwrite(config.HydfsDest, emptyData)
		}
	} else {
		rs.logger.Printf("[LEADER] Creating output file remotely on primary %s", primary)
		// Try remote create, if fails try overwrite
		createErr = rs.hydfs.RemoteCreate(primary, config.HydfsDest, emptyData)
		if createErr != nil && strings.Contains(createErr.Error(), "already exists") {
			rs.logger.Printf("[LEADER] Output file exists, overwriting with empty content")
			createErr = rs.hydfs.RemoteOverwrite(primary, config.HydfsDest, emptyData)
		}
	}

	if createErr != nil {
		rs.logger.Printf("[ERROR] Failed to create output file %s: %v", config.HydfsDest, createErr)
		fmt.Printf("[ERROR] Failed to create output file %s: %v\n", config.HydfsDest, createErr)
		return
	}

	rs.logger.Printf("[LEADER] Output file %s created successfully", config.HydfsDest)
	fmt.Printf("[LEADER] Output file %s created and ready\n", config.HydfsDest)

	// Verify the file exists
	time.Sleep(500 * time.Millisecond)
	replicas := rs.membershipList.GetReplicas(config.HydfsDest, 3)
	rs.logger.Printf("[LEADER] Output file replicas: %v", replicas)

	rs.scheduleStage(0, 1, OperatorInfo{Exe: "source", Args: []string{config.HydfsSrc}}, jobID)

	for i := 0; i < config.NumStages; i++ {
		rs.scheduleStage(i+1, config.NumTasksPerStage, config.Operators[i], jobID)
	}

	rs.broadcastRouting()
}

func (rs *RainStormServer) scheduleStage(stageIdx int, numTasks int, op OperatorInfo, jobID string) {
	aliveNodes := rs.membershipList.GetSortedAlive()
	if len(aliveNodes) == 0 {
		rs.logger.Println("[ERROR] No alive nodes to schedule tasks")
		return
	}

	rs.mu.Lock()
	startIndex := len(rs.stageTasks[stageIdx])
	rs.mu.Unlock()

	for i := 0; i < numTasks; i++ {
		globalTaskIndex := startIndex + i

		taskID := fmt.Sprintf("stage%d_task%d_%s_%d", stageIdx, globalTaskIndex, jobID, time.Now().UnixNano())

		// Round-robin scheduling: (startIndex + i) % num_nodes
		workerNode := aliveNodes[(globalTaskIndex)%len(aliveNodes)].id
		port := RAINSTORM_DATA_PORT_START + (stageIdx * 100) + globalTaskIndex

		wal := fmt.Sprintf("wal_%s_stage%d_task%d", jobID, stageIdx, globalTaskIndex)
		logFile := fmt.Sprintf("task_%s_stage%d_task%d.log", jobID, stageIdx, globalTaskIndex)

		stateF := ""
		if op.Exe == "count" {
			stateF = fmt.Sprintf("state_%s_stage%d_task%d", jobID, stageIdx, globalTaskIndex)
		}

		meta := &TaskMetadata{
			TaskID:     taskID,
			StageIndex: stageIdx,
			TaskIndex:  globalTaskIndex,
			Op:         op,
			WorkerID:   workerNode,
			Port:       port,
			State:      "RUNNING",
			WALFile:    wal,
			StateFile:  stateF,
			LogFile:    logFile,
		}

		rs.mu.Lock()
		rs.tasks[taskID] = meta
		rs.stageTasks[stageIdx] = append(rs.stageTasks[stageIdx], taskID)

		addr := StripPortFromAddr(workerNode, true) + ":" + strconv.Itoa(port)
		rs.routing[stageIdx] = append(rs.routing[stageIdx], addr)
		rs.mu.Unlock()

		go rs.sendStartTask(workerNode, meta, globalTaskIndex, numTasks)
	}
}

func (rs *RainStormServer) sendStartTask(workerID string, meta *TaskMetadata, taskIdx, totalTasks int) {
	client, err := rpc.Dial("tcp", StripPortFromAddr(workerID, true)+":"+strconv.Itoa(RAINSTORM_RPC_PORT))
	if err != nil {
		rs.logger.Printf("[WARN] Failed to dial worker %s for start task: %v", workerID, err)
		return
	}
	defer client.Close()

	host := StripPortFromAddr(rs.me, true)
	leaderAddr := host + ":" + strconv.Itoa(RAINSTORM_RPC_PORT)

	rs.mu.RLock()
	if rs.jobConfig == nil {
		rs.mu.RUnlock()
		return
	}

	routingCopy := make(RoutingTable)
	for k, v := range rs.routing {
		routingCopy[k] = make([]string, len(v))
		copy(routingCopy[k], v)
	}

	args := StartTaskArgs{
		TaskID:     meta.TaskID,
		StageIndex: meta.StageIndex,
		Op:         meta.Op,
		Config:     *rs.jobConfig,
		Routing:    routingCopy,
		TotalTasks: totalTasks,
		TaskIndex:  taskIdx,
		LeaderAddr: leaderAddr,
	}
	rs.mu.RUnlock()

	var reply StartTaskReply
	err = client.Call("RainStormServer.StartTask", args, &reply)
	if err == nil && reply.Ok {
		rs.mu.Lock()
		task, exists := rs.tasks[meta.TaskID]
		if exists && task != nil {
			task.Pid = reply.Pid
			rs.logger.Printf("[LEADER] Task Started: ID=%s Node=%s PID=%d LogFile=%s", meta.TaskID, workerID, reply.Pid, meta.LogFile)
		} else {
			rs.logger.Printf("[WARN] Task %s confirmed started but not found in active list (Job restarted/Rescheduled?)", meta.TaskID)
		}
		rs.mu.Unlock()
	} else {
		rs.logger.Printf("[WARN] RPC StartTask failed for %s: %v", meta.TaskID, err)
	}
}

func (rs *RainStormServer) broadcastRouting() {
	rs.mu.RLock()
	routingCopy := make(RoutingTable)
	for k, v := range rs.routing {
		routingCopy[k] = v
	}
	rs.mu.RUnlock()

	peers := rs.membershipList.GetAllValidPeers()
	peers = append(peers, &Member{Id: rs.me})

	for _, p := range peers {
		go func(pid string) {
			client, err := rpc.Dial("tcp", StripPortFromAddr(pid, true)+":"+strconv.Itoa(RAINSTORM_RPC_PORT))
			if err == nil {
				defer client.Close()
				var reply bool
				client.Call("RainStormServer.UpdateRouting", UpdateRoutingArgs{Routing: routingCopy}, &reply)
			}
		}(p.Id)
	}
}

// Failure Monitoring

func (rs *RainStormServer) MonitorFailures() {
	for {
		time.Sleep(1 * time.Second)

		rs.mu.RLock()
		toCheck := make([]TaskMetadata, 0, len(rs.tasks))
		for _, meta := range rs.tasks {
			if meta.State == "RUNNING" {
				toCheck = append(toCheck, *meta) // Copy by value for safety during check
			}
		}

		// end detection logic (kept under lock)
		allZero := true
		rs.resourceMgr.mu.Lock()
		for _, rate := range rs.resourceMgr.stats {
			if rate > 0 {
				allZero = false
				break
			}
		}
		rs.resourceMgr.mu.Unlock()
		if allZero && time.Since(rs.lastRates) > END_DETECT_TIMEOUT {
			rs.logger.Println("[LEADER] End of run detected")
		} else if !allZero {
			rs.lastRates = time.Now()
		}
		rs.mu.RUnlock()

		for _, meta := range toCheck {
			// check membership
			member, exists := rs.membershipList.Get(meta.WorkerID)
			if !exists || member.Status == FAILED || member.Status == LEFT {
				rs.logger.Printf("[LEADER] Detected failure of worker %s running task %s", meta.WorkerID, meta.TaskID)
				go rs.RescheduleTask(meta.TaskID)
				continue
			}

			// check task connectivitiy (dial)
			if meta.StageIndex > 0 {
				addr := StripPortFromAddr(meta.WorkerID, true) + ":" + strconv.Itoa(meta.Port)
				conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond) // Increased timeout slightly
				if err != nil {
					rs.logger.Printf("[LEADER] Detected failure of task process %s on %s (Dial failed: %v)", meta.TaskID, addr, err)
					go rs.RescheduleTask(meta.TaskID)
				} else {
					conn.Close()
				}
			}
		}
	}
}

func (rs *RainStormServer) RescheduleTask(taskID string) {
	rs.mu.Lock()
	meta, ok := rs.tasks[taskID]
	if !ok {
		rs.mu.Unlock()
		return
	}

	if meta.State == "KILLED" {
		rs.mu.Unlock()
		return
	}

	meta.State = "KILLED"

	alive := rs.membershipList.GetSortedAlive()
	if len(alive) == 0 {
		rs.logger.Println("[FATAL] No alive nodes to reschedule task!")
		rs.mu.Unlock()
		return
	}

	newNode := alive[0].id
	if len(alive) > 1 && newNode == meta.WorkerID {
		newNode = alive[1].id
	}

	// Create new TaskID: BaseID_rTIMESTAMP
	baseID := strings.Split(taskID, "_r")[0]
	newTaskID := fmt.Sprintf("%s_r%d", baseID, time.Now().UnixNano())

	newMeta := &TaskMetadata{
		TaskID:     newTaskID,
		StageIndex: meta.StageIndex,
		TaskIndex:  meta.TaskIndex,
		Op:         meta.Op,
		WorkerID:   newNode,
		Port:       meta.Port,
		State:      "RUNNING",
		WALFile:    meta.WALFile,
		StateFile:  meta.StateFile,
		LogFile:    fmt.Sprintf("task_%s.log", newTaskID),
	}

	delete(rs.tasks, taskID)
	rs.tasks[newTaskID] = newMeta

	taskList := rs.stageTasks[meta.StageIndex]
	for i, tid := range taskList {
		if tid == taskID {
			taskList[i] = newTaskID
			break
		}
	}

	oldAddr := StripPortFromAddr(meta.WorkerID, true) + ":" + strconv.Itoa(meta.Port)
	newAddr := StripPortFromAddr(newNode, true) + ":" + strconv.Itoa(newMeta.Port)

	if list, ok := rs.routing[meta.StageIndex]; ok {
		for i, a := range list {
			if a == oldAddr {
				list[i] = newAddr
				break
			}
		}
	}

	rs.mu.Unlock()

	rs.logger.Printf("[LEADER] Restart of task after failure: old %s -> new %s on %s", taskID, newTaskID, newNode)

	rs.sendStartTask(newNode, newMeta, meta.TaskIndex, len(taskList))
	rs.broadcastRouting()
}

// Worker RPC Handlers

func (rs *RainStormServer) StartTask(args StartTaskArgs, reply *StartTaskReply) error {
    rs.logger.Printf("[WORKER] Starting Task %s (Stage %d)", args.TaskID, args.StageIndex)
    logName := fmt.Sprintf("task_stage%d_task%d_%d.log", args.StageIndex, args.TaskIndex, time.Now().UnixNano())
    f, err := os.OpenFile(logName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
    if err != nil {
        return err
    }
    taskLogger := log.New(f, "", log.Ltime|log.Lmicroseconds)

    rs.mu.Lock()
    rs.leaderAddr = args.LeaderAddr
    rs.mu.Unlock()

    task := &RainStormTask{
        meta:        TaskMetadata{TaskID: args.TaskID, StageIndex: args.StageIndex, Op: args.Op, TaskIndex: args.TaskIndex},
        config:      args.Config,
        routing:     args.Routing,
        incoming:    make(chan Tuple, 1000),
        stopChan:    make(chan struct{}),
        ackChan:     make(chan string, 1000),
        processed:   make(map[string]bool),
        pendingAcks: make(map[string]Tuple),
        state:       make(map[string]int),
        logger:      taskLogger,
        lastStat:    time.Now(),
        rs:          rs,
    }

    rs.mu.Lock()
    rs.activeTasks[args.TaskID] = task
    rs.taskLoggers[args.TaskID] = taskLogger
    rs.mu.Unlock()

    taskLogger.Printf("Task started: %s Stage: %d Op: %s", args.TaskID, args.StageIndex, args.Op.Exe)

    wal := fmt.Sprintf("wal_%s_stage%d_task%d", args.Config.JobID, args.StageIndex, args.TaskIndex)
    stateF := ""
    if args.Op.Exe == "count" {
        stateF = fmt.Sprintf("state_%s_stage%d_task%d", args.Config.JobID, args.StageIndex, args.TaskIndex)
    }
    task.meta.WALFile = wal
    task.meta.StateFile = stateF
    task.meta.Pid = 10000 + int(time.Now().UnixNano()%50000) + (args.StageIndex * 1000) + args.TaskIndex
    reply.Pid = task.meta.Pid
    reply.Ok = true

    if args.Config.ExactlyOnce {
        // Recover from State File (Checkpoint)
        if stateF != "" {
            localState := fmt.Sprintf("local_state_%s.gob", args.TaskID)
            err = rs.hydfs.HandleGet(stateF, localState)
            if err == nil {
                f, _ := os.Open(localState)
                dec := gob.NewDecoder(f)

                // decoding as checkpoint first
                var kp Checkpoint
                err := dec.Decode(&kp)
                if err == nil {
                     task.state = kp.State
                     // merge processed map from checkpoint to ensure consistency
                     for k, v := range kp.Processed {
                         task.processed[k] = v
                     }
                     taskLogger.Printf("Recovered Checkpoint: %d keys, %d processed entries", len(task.state), len(kp.Processed))
                } else {
                    // fallback to old map[string]int (legacy support)
                    f.Seek(0, 0)
                    dec2 := gob.NewDecoder(f)
                    err2 := dec2.Decode(&task.state)
                    if err2 == nil {
                        taskLogger.Printf("Recovered Legacy State: %d keys", len(task.state))
                    }
                }
                f.Close()
                os.Remove(localState)
            }
        }

        // recover from wal (merge into processed)
        localWal := fmt.Sprintf("local_wal_%s.txt", args.TaskID)
        err = rs.hydfs.HandleGet(wal, localWal)
        if err == nil {
            f, _ := os.Open(localWal)
            scanner := bufio.NewScanner(f)
            processedCount := 0
            for scanner.Scan() {
                line := scanner.Text()
                if strings.HasPrefix(line, "PROCESSED:") {
                    id := strings.TrimPrefix(line, "PROCESSED:")
                    task.processed[id] = true
                    processedCount++
                }
            }
            f.Close()
            os.Remove(localWal)
            taskLogger.Printf("Recovered %d processed tuples from WAL", processedCount)
        } else {
	        emptyData := []byte("")
            primary := rs.membershipList.GetPrimary(wal)
            if primary == rs.me {
                rs.hydfs.LocalCreate(wal, emptyData)
            } else {
                rs.hydfs.RemoteCreate(primary, wal, emptyData)
            }
            taskLogger.Printf("Created new WAL file: %s", wal)
        }
    }

    go rs.RunTask(task, args)
    return nil
}

func (rs *RainStormServer) UpdateRouting(args UpdateRoutingArgs, reply *bool) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	rs.routing = args.Routing
	for _, task := range rs.activeTasks {
		task.mu.Lock()
		task.routing = args.Routing
		task.mu.Unlock()
	}
	return nil
}

func (rs *RainStormServer) DeliverAck(args AckArgs, reply *bool) error {
	rs.mu.RLock()
	task, ok := rs.activeTasks[args.TaskID]
	rs.mu.RUnlock()

	if ok {
		select {
		case task.ackChan <- args.TupleID:
		default:
		}
	}

	*reply = true
	return nil
}

func (rs *RainStormServer) KillTask(args KillTaskArgs, reply *bool) error {
	rs.mu.Lock()
	task, ok := rs.activeTasks[args.TaskID]
	if !ok {
		rs.mu.Unlock()
		return fmt.Errorf("Task not found")
	}

	// Remove from active tasks immediately to prevent reuse
	delete(rs.activeTasks, args.TaskID)

	// Close and remove logger
	if taskLogger, exists := rs.taskLoggers[args.TaskID]; exists {
		// Flush and close the logger's file
		if file, ok := taskLogger.Writer().(*os.File); ok {
			file.Sync()
			file.Close()
		}
		delete(rs.taskLoggers, args.TaskID)
	}
	rs.mu.Unlock()

	// Signal task to stop (check if already closed to avoid panic)
	select {
	case <-task.stopChan:
		// Already closed
	default:
		close(task.stopChan)
	}

	task.mu.Lock()
	task.pendingAcks = make(map[string]Tuple)
	task.mu.Unlock()

	task.logger.Println("[TASK] Killed")

	// Give task goroutines time to exit and release resources
	time.Sleep(500 * time.Millisecond)

	*reply = true
	return nil
}

func (rs *RainStormServer) KillTaskByPID(pidStr string, reply *bool) error {
    rs.KillTaskLocal(pidStr)
    *reply = true
    return nil
}

func cleanArg(arg string) string { // trim every type of quote
    arg = strings.TrimSpace(arg)
    arg = strings.Trim(arg, "\"")
    arg = strings.Trim(arg, "'")
    arg = strings.Trim(arg, "“")
    arg = strings.Trim(arg, "”")
    return arg
}

func (t *RainStormTask) ExecuteOp(input Tuple, args []string) []Tuple {
    var output []Tuple
    opExe := t.meta.Op.Exe

    t.logger.Printf("[EXEC] Stage %d Op %s processing tuple %s", t.meta.StageIndex, opExe, input.ID)

    switch opExe {

    case "identity":
        output = append(output, input)

    case "filter":
        pattern := cleanArg(args[0])
        if strings.Contains(input.Value, pattern) {
            output = append(output, input)
            t.logger.Printf("[FILTER] Accepted: %s", input.Value)
        } else {
            t.logger.Printf("[FILTER] Rejected: %s", input.Value)
        }

    case "count":
        colIdx, _ := strconv.Atoi(cleanArg(args[0]))

        r := csv.NewReader(strings.NewReader(input.Value))
        r.LazyQuotes = true
        r.FieldsPerRecord = -1

		record, err := r.Read()
		if err != nil {
             record = strings.Split(input.Value, ",")
        }

		key := "EMPTY"
		if len(record) > colIdx {
            key = record[colIdx]
            key = strings.Trim(key, "\"")
            if key == "" { key = "EMPTY" }
        }

        t.mu.Lock()
        t.state[key]++
        val := strconv.Itoa(t.state[key])
		t.mu.Unlock()

        resultValue := fmt.Sprintf("%s, %s", key, val)
        output = append(output, Tuple{ID: input.ID, Key: key, Value: resultValue})

        t.logger.Printf("[COUNT] Key=%s Count=%s", key, val)

        if t.config.ExactlyOnce && t.meta.StateFile != "" {
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			t.mu.Lock()
			enc.Encode(t.state)
			t.mu.Unlock()

            // Use unique temp file
			tempFile := fmt.Sprintf("temp_state_%s.gob", t.meta.TaskID)
			err := os.WriteFile(tempFile, buf.Bytes(), 0644)
			if err == nil {
                // Use HandleOverwrite instead of HandleCreate
				err = t.rs.hydfs.HandleOverwrite(tempFile, t.meta.StateFile)
                if err != nil {
                    t.logger.Printf("[WARN] State overwrite failed: %v", err)
                }
				os.Remove(tempFile)
			} else {
				t.logger.Printf("[WARN] State write failed: %v", err)
			}
        }

    case "transform":
	    r := csv.NewReader(strings.NewReader(input.Value))
	    r.LazyQuotes = true
	    r.FieldsPerRecord = -1

	    record, err := r.Read()
	    if err != nil {
	        record = strings.Split(input.Value, ",")
	    }

        if len(record) >= 3 {
            val := fmt.Sprintf("%s,%s,%s", record[0], record[1], record[2])
            output = append(output, Tuple{ID: input.ID, Key: input.Key, Value: val})
            t.logger.Printf("[TRANSFORM] Output: %s", val)
        }
    }

    t.logger.Printf("[EXEC] Stage %d Op %s produced %d output tuples", t.meta.StageIndex, opExe, len(output))
    return output
}

func (t *RainStormTask) ListenForInput(prevOp OperatorInfo, taskIdx int) {
	port := RAINSTORM_DATA_PORT_START + (t.meta.StageIndex * 100) + taskIdx

	var l net.Listener
	var err error
	for attempt := 0; attempt < 10; attempt++ {
		l, err = net.Listen("tcp", ":"+strconv.Itoa(port))
		if err == nil {
			break
		}

		// Port might still be in TIME_WAIT from previous task
		if attempt < 9 {
			t.logger.Printf("[WARN] Port %d in use, retrying in 500ms (attempt %d/10)", port, attempt+1)
			time.Sleep(500 * time.Millisecond)
		}
	}

	if err != nil {
		t.logger.Printf("[ERROR] Listen failed after 10 attempts: %v", err)
		return
	}

	t.logger.Printf("[INFO] Successfully bound to port %d", port)

	go func() {
		<-t.stopChan
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-t.stopChan: return
			default: continue
			}
		}
		go func(c net.Conn) {
			dec := gob.NewDecoder(c)
			for {
				var tuple Tuple
				if err := dec.Decode(&tuple); err != nil { return }
				t.incoming <- tuple
			}
		}(conn)
	}
}

func (t *RainStormTask) sendDownstream(tuple Tuple, rs *RainStormServer) error {
	host, _ := os.Hostname()
	tuple.SenderAddr = host + ":" + strconv.Itoa(RAINSTORM_RPC_PORT)

	nextStage := t.meta.StageIndex + 1
	isFinalStage := (nextStage > t.config.NumStages)

	var targets []string
	if !isFinalStage {
		targets = t.routing[nextStage]
	}

	t.mu.Lock()
	if t.config.ExactlyOnce && !isFinalStage && len(targets) > 0 {
		t.pendingAcks[tuple.ID] = tuple
	}

	partitionKey := tuple.Key
	if !isFinalStage && nextStage <= t.config.NumStages {
		nextOp := t.config.Operators[nextStage-1]
		if nextOp.Exe == "count" && len(nextOp.Args) > 0 {
			colIdx, _ := strconv.Atoi(cleanArg(nextOp.Args[0]))
			r := csv.NewReader(strings.NewReader(tuple.Value))
			r.LazyQuotes = true
			r.FieldsPerRecord = -1
			record, err := r.Read()
			if err != nil {
				record = strings.Split(tuple.Value, ",")
			}
			if len(record) > colIdx {
				val := strings.TrimSpace(record[colIdx])
				val = strings.Trim(val, "\"")
				if val == "" {
					val = "EMPTY"
				}
				partitionKey = val
				t.logger.Printf("[PARTITION_KEY] Extracted key '%s' from column %d of: %s", partitionKey, colIdx, tuple.Value)
			} else {
				t.logger.Printf("[PARTITION_KEY] Column %d not found in: %s (only %d columns)", colIdx, tuple.Value, len(record))
			}
		}
	}
	t.mu.Unlock()

	// write to output file
	if isFinalStage || len(targets) == 0 {
		data := []byte(fmt.Sprintf("%s\n", tuple.Value))
		success := false
		var lastErr error

		maxAttempts := 10
		for i := 0; i < maxAttempts; i++ {
			rs.membershipList.mu.RLock()
			primary := rs.membershipList.GetPrimary(t.config.HydfsDest)
			rs.membershipList.mu.RUnlock()

			if primary == "" {
				lastErr = fmt.Errorf("no primary found")
				time.Sleep(100 * time.Millisecond)
				continue
			}

			var err error
			if primary == rs.me {
				err = rs.hydfs.LocalAppend(t.config.HydfsDest, data)
			} else {
				err = rs.hydfs.RemoteAppend(primary, t.config.HydfsDest, data)
			}

			if err == nil {
				success = true
				break
			}

			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}

		if !success {
			t.logger.Printf("[ERROR] Failed to write output for tuple %s: %v", tuple.ID, lastErr)
			return lastErr
		}

		fmt.Printf("[RESULT] %s\n", tuple.Value)
		return nil
	}

	// send to next stage
	hash := hashKey(partitionKey)
	targetIdx := int(hash) % len(targets)
	targetAddr := targets[targetIdx]

	t.logger.Printf("[PARTITION] Stage %d sending tuple with key '%s' (hash %d) to target %d/%d: %s",
	t.meta.StageIndex, partitionKey, hash, targetIdx, len(targets), targetAddr)

    // Synchronous send call
	return t.sendToAddr(targetAddr, tuple)
}

func (rs *RainStormServer) SendAckUpstream(senderAddr string, tupleID string, taskID string) {
    client, err := rpc.Dial("tcp", senderAddr)
    if err != nil {
        return
    }
    defer client.Close()
    args := AckArgs{TupleID: tupleID, TaskID: taskID}
    var reply bool
    client.Call("RainStormServer.DeliverAck", args, &reply)
}

// Source Logic

func (t *RainStormTask) RunSource(rs *RainStormServer, args StartTaskArgs) {
	port := RAINSTORM_DATA_PORT_START + (args.StageIndex * 100) + args.TaskIndex
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		t.logger.Printf("[ERROR] Source listen failed: %v", err)
		return
	}

	go func() {
		<-t.stopChan
		l.Close()
	}()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil { return }
			conn.Close()
		}
	}()

	localSrc := fmt.Sprintf("local_src_%s", t.meta.TaskID)
	err = rs.hydfs.HandleGet(args.Config.HydfsSrc, localSrc)
	if err != nil {
		t.logger.Printf("[ERROR] HandleGet failed for %s: %v", args.Config.HydfsSrc, err)
		return
	}

	file, err := os.Open(localSrc)
	if err != nil {
		t.logger.Printf("[ERROR] Source open failed: %v", err)
		return
	}
	defer file.Close()
	defer os.Remove(localSrc)

	scanner := bufio.NewScanner(file)
	lineNum := 0
	rate := args.Config.InputRate
	if rate <= 0 { rate = 1 }
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	filename := filepath.Base(args.Config.HydfsSrc)

	doneReading := make(chan struct{})

	// Goroutine to handle acks and retries - will stop when doneReading closes
	go func() {
		retryTicker := time.NewTicker(100 * time.Millisecond)
		defer retryTicker.Stop()
		for {
			select {
			case <-t.stopChan:
				return
			case <-doneReading:
				// Stop retrying once we're done reading
				return
			case ackID := <-t.ackChan:
				t.mu.Lock()
				delete(t.pendingAcks, ackID)
				t.mu.Unlock()
			case <-retryTicker.C:
				if t.config.ExactlyOnce {
					t.mu.Lock()
					var toRetry []Tuple
					for _, tuple := range t.pendingAcks {
						toRetry = append(toRetry, tuple)
					}
					t.mu.Unlock()

					for _, tuple := range toRetry {
						t.retryTuple(tuple, rs)
					}
				}
			}
		}
	}()

	for scanner.Scan() {
		<-ticker.C
		select {
		case <-t.stopChan:
			return
		default:
			line := scanner.Text()
			tuple := Tuple{
				ID:       fmt.Sprintf("%s:%d", filename, lineNum),
				Key:      filename + ":" + strconv.Itoa(lineNum),
				Value:    line,
				SenderID: t.meta.TaskID,
			}
			t.logger.Printf("[SOURCE] Output: %s", tuple.Value)
			t.sendDownstream(tuple, rs)
			lineNum++

			t.mu.Lock()
			t.count++
			if time.Since(t.lastStat) >= STAT_REPORT_INTERVAL {
				rate := float64(t.count) / time.Since(t.lastStat).Seconds()
				t.count = 0
				t.lastStat = time.Now()
				go rs.SendStats(t.meta.TaskID, t.meta.StageIndex, rate)
			}
			t.mu.Unlock()
		}
	}

	t.logger.Println("[SOURCE] Finished reading input file")
	close(doneReading)

	t.mu.Lock()
	t.pendingAcks = make(map[string]Tuple)
	t.mu.Unlock()

	t.logger.Println("[SOURCE] Cleared all pending acks, no more retries")

	// Notify leader that source is done
	go func() {
		client, err := rpc.Dial("tcp", rs.leaderAddr)
		if err != nil {
			t.logger.Printf("[ERROR] NotifySourceDone failed to dial leader %s: %v", rs.leaderAddr, err)
			return
		}
		defer client.Close()

		var reply bool
		err = client.Call("RainStormServer.NotifySourceDone", SourceDoneArgs{TaskID: t.meta.TaskID}, &reply)
		if err != nil {
			t.logger.Printf("[ERROR] NotifySourceDone RPC failed: %v", err)
		}
	}()
}

// --- Autoscaling ---

type ResourceManager struct {
	mu    sync.Mutex
	stats map[string]float64
	lastScaleTime time.Time
}

func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		stats: make(map[string]float64),
		lastScaleTime: time.Now(),
	}
}

func (rm *ResourceManager) Run(rs *RainStormServer) {
	for {
		time.Sleep(2 * time.Second)

		rm.mu.Lock()
		if time.Since(rm.lastScaleTime) < 5 * time.Second {
			rm.mu.Unlock()
			continue
		}
		rm.mu.Unlock()

		rs.mu.Lock()
		if rs.jobConfig == nil || !rs.jobConfig.Autoscale {
			rs.mu.Unlock()
			continue
		}

		stageRates := make(map[int]float64)
		stageCounts := make(map[int]int)

		rm.mu.Lock()
		for id, rate := range rm.stats {
			if meta, ok := rs.tasks[id]; ok {
				stageRates[meta.StageIndex] += rate
				stageCounts[meta.StageIndex]++
			}
		}
		rm.mu.Unlock()

		var upscaleStage int = -1
		var downscaleStage int = -1
		var downscaleVictimID string = ""
		// var downscaleVictimNode string = ""

		for stage, totalRate := range stageRates {
			if stage == 0 || rs.jobConfig.Operators[stage-1].Exe == "count" {
				continue
			}

			count := len(rs.stageTasks[stage])
			if count == 0 {
				continue
			}

			avg := totalRate / float64(count)

			if avg > float64(rs.jobConfig.HighWatermark) {
				upscaleStage = stage
				break
			} else if avg < float64(rs.jobConfig.LowWatermark) && count > 1 {
				rs.logger.Printf("[AUTOSCALE] Downscaling stage %d (Avg: %.2f)", stage, avg)
				downscaleStage = stage
				tasks := rs.stageTasks[stage]
				downscaleVictimID = tasks[len(tasks)-1]
				// downscaleVictimNode = rs.tasks[downscaleVictimID].WorkerID
				break
			}
		}

		rs.mu.Unlock()

		if upscaleStage != -1 { // execute upscale
			// Update Cooldown
			rm.mu.Lock()
			rm.lastScaleTime = time.Now()
			rm.mu.Unlock()

			op := rs.jobConfig.Operators[upscaleStage-1]

			fmt.Printf("[AUTOSCALE] Upscaling stage %d\n", upscaleStage)
			rs.logger.Printf("[AUTOSCALE] Upscaling stage %d", upscaleStage)

			rs.scheduleStage(upscaleStage, 1, op, rs.jobConfig.JobID)
			rs.broadcastRouting()
		}

		// Execute Downscale
		if downscaleStage != -1 && downscaleVictimID != "" {
			// Update Cooldown
			rm.mu.Lock()
			rm.lastScaleTime = time.Now()
			rm.mu.Unlock()

			fmt.Printf("[AUTOSCALE] Downscaling stage %d (Killing %s)\n", downscaleStage, downscaleVictimID)
			rs.logger.Printf("[AUTOSCALE] Downscaling stage %d", downscaleStage)

			var victimNodeAddr string

			rs.mu.Lock()
			if taskMeta, ok := rs.tasks[downscaleVictimID]; ok {
				victimNodeAddr = taskMeta.WorkerID // Save address for RPC later

				// remove from global map immediately
				delete(rs.tasks, downscaleVictimID)

				tasks := rs.stageTasks[downscaleStage]
				for i, tid := range tasks {
					if tid == downscaleVictimID {
						rs.stageTasks[downscaleStage] = append(tasks[:i], tasks[i+1:]...)
						break
					}
				}

				// remove from routing table
				if endpoints, ok := rs.routing[downscaleStage]; ok && len(endpoints) > 0 {
					rs.routing[downscaleStage] = endpoints[:len(endpoints)-1]
				}
			} else {
				// Task might have already died/vanished
				victimNodeAddr = ""
			}
			rs.mu.Unlock()

			// kill physically
			if victimNodeAddr != "" {
				client, err := rpc.Dial("tcp", StripPortFromAddr(victimNodeAddr, true)+":"+strconv.Itoa(RAINSTORM_RPC_PORT))
				if err == nil {
					var reply bool
					client.Call("RainStormServer.KillTask", KillTaskArgs{TaskID: downscaleVictimID}, &reply)
					client.Close()
				}
			}

			rs.broadcastRouting()
		}
    }
}

// Demo Commands

func (rs *RainStormServer) ListTasks() {
    rs.mu.RLock()
    defer rs.mu.RUnlock()
    fmt.Println("--- Active Tasks ---")
    if rs.isLeader {
        for _, t := range rs.tasks {
            vm := StripPortFromAddr(t.WorkerID, true)
            fmt.Printf("ID: %s | VM: %s | PID: %d | Op: %s | Log: %s\n", t.TaskID, vm, t.Pid, t.Op.Exe, t.LogFile)
        }
    } else {
        for id, task := range rs.activeTasks {
            vm := "local"
            fmt.Printf("ID: %s | VM: %s | PID: %d | Op: %s | Log: %s\n", id, vm, task.meta.Pid, task.meta.Op.Exe, task.meta.LogFile)
        }
    }
}

func (rs *RainStormServer) KillTaskLocal(pidStr string) {
	pidInt, _ := strconv.Atoi(pidStr)
	rs.mu.Lock()
	defer rs.mu.Unlock()

	killed := false
	for id, task := range rs.activeTasks {
		if task.meta.Pid == pidInt {
			close(task.stopChan)
			delete(rs.activeTasks, id)
			fmt.Printf("Killed task %s on PID %d\n", id, pidInt)
			killed = true
			break // Assuming PIDs are unique for our simulated tasks
		}
	}
	if !killed {
		fmt.Println("No task found for PID", pidStr)
	}
}

// Utils

func (rs *RainStormServer) SendStats(taskID string, stage int, rate float64) {
	if !rs.isLeader {
		client, err := rpc.Dial("tcp", rs.leaderAddr)
		if err != nil {
			rs.logger.Printf("[ERROR] SendStats failed to dial leader %s: %v", rs.leaderAddr, err)
			return
		}
		defer client.Close()

		args := StatsReportArgs{TaskID: taskID, StageIndex: stage, Rate: rate}
		var reply bool
		err = client.Call("RainStormServer.ReportStats", args, &reply)
		if err != nil {
			rs.logger.Printf("[ERROR] SendStats RPC failed: %v", err)
		}
	} else {
		rs.logger.Printf("[RATE] Task %s Stage %d: %.2f tuples/sec", taskID, stage, rate)
	}
}

func (rs *RainStormServer) ReportStats(args StatsReportArgs, reply *bool) error {
    if !rs.isLeader { return nil }

    rs.logger.Printf("[RATE] Task %s Stage %d: %.2f tuples/sec", args.TaskID, args.StageIndex, args.Rate)

    rs.resourceMgr.mu.Lock()
    rs.resourceMgr.stats[args.TaskID] = args.Rate
    rs.resourceMgr.mu.Unlock()
    *reply = true
    return nil
}

type SourceDoneArgs struct {
	TaskID string
}

func (rs *RainStormServer) NotifySourceDone(args SourceDoneArgs, reply *bool) error {
	if rs.isLeader {
		rs.logger.Printf("[LEADER] Source Task %s has finished reading input file.", args.TaskID)
		fmt.Printf("[LEADER] Source Task %s has finished reading input file.\n", args.TaskID) // Print to console too
	}
	*reply = true
	return nil
}

// Task Execution

func (rs *RainStormServer) RunTask(t *RainStormTask, args StartTaskArgs) {
    if args.StageIndex > 0 {
        t.logger.Printf("[TASK] Starting worker task for Stage %d, Op: %s", args.StageIndex, args.Op.Exe)
        go t.ListenForInput(args.Config.Operators[args.StageIndex-1], args.TaskIndex)
    } else {
        t.logger.Printf("[TASK] Starting source task")
        go t.RunSource(rs, args)
        return
    }

    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    retryCount := make(map[string]int)

    for {
        select {
        case <-t.stopChan:
            return

        case tuple := <-t.incoming:
            // dedup check
            if t.config.ExactlyOnce && t.processed[tuple.ID] {
                // already processed, ack again
                t.logger.Printf("[DUPLICATE] Rejected duplicate tuple: %s", tuple.ID)
                go rs.SendAckUpstream(tuple.SenderAddr, tuple.ID, t.meta.TaskID)
                continue
            }

            // state snapshot for rollback on failure
            stateSnapshot := make(map[string]int)
            if t.config.ExactlyOnce && t.meta.StateFile != "" {
                t.mu.Lock()
                for k, v := range t.state {
                    stateSnapshot[k] = v
                }
                t.mu.Unlock()
            }

            // execute op
            results := t.ExecuteOp(tuple, t.meta.Op.Args)

            //send downstream
            sendSuccess := true
            for _, res := range results {
                res.SenderID = t.meta.TaskID
                err := t.sendDownstream(res, rs)
                if err != nil {
                    t.logger.Printf("[ERROR] Send downstream failed: %v. Rolling back state.", err)
                    sendSuccess = false
                    break
                }
            }

            if !sendSuccess {
                // rollback state to prevent double counting
                if t.config.ExactlyOnce && t.meta.StateFile != "" {
                    t.mu.Lock()
                    t.state = stateSnapshot
                    t.mu.Unlock()
                }
                // do not ack upstream
                continue
            }

            // success
            if t.config.ExactlyOnce {
                // mark as processed
                t.processed[tuple.ID] = true

                // save checkpoint
                if t.meta.StateFile != "" {
                    var buf bytes.Buffer
                    enc := gob.NewEncoder(&buf)
                    kp := Checkpoint{
                        State: t.state,
                        Processed: t.processed,
                    }

                    t.mu.Lock()
                    enc.Encode(kp)
                    t.mu.Unlock()

                    tempFile := fmt.Sprintf("temp_state_%s.gob", t.meta.TaskID)
                    err := os.WriteFile(tempFile, buf.Bytes(), 0644)
                    if err == nil {
                        t.rs.hydfs.HandleOverwrite(tempFile, t.meta.StateFile)
                        os.Remove(tempFile)
                    } else {
                        t.logger.Printf("[WARN] Failed to write local state temp file: %v", err)
                    }
                }

                // log to wall
                walData := []byte("PROCESSED:" + tuple.ID + "\n")

                t.rs.membershipList.mu.RLock()
                primary := t.rs.membershipList.GetPrimary(t.meta.WALFile)
                t.rs.membershipList.mu.RUnlock()

                if primary == t.rs.me {
                    t.rs.hydfs.LocalAppend(t.meta.WALFile, walData)
                } else {
                    t.rs.hydfs.RemoteAppend(primary, t.meta.WALFile, walData)
                }

                // upstream ack
                go rs.SendAckUpstream(tuple.SenderAddr, tuple.ID, t.meta.TaskID)
            }

            // report stats
            t.mu.Lock()
            t.count++
            if time.Since(t.lastStat) >= STAT_REPORT_INTERVAL {
                rate := float64(t.count) / time.Since(t.lastStat).Seconds()
                t.count = 0
                t.lastStat = time.Now()
                go rs.SendStats(t.meta.TaskID, args.StageIndex, rate)
            }
            t.mu.Unlock()

        case ackID := <-t.ackChan:
            t.mu.Lock()
            delete(t.pendingAcks, ackID)
            delete(retryCount, ackID)
            t.mu.Unlock()

        case <-ticker.C:
            if t.config.ExactlyOnce {
                 t.mu.Lock()
                 var toRetry []Tuple
                 for id, tuple := range t.pendingAcks {
                    if retryCount[id] < 50 {
                        toRetry = append(toRetry, tuple)
                        retryCount[id]++
                    } else {
                         delete(t.pendingAcks, id)
                    }
                 }
                 t.mu.Unlock()

                 for _, tuple := range toRetry {
                     go t.retryTuple(tuple, rs)
                 }
            }
        }
    }
}

func (t *RainStormTask) sendToAddr(addr string, tuple Tuple) error {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		t.logger.Printf("[WARN] Dial failed %s: %v", addr, err)
		return err
	}
	defer conn.Close()
	return gob.NewEncoder(conn).Encode(tuple)
}

func (t *RainStormTask) retryTuple(tuple Tuple, rs *RainStormServer) {
	t.mu.Lock()
	nextStage := t.meta.StageIndex + 1
	targets := t.routing[nextStage]

	// use the same partition key extraction logic as sendDownstream
	partitionKey := tuple.Key
	if nextStage <= t.config.NumStages {
		nextOp := t.config.Operators[nextStage-1]
		if nextOp.Exe == "count" && len(nextOp.Args) > 0 {
			colIdx, _ := strconv.Atoi(cleanArg(nextOp.Args[0]))
			r := csv.NewReader(strings.NewReader(tuple.Value))
			r.LazyQuotes = true
			r.FieldsPerRecord = -1
			record, err := r.Read()
			if err != nil {
				record = strings.Split(tuple.Value, ",")
			}
			if len(record) > colIdx {
				val := strings.TrimSpace(record[colIdx])
				val = strings.Trim(val, "\"")
				if val == "" {
					val = "EMPTY"
				}
				partitionKey = val
			}
		}
	}
	t.mu.Unlock()

	if len(targets) > 0 {
		hash := hashKey(partitionKey)  // Use extracted partition key, not tuple.Key
		targetAddr := targets[int(hash) % len(targets)]
		t.sendToAddr(targetAddr, tuple)
	}
}
