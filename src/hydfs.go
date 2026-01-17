package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HYDFS_PORT = 9100
const STORAGE_DIR = "hydfs_storage"
const QUORUM_WRITE = 1 // expected # acks from secondaries for write operations
const MERGE_INTERVAL = 10 * time.Second

type HyDFS struct {
	membershipList *MembershipList // all nodes share a single state
	logger         *log.Logger
	storageDir     string
	fileLocks      sync.Map     // map[string]*sync.Mutex for per-file locks
	listener       net.Listener // TCP listener for requests
}

func NewHyDFS(membershipList *MembershipList, logger *log.Logger, storageDir string) *HyDFS {
	os.RemoveAll(storageDir) // cleanup old storage on startup for rejoin

	if err := os.MkdirAll(storageDir, 0755); err != nil {
		logger.Fatal("[FATAL] failed to create HyDFS storage dir: ", err)
	}

	h := &HyDFS{
		membershipList: membershipList,
		logger:         logger,
		storageDir:     storageDir,
		fileLocks:      sync.Map{},
		listener:       nil,
	}

	go h.ListenForRequests() // start TCP listener as goroutine
	go h.PeriodicMerge()     // background merge
	return h
}

func (h *HyDFS) PeriodicMerge() {
	for {
		time.Sleep(MERGE_INTERVAL)
		h.Rebalance()
	}
}

func (h *HyDFS) ListenForRequests() { // TCP listener for requests to ensure reliability and order
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(HYDFS_PORT))
	if err != nil {
		h.logger.Fatal("[FATAL]: TCP listener failed to initialize: ", err)
	}

	h.listener = listener
	defer h.listener.Close()

	for {
		conn, err := h.listener.Accept()
		if err != nil {
			h.logger.Printf("[WARN]: Connection with client refused: %v", err)
			continue
		}

		go h.HandleConnectionRequests(conn) // send request as goroutine
	}
}

// handler for incoming TCP requests (e.g., create) and internal requests (e.g., replicate_create) coming from other nodes
func (h *HyDFS) HandleConnectionRequests(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n') // read just the first line (command)
	if err != nil {
		h.logger.Printf("[WARN] Failed to read command: %v", err)
		return
	}

	parts := strings.Fields(strings.TrimSpace(line))
	if len(parts) < 1 {
		fmt.Fprintln(conn, "[FAIL] Invalid command")
		return
	}

	cmd := strings.ToLower(parts[0])
	remoteAddr := conn.RemoteAddr().String()
	remoteAddrPretty := StripPortFromAddr(remoteAddr, false)
	hydfsName := parts[1]

	h.logger.Printf("[INFO] Received command: %s for file: %s", cmd, hydfsName)

	switch cmd { // use a switch to handle different commands

	case "create", "append": // create new file or append to file
		var dataLen int
		if len(parts) > 2 {
			dataLen, _ = strconv.Atoi(parts[2])
		}

		data := make([]byte, dataLen) // empty byte slice to hold incoming file data
		_, err = io.ReadFull(reader, data)
		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to read data for %s: %v", cmd, err)
			fmt.Fprintln(conn, "[FAIL] read error")
			return
		}

		var err error
		if cmd == "create" {
			err = h.LocalCreate(hydfsName, data)
		} else { // == "append"
			err = h.LocalAppend(hydfsName, data)
		}
		// fmt.Printf("Completed %s for %s from %s\n> ", cmd, hydfsName, remoteAddrPretty)

		if err != nil {
			h.logger.Printf("[WARN] Responding with %s operation failed for %s: %v", strings.ToUpper(cmd), hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] "+err.Error())
		} else {
			h.logger.Printf("[INFO] Responding with %s operation succeeded for %s", strings.ToUpper(cmd), hydfsName)
			fmt.Fprintln(conn, "OK")
		}

	case "replicate_create": // handler to replicate a file on a secondary node
		if len(parts) < 3 {
			h.logger.Printf("[WARN] Responding with invalid replicate_create command for %s", hydfsName)
			fmt.Fprintln(conn, "[FAIL] invalid replicate_create")
			return
		}

		datalen, _ := strconv.Atoi(parts[2])

		data := make([]byte, datalen)
		_, err = io.ReadFull(reader, data) // read dataLen bytes
		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to read data for replicate_create %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] read error")
			return
		}

		// fmt.Printf("Received replicate_create for %s from %s\n", hydfsName, remoteAddrPretty)
		mutex := h.GetFileLock(hydfsName) // acquire lock for corresponding file
		mutex.Lock()
		filePath := h.GetFilePath(hydfsName)
		os.MkdirAll(filepath.Dir(filePath), 0755) // write the file to local storage
		err := os.WriteFile(filePath, data, 0644)
		mutex.Unlock()
		// fmt.Printf("Completed replicate_create for %s from %s\n", hydfsName, remoteAddrPretty)

		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to write data for replicate_create %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] "+err.Error())
		} else {
			h.logger.Printf("[INFO] Responding with successful replicate_create for %s", hydfsName)
			fmt.Fprintln(conn, "OK")
		}

	case "replicate_append": // handler to append to replicas on a secondary node
		dataLen, _ := strconv.Atoi(parts[2])

		data := make([]byte, dataLen)
		io.ReadFull(reader, data) // read dataLen bytes

		// fmt.Printf("Received replicate_append for %s from %s\n", hydfsName, remoteAddrPretty)
		mutex := h.GetFileLock(hydfsName) // acquire lock for corresponding file
		mutex.Lock()
		filePath := h.GetFilePath(hydfsName)
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			mutex.Unlock()
			h.logger.Printf("[WARN] Responding with failed to open file for replicate_append %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] "+err.Error())
			return
		}

		_, err = f.Write(data)
		f.Close()
		mutex.Unlock()
		// fmt.Printf("Completed replicate_append for %s from %s\n", hydfsName, remoteAddrPretty)

		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to write data for replicate_append %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] "+err.Error())
		} else {
			h.logger.Printf("[INFO] Responding with successful replicate_append for %s", hydfsName)
			fmt.Fprintln(conn, "OK")
		}

	case "remote_append":
		localName := parts[1]
		hydfsName := parts[2]
		err := h.HandleAppend(localName, hydfsName)
		if err != nil {
			fmt.Fprintln(conn, "FAIL "+err.Error())
		} else {
			fmt.Fprintln(conn, "OK")
		}

	case "compare": // handler to check if file is up to date
		// request the size of copy of file. if too short/doesn't exist, file is out of date

		mutex := h.GetFileLock(hydfsName)
		mutex.Lock()
		info, err := os.Stat(h.GetFilePath(hydfsName))
		mutex.Unlock()
		if err != nil {
			h.logger.Printf("[FAIL] Failed to stat file for compare request %s", hydfsName)
			fmt.Fprintln(conn, "0")
		} else {
			fmt.Fprintln(conn, strconv.FormatInt(info.Size(), 10))
		}

	case "overwrite":
		dataLen, _ := strconv.Atoi(parts[2])

		data := make([]byte, dataLen)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			h.logger.Printf("[FAIL] Failed to read for overwrite request %s", hydfsName)
			fmt.Fprintln(conn, "[FAIL] read error")
			return
		}

		fmt.Printf("Received OVERWRITE for %s from %s\n> ", hydfsName, remoteAddrPretty)
		mutex := h.GetFileLock(hydfsName)
		mutex.Lock()
		filePath := h.GetFilePath(hydfsName)
		os.MkdirAll(filepath.Dir(filePath), 0755)
		err := os.WriteFile(filePath, data, 0644)
		mutex.Unlock()
		fmt.Printf("Completed OVERWRITE for %s from %s\n> ", hydfsName, remoteAddrPretty)

		if err != nil {
			fmt.Fprintln(conn, "FAIL "+err.Error())
		} else {
			fmt.Fprintln(conn, "OK")
		}

	case "get_file": // handler to retrieve the contents of a file
		mutex := h.GetFileLock(hydfsName) // acquire lock for corresponding file
		filePath := h.GetFilePath(hydfsName)

		fmt.Printf("Received GET_FILE for %s from %s\n> ", hydfsName, remoteAddrPretty)
		mutex.Lock()
		data, err := os.ReadFile(filePath)
		mutex.Unlock()
		fmt.Printf("Completed GET_FILE for %s from %s\n> ", hydfsName, remoteAddrPretty)

		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to read file for get_file %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "[FAIL] "+err.Error())
		} else {
			h.logger.Printf("[INFO] Responding with successful get_file for %s", hydfsName)
			fmt.Fprintf(conn, "OK %d\n", len(data)) // on success, respond with "OK <length>"
			conn.Write(data)
		}

	case "merge": // handler to send a full, correct copy to overwrite incorrect/missing data
		err := h.Merge(hydfsName)
		if err != nil {
			h.logger.Printf("[WARN] Responding with failed to merge %s: %v", hydfsName, err)
			fmt.Fprintln(conn, "FAIL"+err.Error())
		} else {
			fmt.Fprintln(conn, "OK")
		}

	default:
		h.logger.Printf("[WARN] Responding with unknown command: %s", cmd)
		fmt.Fprintln(conn, "[FAIL] unknown command")
	}
}

func (h *HyDFS) GetFilePath(hydfsName string) string {
	return filepath.Join(h.storageDir, hydfsName)
}

func (h *HyDFS) GetFileLock(hydfsName string) *sync.Mutex {
	mutexI, _ := h.fileLocks.LoadOrStore(hydfsName, &sync.Mutex{}) // LoadOrStore will create/load a lock corresponding to hydfsName
	return mutexI.(*sync.Mutex)
}

func (h *HyDFS) HandleLS(hydfsName string) (string, error) { // handle cli ls
	key := hashKey(hydfsName)
	replicas := h.membershipList.GetReplicas(hydfsName, 3)

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("%s (%d)\n", hydfsName, key))

	for _, rep := range replicas {
		builder.WriteString(fmt.Sprintf("%s (%d)\n", rep, hashKey(rep)))
	}

	return builder.String(), nil
}

func (h *HyDFS) HandleListStore() (string, error) { // handle cli liststore
	var builder strings.Builder
	err := filepath.Walk(h.storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, _ := filepath.Rel(h.storageDir, path)
			builder.WriteString(relPath + "\n")
		}
		return nil
	})
	if err != nil {
		h.logger.Printf("[ERROR] Failed to list stored files: %v", err)
		return "", err
	}
	return builder.String(), nil
}

func (h *HyDFS) HandleCreate(localName, hydfsName string) error { // handle cli create
	data, err := os.ReadFile(localName)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to read local file for create %s: %v", localName, err)
		return err
	}

	primary := h.membershipList.GetPrimary(hydfsName)
	if primary == h.membershipList.selfId {
		h.logger.Printf("[INFO] Creating file %s locally as primary", hydfsName)
		err = h.LocalCreate(hydfsName, data)
	} else {
		h.logger.Printf("[INFO] Creating file %s remotely on primary %s", hydfsName, primary)
		err = h.RemoteCreate(primary, hydfsName, data)
	}

	if err == nil {
		replicas := h.membershipList.GetReplicas(hydfsName, 3)
		fmt.Printf("Completed create %s. Replicated to:\n", hydfsName)
		if len(replicas) > 0 {
			fmt.Printf("  p: %s\n", StripPortFromAddr(replicas[0], true))
		}
		if len(replicas) > 1 {
			fmt.Printf(" s1: %s\n", StripPortFromAddr(replicas[1], true))
		}
		if len(replicas) > 2 {
			fmt.Printf(" s2: %s\n", StripPortFromAddr(replicas[2], true))
		}
	}
	return err
}

func (h *HyDFS) LocalCreate(hydfsName string, data []byte) error { // "create" case logic
	mutex := h.GetFileLock(hydfsName)
	mutex.Lock()
	defer mutex.Unlock()

	filePath := h.GetFilePath(hydfsName)
	if _, err := os.Stat(filePath); err == nil {
		h.logger.Printf("[ERROR] File %s already exists locally", hydfsName)
		return errors.New("file already exists")
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		h.logger.Printf("[ERROR] Failed to create directories for %s: %v", hydfsName, err)
		return err
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil { // writes to primary node
		h.logger.Printf("[ERROR] Failed to write file %s: %v", hydfsName, err)
		return err
	}

	err := h.ReplicateAppend(hydfsName, data, true) // send the data to secondary nodes for replication
	if err != nil {
		h.logger.Printf("[ERROR] Replication for file %s failed: %v", hydfsName, err)
		os.Remove(filePath) // Rollback
		return err
	}
	return nil
}

func (h *HyDFS) RemoteCreate(primaryId string, hydfsName string, data []byte) error { // remote create to primary
	conn, err := h.DialNode(primaryId)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to dial primary %s for remote create of file %s: %v", primaryId, hydfsName, err)
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "create %s %d\n", hydfsName, len(data))
	conn.Write(data)

	response, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.HasPrefix(response, "OK") {
		h.logger.Printf("[INFO] Remote create for file %s succeeded on primary %s", hydfsName, primaryId)
		return nil
	}
	h.logger.Printf("[ERROR] Remote create for file %s failed on primary %s: %s", hydfsName, primaryId, strings.TrimSpace(response))
	return errors.New(strings.TrimSpace(response))
}

func (h *HyDFS) HandleAppend(localName, hydfsName string) error { // handle cli append
	data, err := os.ReadFile(localName)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to read local file for append %s: %v", localName, err)
		return err
	}

	primary := h.membershipList.GetPrimary(hydfsName)
	if primary == h.membershipList.selfId {
		h.logger.Printf("[INFO] Appending to file %s locally as primary", hydfsName)
		err = h.LocalAppend(hydfsName, data)
	} else {
		h.logger.Printf("[INFO] Appending to file %s remotely on primary %s", hydfsName, primary)
		err = h.RemoteAppend(primary, hydfsName, data)
	}

	if err == nil {
		replicas := h.membershipList.GetReplicas(hydfsName, 3)
		fmt.Printf("Completed append %s. Replicated to:\n", hydfsName)
		if len(replicas) > 0 {
			fmt.Printf("  p: %s\n", StripPortFromAddr(replicas[0], true))
		}
		if len(replicas) > 1 {
			fmt.Printf(" s1: %s\n", StripPortFromAddr(replicas[1], true))
		}
		if len(replicas) > 2 {
			fmt.Printf(" s2: %s\n", StripPortFromAddr(replicas[2], true))
		}
	}

	return err
}

func (h *HyDFS) LocalAppend(hydfsName string, data []byte) error {
	h.logger.Printf("[INFO] LocalAppend called for %s with %d bytes", hydfsName, len(data))

	mutex := h.GetFileLock(hydfsName)
	mutex.Lock()
	defer mutex.Unlock()

	filePath := h.GetFilePath(hydfsName)
	h.logger.Printf("[INFO] LocalAppend: Opening file %s", filePath)

	// Check if file exists first
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		h.logger.Printf("[ERROR] LocalAppend: File %s does not exist!", filePath)
		return fmt.Errorf("file does not exist: %s", hydfsName)
	}

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to open file %s for append: %v", hydfsName, err)
		return err
	}
	defer f.Close()

	n, err := f.Write(data)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to append data to file %s: %v", hydfsName, err)
		return err
	}

	h.logger.Printf("[INFO] LocalAppend: Successfully wrote %d bytes to %s", n, hydfsName)

	// Explicitly sync to disk
	if err := f.Sync(); err != nil {
		h.logger.Printf("[WARN] Failed to sync file %s: %v", hydfsName, err)
	}

	h.logger.Printf("[INFO] Successfully appended data to file %s locally, replicating...", hydfsName)
	return h.ReplicateAppend(hydfsName, data, false)
}

func (h *HyDFS) RemoteAppend(primaryId string, hydfsName string, data []byte) error {
	conn, err := h.DialNode(primaryId)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to dial primary %s for remote append of file %s: %v", primaryId, hydfsName, err)
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "append %s %d\n", hydfsName, len(data))
	conn.Write(data)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.HasPrefix(response, "OK") {
		h.logger.Printf("[INFO] Remote append for file %s succeeded on primary %s", hydfsName, primaryId)
		return nil
	}
	h.logger.Printf("[ERROR] Remote append for file %s failed on primary %s: %s", hydfsName, primaryId, strings.TrimSpace(response))
	return errors.New(strings.TrimSpace(response))
}

// replicate append (or create data) to secondaries, wait for quorum
func (h *HyDFS) ReplicateAppend(hydfsName string, data []byte, isCreate bool) error {
	replicas := h.membershipList.GetReplicas(hydfsName, 3) // 3 replicas: 1 primary, 2 secondary

	// retrieve secondaries by filtering selfId out
	var secondaries []string
	for _, rep := range replicas {
		if rep != h.membershipList.selfId {
			secondaries = append(secondaries, rep)
		}
	}

	type result struct {
		id  string
		err error
	}

	// loops through each secondary node and starts a new goroutine for each one
	results := make(chan result, len(secondaries))
	for _, sec := range secondaries {
		go func(sec string) {
			h.logger.Printf("[INFO] Replicating to secondary %s for file %s", sec, hydfsName)
			err := h.RemoteReplicateAppend(sec, hydfsName, data, isCreate)
			results <- result{id: sec, err: err}
		}(sec)
	}

	ackCount := 0
	for i := 0; i < len(secondaries); i++ {
		res := <-results
		if res.err == nil {
			ackCount++
		} else {
			h.logger.Printf("[ERROR] Replicate to %s failed: %v", res.id, res.err)
		}
	}

	if ackCount < QUORUM_WRITE {
		return errors.New("quorum not met")
	}

	return nil
}

// send replicate append to secondary
func (h *HyDFS) RemoteReplicateAppend(secId string, hydfsName string, data []byte, isCreate bool) error {
	conn, err := h.DialNode(secId)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to dial secondary %s for replicate append of file %s: %v", secId, hydfsName, err)
		return err
	}
	defer conn.Close()

	cmd := "replicate_append"
	if isCreate {
		cmd = "replicate_create"
	}

	fmt.Fprintf(conn, "%s %s %d\n", cmd, hydfsName, len(data))
	conn.Write(data)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.HasPrefix(response, "OK") {
		h.logger.Printf("[INFO] Replicate %s for file %s succeeded on secondary %s", cmd, hydfsName, secId)
		return nil
	}
	h.logger.Printf("[ERROR] Replicate %s for file %s failed on secondary %s: %s", cmd, hydfsName, secId, strings.TrimSpace(response))
	return errors.New(strings.TrimSpace(response))
}

func (h *HyDFS) HandleMultiAppend(args []string) error { // handle multiappend
	if len(args) < 3 || len(args)%2 != 1 {
		// return errors.New("invalid multiappend args: multiappend HyDFSfilename VM1 local1 VM2 local2 ...")
		return errors.New("invalid multiappend args: multiappend HyDFSfilename VM1 VM2 ... local1 local2 ...")
	}
	hydfsName := args[0]
	type result struct {
		vm  string
		err error
	}

	n := (len(args) - 1) / 2
	results := make(chan result, n)
	// for i := 1; i < len(args); i += 2 {
	// 	vm := args[i]
	// 	local := args[i+1]

	// 	go func(vm, local string) {
	// 		var err error
	// 		if vm == h.membershipList.selfId {
	// 			err = h.HandleAppend(local, hydfsName)
	// 		} else {
	// 			err = h.RemoteAppendFromVM(vm, local, hydfsName)
	// 		}

	// 		results <- result{vm: vm, err: err}
	// 	}(vm, local)
	// }
	for i := 1; i <= n; i++ {
		vm := args[i]
		local := args[i+n]

		go func(vm, local string) {
			var err error
			if vm == h.membershipList.selfId || vm == StripTSFromAddr(h.membershipList.selfId) {
				err = h.HandleAppend(local, hydfsName)
			} else {
				err = h.RemoteAppendFromVM(vm, local, hydfsName)
			}

			results <- result{vm: vm, err: err}
		}(vm, local)
	}

	errCount := 0
	for i := 0; i < (len(args)-1)/2; i++ {
		res := <-results
		if res.err != nil {
			h.logger.Printf("[WARN] Append from %s failed: %v", res.vm, res.err)
			errCount++
		}
	}

	if errCount > 0 {
		return errors.New("some appends failed")
	}

	fmt.Printf("Completed multiappend to %s\n> ", hydfsName)
	return nil
}

func (h *HyDFS) RemoteAppendFromVM(vmId string, localName, hydfsName string) error { // remote trigger append from another VM's local file
	conn, err := h.DialNode(vmId)
	if err != nil {
		return err
	}
	defer conn.Close()
	fmt.Fprintf(conn, "remote_append %s %s\n", localName, hydfsName)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.HasPrefix(response, "OK") {
		return nil
	}
	return errors.New(strings.TrimSpace(response))
}

func (h *HyDFS) HandleGet(hydfsName, localName string) error { // handle cli get
	primary := h.membershipList.GetPrimary(hydfsName)
	if primary == h.membershipList.selfId {
		return h.LocalGet(hydfsName, localName)
	}

	data, err := h.RemoteGet(primary, hydfsName)
	if err != nil {
		return err
	}

	return os.WriteFile(localName, data, 0644)
}

func (h *HyDFS) HandleGetFromReplica(nodeAddr string, hydfsName string, localName string) error { // handle cli get
	replicas := h.membershipList.GetReplicas(hydfsName, 3)
	replica := ""
	for _, rep := range replicas {
		if StripPortFromAddr(rep, true) == nodeAddr {
			replica = rep
			break
		}
	}

	if replica == "" {
		return errors.New("Specified node is not a replica for the file")
	}

	data, err := h.RemoteGet(replica, hydfsName)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to get file %s from replica %s: %v", hydfsName, replica, err)
		return err
	}

	return os.WriteFile(localName, data, 0644)
}

func (h *HyDFS) LocalGet(hydfsName, localName string) error { // local get
	data, err := os.ReadFile(h.GetFilePath(hydfsName))
	if err != nil {
		return err
	}

	return os.WriteFile(localName, data, 0644)
}

func (h *HyDFS) RemoteGet(primaryId string, hydfsName string) ([]byte, error) {
	conn, err := h.DialNode(primaryId)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "get_file %s\n", hydfsName)
	reader := bufio.NewReader(conn)
	response, _ := reader.ReadString('\n')
	if !strings.HasPrefix(response, "OK") {
		return nil, errors.New(strings.TrimSpace(response))
	}

	parts := strings.Fields(response)
	if len(parts) < 2 {
		return nil, errors.New("invalid response")
	}

	lenInt, _ := strconv.Atoi(parts[1])
	data := make([]byte, lenInt)
	_, err = io.ReadFull(reader, data)
	return data, err
}

func (h *HyDFS) HandleMerge(hydfsName string) error {
	primary := h.membershipList.GetPrimary(hydfsName)
	if primary == h.membershipList.selfId {
		err := h.Merge(hydfsName)
		if err == nil {
			fmt.Printf("Completed merge %s\n> ", hydfsName)
		}

		return err
	}

	conn, err := h.DialNode(primary)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "merge %s\n", hydfsName)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.HasPrefix(response, "OK") {
		fmt.Printf("Completed merge %s\n> ", hydfsName)
		return nil
	}

	return errors.New(strings.TrimSpace(response))
}

func (h *HyDFS) Merge(hydfsName string) error { // merge a file (push primary's version)
	replicas := h.membershipList.GetReplicas(hydfsName, 3)
	data, err := os.ReadFile(h.GetFilePath(hydfsName))
	if err != nil {
		return err
	}

	// sync as new primary if needed (select max size)
	h.SyncAsNewPrimary(hydfsName, replicas)

	// push to all replicas
	for _, rep := range replicas {
		if rep == h.membershipList.selfId {
			continue
		}

		remoteSize, err := h.RemoteCompare(rep, hydfsName)
		if err == nil && remoteSize == int64(len(data)) {
			continue
		}

		err = h.RemoteOverwrite(rep, hydfsName, data)
		if err != nil {
			h.logger.Printf("[WARN] Merge push to %s for %s failed: %v", rep, hydfsName, err)
		}
	}

	return nil
}

func (h *HyDFS) RemoteCompare(repId string, hydfsName string) (int64, error) { // remotely retrieve file size
	conn, err := h.DialNode(repId)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "compare %s\n", hydfsName)
	response, _ := bufio.NewReader(conn).ReadString('\n')
	size, err := strconv.ParseInt(strings.TrimSpace(response), 10, 64) // Parse int64
	return size, err
}

func (h *HyDFS) HandleOverwrite(localName, hydfsName string) error {
	data, err := os.ReadFile(localName)
	if err != nil {
		h.logger.Printf("[ERROR] Failed to read local file for overwrite %s: %v", localName, err)
		return err
	}

	primary := h.membershipList.GetPrimary(hydfsName)
	if primary == h.membershipList.selfId {
		h.logger.Printf("[INFO] Overwriting file %s locally as primary", hydfsName)
		return h.LocalOverwrite(hydfsName, data)
	} else {
		h.logger.Printf("[INFO] Overwriting file %s remotely on primary %s", hydfsName, primary)
		return h.RemoteOverwrite(primary, hydfsName, data)
	}
}

func (h *HyDFS) LocalOverwrite(hydfsName string, data []byte) error {
	mutex := h.GetFileLock(hydfsName)
	mutex.Lock()
	defer mutex.Unlock()

	filePath := h.GetFilePath(hydfsName)

	// ensure directory exists
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		h.logger.Printf("[ERROR] Failed to create directories for %s: %v", hydfsName, err)
		return err
	}

	// write (overwrite) to primary node
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		h.logger.Printf("[ERROR] Failed to write file %s: %v", hydfsName, err)
		return err
	}

	h.logger.Printf("[INFO] Successfully overwrote data to file %s locally, replicating...", hydfsName)
	return h.ReplicateAppend(hydfsName, data, true)
}

func (h *HyDFS) RemoteOverwrite(repId string, hydfsName string, data []byte) error { // remotely override
	conn, err := h.DialNode(repId)
	if err != nil {
		return err
	}
	defer conn.Close()

	fmt.Fprintf(conn, "overwrite %s %d\n", hydfsName, len(data))
	conn.Write(data)
	response, _ := bufio.NewReader(conn).ReadString('\n')

	if strings.HasPrefix(response, "OK") {
		return nil
	}

	return errors.New(strings.TrimSpace(response))
}

func (h *HyDFS) SyncAsNewPrimary(hydfsName string, replicas []string) { // sync as new primary (select max size version)
	localPath := h.GetFilePath(hydfsName)
	info, err := os.Stat(localPath)
	localSize := int64(0)
	if err == nil {
		localSize = info.Size()
	}
	maxSize := localSize

	var maxRep string
	for _, rep := range replicas {
		if rep == h.membershipList.selfId {
			continue
		}

		size, err := h.RemoteCompare(rep, hydfsName)
		if err == nil && size > maxSize {
			maxSize = size
			maxRep = rep
		}
	}

	if maxSize > localSize && maxRep != "" {
		data, err := h.RemoteGet(maxRep, hydfsName)
		if err == nil {
			os.WriteFile(localPath, data, 0644)
			h.logger.Printf("[INFO] Synced %s from %s as new primary", hydfsName, maxRep)
		}
	}
}

func (h *HyDFS) Rebalance() { // rebalance all local files
	var files []string
	filepath.Walk(h.storageDir, func(path string, info fs.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			rel, _ := filepath.Rel(h.storageDir, path)
			files = append(files, rel)
		}
		return nil
	})
	for _, hydfsName := range files {
		replicas := h.membershipList.GetReplicas(hydfsName, 3)
		isReplica := false
		for _, rep := range replicas {
			if rep == h.membershipList.selfId {
				isReplica = true
				break
			}
		}
		if !isReplica {
			os.Remove(h.GetFilePath(hydfsName))
			h.logger.Printf("[INFO] Deleted %s during rebalance", hydfsName)
			continue
		}
		primary := replicas[0]
		if primary != h.membershipList.selfId {
			continue // Only primary pushes
		}
		h.SyncAsNewPrimary(hydfsName, replicas) // Sync if new primary
		data, err := os.ReadFile(h.GetFilePath(hydfsName))
		if err != nil {
			continue
		}
		for _, rep := range replicas {
			if rep == h.membershipList.selfId {
				continue
			}
			remoteSize, err := h.RemoteCompare(rep, hydfsName)
			if err == nil && remoteSize == int64(len(data)) {
				continue // Match
			}

			h.logger.Printf("[INFO] Size mismatch for %s on %s, pushing update", hydfsName, rep)
			err = h.RemoteOverwrite(rep, hydfsName, data)
			if err != nil {
				h.logger.Printf("[WARN] Overwrite to %s for %s failed in rebalance: %v", rep, hydfsName, err)
			} else {
				h.logger.Printf("[INFO] Successfully overwritten %s to %s during rebalance", hydfsName, rep)
			}
		}
	}
}

func (h *HyDFS) DialNode(nodeId string) (net.Conn, error) {
	host := StripPortFromAddr(nodeId, true)
	addr := host + ":" + strconv.Itoa(HYDFS_PORT)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	return conn, err
}
