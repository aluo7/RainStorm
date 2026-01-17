package main

// rainstorm 1 3 identity "" dataset1.csv output.txt true false 100 0 0
// rainstorm 2 3 filter Traffic count 2 dataset1.csv output1.txt true false 100 0 0

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"encoding/csv"
	"net/rpc"
)

const MAX_JOIN_ATTEMPTS = 10 // max attempts to join via introducer

var hydfsLogger *log.Logger
var fdLogger *log.Logger
var hydfs *HyDFS
var rainStorm *RainStormServer

type RequestType int

const (
	PING_TYPE RequestType = iota
	ACK_TYPE
	PING_REQUEST_TYPE
	PING_FOR_REQUESTER_TYPE
	INDIRECT_ACK_TYPE
	ACK_CHAINED_TYPE
	JOIN_TYPE
	LEAVE_TYPE
)

var RequestTypeToString = []string{"PING", "ACK", "PING_REQUEST", "PING_FOR_REQUESTER", "INDIRECT_ACK", "ACK_CHAINED", "JOIN", "LEAVE"}

func StripTSFromAddr(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

func StripPortFromAddr(addr string, stripTS bool) string {
	if stripTS {
		addr = StripTSFromAddr(addr)
	}
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

func hashKey(key string) uint16 {
	var hash uint16 = 0
	for i := 0; i < len(key); i++ {
		hash = (hash << 5) + hash + uint16(key[i])
	}
	return hash
}

func GetLAddrUDP(port int) (*net.UDPAddr, error) {
	// laddr, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	// if err != nil {
	// 	return nil, err
	// }
	// raddr, err := net.ResolveUDPAddr("udp", "8.8.8.8:80")
	// if err != nil {
	// 	return nil, err
	// }
	// conn, err := net.DialUDP("udp", laddr, raddr)
	// if err != nil {
	// 	return nil, err
	// }
	// defer conn.Close()

	// return conn.LocalAddr().(*net.UDPAddr), nil
	h, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	laddr, err := net.ResolveUDPAddr("udp", h+":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	return laddr, nil
}

func Join(membershipList *MembershipList, introducerAddr *net.UDPAddr, laddr_out *net.UDPAddr) error {
	return nil
}

func Leave(membershipList *MembershipList, laddr_leave *net.UDPAddr) error {
	return nil
}

func InputLoop(membershipList *MembershipList, laddr_leave *net.UDPAddr, fd *FailureDetectionModule, hydfs *HyDFS) {

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			continue
		}

		input = strings.TrimSpace(input)
		r := csv.NewReader(strings.NewReader(input))
		r.Comma = ' ' // Use space as delimiter
		r.LazyQuotes = true
		r.TrimLeadingSpace = true
		inputArr, err := r.Read()
		if err != nil {
		    // Fallback if CSV parsing fails (e.g. strict quoting error)
		    inputArr = strings.Fields(input)
		}

		if len(inputArr) == 0 {
			continue
		}

		cmd := strings.ToLower(inputArr[0])

		if input == "display_protocol" {

			fd.modeLock.RLock()
			fmt.Printf("<%s, %s>\n", DisseminationModeToString[fd.dmode], SuspicionModeToString[fd.smode])
			fd.modeLock.RUnlock()

		} else if input == "list_mem" || input == "l" {
			membershipList.mu.RLock()
			if membershipList.Size() == 0 {
				fmt.Println("Empty membership list")
			} else {
				fmt.Println("Full Membership List:")
				fmt.Println(membershipList.String())
			}
			membershipList.mu.RUnlock()

		} else if input == "list_self" || input == "i" {

			membershipList.mu.RLock()
			self := membershipList.selfId
			membershipList.mu.RUnlock()

			fmt.Printf("%s (%d)\n", self, hashKey(self))

		} else if input == "join" {
			// nothing
		} else if input == "leave" {
			_ = Leave(membershipList, laddr_leave)
			break
		} else if input == "q" {
			break
		} else if cmd == "create" && len(inputArr) == 3 {

			localName := inputArr[1]
			hydfsName := inputArr[2]

			// dummy data
			// if _, err := os.Stat(localName); os.IsNotExist(err) {
			// 	fmt.Printf("Creating dummy file: %s\n", localName)
			// 	dummyData := []byte("This is test data for " + localName)
			// 	os.WriteFile(localName, dummyData, 0644)
			// }

			err := hydfs.HandleCreate(localName, hydfsName) // TODO
			if err != nil {
				fmt.Println("[Error] Create failed: ", err)
			} else {
				fmt.Println("[Success] File created!")
			}

		} else if cmd == "append" && len(inputArr) == 3 {
			localName := inputArr[1]
			hydfsName := inputArr[2]

			// if _, err := os.Stat(localName); os.IsNotExist(err) {
			// 	fmt.Printf("Creating dummy file: %s\n", localName)
			// 	dummyData := []byte("\n...Appending data from " + localName)
			// 	os.WriteFile(localName, dummyData, 0644)
			// }

			err := hydfs.HandleAppend(localName, hydfsName) // TODO
			if err != nil {
				fmt.Println("[Error] Append failed: ", err)
			} else {
				fmt.Println("[Success] File appended!")
			}
		} else if cmd == "get" && len(inputArr) == 3 {
			hydfsName := inputArr[1]
			localName := inputArr[2]
			err := hydfs.HandleGet(hydfsName, localName) // TODO
			if err != nil {
				fmt.Println("[Error] Get failed: ", err)
			} else {
				fmt.Println("[Success] File retrieved!")
			}
		} else if cmd == "merge" && len(inputArr) == 2 {
			hydfsName := inputArr[1]
			err := hydfs.HandleMerge(hydfsName)
			if err != nil {
				fmt.Println("[Error] Merge failed: ", err)
			} else {
				fmt.Println("[Success] Merge completed!")
			}
		} else if cmd == "multiappend" && len(inputArr) >= 2 {
			err := hydfs.HandleMultiAppend(inputArr[1:])
			if err != nil {
				fmt.Println("[Error] Multiappend failed:", err)
			} else {
				fmt.Println("[Success] Multiappend completed!")
			}
		} else if cmd == "ls" && len(inputArr) == 2 {
			hydfsName := inputArr[1]
			out, err := hydfs.HandleLS(hydfsName)
			if err != nil {
				fmt.Println("[Error] LS failed: ", err)
			} else {
				fmt.Printf("%s", out)
			}
		} else if cmd == "liststore" {
			out, err := hydfs.HandleListStore()
			if err != nil {
				fmt.Println("[Error] ListStore failed: ", err)
			} else {
				fmt.Printf("%s", out)
			}
		} else if cmd == "getfromreplica" && len(inputArr) == 4 {
			nodeAddr := inputArr[1]
			hydfsName := inputArr[2]
			localName := inputArr[3]
			err := hydfs.HandleGetFromReplica(nodeAddr, hydfsName, localName)
			if err != nil {
				fmt.Println("[Error] GetFromReplica failed: ", err)
			} else {
				fmt.Println("[Success] File retrieved from replica!")
			}
		} else if cmd == "list_mem_ids" {
			membershipList.mu.RLock()
			if membershipList.Size() == 0 {
				fmt.Println("Empty membership list")
			} else {
				fmt.Println("Full Membership List:")
				fmt.Println(membershipList.String())
			}
			membershipList.mu.RUnlock()
		} else if cmd == "rainstorm" {
			if len(inputArr) < 9 {
				fmt.Println("Invalid RainStorm args")
				continue
			}

			nStages, _ := strconv.Atoi(inputArr[1])
			nTasks, _ := strconv.Atoi(inputArr[2])

			var ops []OperatorInfo
			currArg := 3
			for i := 0; i < nStages; i++ {
				opExe := inputArr[currArg]
				opArg := inputArr[currArg+1]
				ops = append(ops, OperatorInfo{Exe: opExe, Args: []string{opArg}})
				currArg += 2
			}

			src := inputArr[currArg]
			dest := inputArr[currArg+1]
			eo, _ := strconv.ParseBool(inputArr[currArg+2])
			as, _ := strconv.ParseBool(inputArr[currArg+3])
			rate, _ := strconv.Atoi(inputArr[currArg+4])
			lw, _ := strconv.Atoi(inputArr[currArg+5])
			hw, _ := strconv.Atoi(inputArr[currArg+6])

			config := RainStormConfig{
				NumStages: nStages,
				NumTasksPerStage: nTasks,
				Operators: ops,
				HydfsSrc: src,
				HydfsDest: dest,
				ExactlyOnce: eo,
				Autoscale: as,
				InputRate: rate,
				LowWatermark: lw,
				HighWatermark: hw,
			}

			rainStorm.StartJob(config)

		} else if cmd == "list_tasks" {
			rainStorm.ListTasks()

		} else if cmd == "kill_task" && len(inputArr) == 3 {
		    targetVM := inputArr[1]
		    targetPID := inputArr[2]

		    client, err := rpc.Dial("tcp", StripPortFromAddr(targetVM, true)+":"+strconv.Itoa(RAINSTORM_RPC_PORT))
		    if err != nil {
		        fmt.Println("[Error] Failed to dial VM:", err)
		        return
		    }
		    defer client.Close()

		    var reply bool
		    err = client.Call("RainStormServer.KillTaskByPID", targetPID, &reply)
		    if err != nil {
		        fmt.Println("[Error] Remote kill failed:", err)
		    } else {
		        fmt.Println("[Success] Remote task killed")
		    }
		} else {
			continue
		}
	}
}

func main() {

	hydfsLogFile, err := os.OpenFile("server.hydfs.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening log file:", err)
		os.Exit(1)
	}
	defer hydfsLogFile.Close()
	fdLogFile, err := os.OpenFile("server.fd.log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error opening log file:", err)
		os.Exit(1)
	}
	defer hydfsLogFile.Close()
	hydfsLogger = log.New(hydfsLogFile, "", log.Lshortfile|log.Lmicroseconds)
	fdLogger = log.New(fdLogFile, "", log.Lshortfile|log.Lmicroseconds)

	fdLogger.Println("[INFO] Failure Detection log beginning")
	hydfsLogger.Println("[INFO] HyDFS log beginning")

	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "Usage: server <detection mode | introducer>")
		hydfsLogger.Println("[FATAL] Incorrect number of arguments")
		os.Exit(1)
	}

	t_now := time.Now().UnixMilli()

	laddr_in, err := GetLAddrUDP(FD_IN_PORT)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error getting local UDP address:", err)
		hydfsLogger.Println("[FATAL] Error getting local UDP address")
		os.Exit(1)
	}

	laddr_out, err := GetLAddrUDP(FD_OUT_PORT)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error getting local UDP address:", err)
		hydfsLogger.Println("[FATAL] Error getting local UDP address")
		os.Exit(1)
	}

	laddr_leave, err := GetLAddrUDP(FD_LEAVE_PORT)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error getting local UDP address:", err)
		hydfsLogger.Println("[FATAL] Error getting local UDP address")
		os.Exit(1)
	}

	selfId := StripPortFromAddr(laddr_in.String(), false) + ":" + strconv.FormatInt(t_now, 10)
	fmt.Println("Node ID:", selfId)

	membershipList := NewMembershipList(selfId, 0, fdLogger, T_FAIL, T_CLEANUP)
	fd := NewFailureDetectionModule(membershipList, GOSSIP, NO_SUSPICION, fdLogger)

	storageDir := STORAGE_DIR + "_" + strconv.FormatInt(t_now, 10)
	hydfs = NewHyDFS(membershipList, hydfsLogger, storageDir)

	rainStorm = NewRainStormServer(membershipList, hydfs, hydfsLogger)

	isIntroducer := len(os.Args) == 3
	if isIntroducer {

		// TODO

	} else {

		introducerAddr, err := net.ResolveUDPAddr("udp", os.Args[1]+":"+strconv.Itoa(FD_IN_PORT))
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error connecting to introducer:", err)
			hydfsLogger.Println("[FATAL] Error connecting to introducer")
			os.Exit(1)
		}

		err = errors.New("")
		i := 0
		for err != nil && i < MAX_JOIN_ATTEMPTS {
			hydfsLogger.Printf("[INFO] Attempt %d to join via introducer %s\n", i+1, introducerAddr.String())
			i++
			err = fd.Join(introducerAddr, laddr_out)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error connecting to introducer:", err)
				hydfsLogger.Println("[WARN] Error connecting to introducer")
			} else {
				break
			}
		}
		if err != nil {
			hydfsLogger.Println("[FATAL] Failed to join via introducer")
			os.Exit(1)
		}
	}

	// if !isIntroducer {
	// 	fmt.Printf("Starting node in mode: <%s, %s>\n", dmodeMapInv[fd.dmode], smodeMapInv[fd.smode])
	// } else {
	// 	fmt.Printf("Starting node as introducer in mode: <%s, %s>\n", dmodeMapInv[dmode], smodeMapInv[smode])
	// }
	// logger.Printf("[INFO] Starting server in mode: (%s, %s)\n", DisseminationModeToString[dmode], SuspicionModeToString[smode])
	// logger.Println("[INFO] Introducer:", isIntroducer)

	go fd.UpdateMembershipListLoop()

	go fd.SetupUDPListenServer(laddr_in)

	go fd.PeriodicPingLoop(laddr_out)

	// preload all files in t/ directory
	// go func() {
	// 	time.Sleep(10 * time.Second)
	// 	if isIntroducer {
	// 		files, err := os.ReadDir("t")
	// 		if err != nil {
	// 			fmt.Println("Error reading t/ directory:", err)
	// 			os.Exit(1)
	// 		}
	// 		for _, file := range files {
	// 			if file.IsDir() {
	// 				continue
	// 			}
	// 			localName := "t/" + file.Name()
	// 			hydfsName := file.Name()
	// 			err := hydfs.HandleCreate(localName, hydfsName)
	// 			if err != nil {
	// 				fmt.Printf("Failed to preload %s: %v\n", localName, err)
	// 				os.Exit(1)
	// 			}
	// 		}
	// 	}
	// }()

	InputLoop(membershipList, laddr_leave, fd, hydfs)

	os.Exit(0)
}
