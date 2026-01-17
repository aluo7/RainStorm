package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

const FD_OUT_PORT = 9098
const FD_IN_PORT = 9099
const FD_LEAVE_PORT = 9097

const MEMBERSHIP_UPDATE_INTERVAL = 100 * time.Millisecond
const PING_INTERVAL = 200 * time.Millisecond
const PING_K = 3

var DROP_RATE float32 = 0.0

const T_FAIL = 2000 * time.Millisecond
const T_CLEANUP = 2000 * time.Millisecond
const T_PING_WAIT = 200 * time.Millisecond

type DisseminationMode int
type SuspicionMode int

const (
	GOSSIP DisseminationMode = iota
	PINGACK
)

const (
	SUSPICION SuspicionMode = iota
	NO_SUSPICION
)

type FailureDetectionModule struct {
	membershipList  *MembershipList
	modeLock        sync.RWMutex
	dmode           DisseminationMode
	smode           SuspicionMode
	modeIncarnation int
	logger          *log.Logger
}

var DisseminationModeToString = []string{"GOSSIP", "PING-ACK"}
var SuspicionModeToString = []string{"SUSPICION", "NO SUSPICION"}

func NewFailureDetectionModule(membershipList *MembershipList, dmode DisseminationMode, smode SuspicionMode, logger *log.Logger) *FailureDetectionModule {
	var fd = &FailureDetectionModule{
		membershipList:  membershipList,
		modeLock:        sync.RWMutex{},
		dmode:           dmode,
		smode:           smode,
		modeIncarnation: 0,
		logger:          logger,
	}

	return fd
}

func (fdm *FailureDetectionModule) UpdateMembershipListLoop() {
	var lastSize int
	for {
		fdm.membershipList.mu.Lock()
		fdm.membershipList.CheckTimeouts(fdm.smode == SUSPICION)
		currentSize := fdm.membershipList.Size()
		fdm.membershipList.mu.Unlock()
		
		if currentSize != lastSize {
			// membership changed, trigger rebalance
			hydfs.Rebalance()
			lastSize = currentSize
		}
		time.Sleep(MEMBERSHIP_UPDATE_INTERVAL)
	}
}

func (fdm *FailureDetectionModule) ConditionalModeUpdateFromHeader(header []byte) {
	incomingDMode := DisseminationMode((int(header[0]) >> 1) & 1)
	incomingSMode := SuspicionMode(int(header[0]) & 1)
	incomingModeIncarnation := int(header[0]) >> 2

	if incomingDMode != fdm.dmode || incomingSMode != fdm.smode {
		if incomingModeIncarnation > fdm.modeIncarnation {
			fdm.modeLock.Lock()
			fdm.dmode = incomingDMode
			fdm.smode = incomingSMode
			fdm.modeIncarnation = incomingModeIncarnation
			fdm.modeLock.Unlock()

			fdm.logger.Printf("[INFO] Received mode change to (%s, %s, inc %d)\n",
				DisseminationModeToString[incomingDMode], SuspicionModeToString[incomingSMode], incomingModeIncarnation)
		}
	}
}

func BuildHeader(currentDmode DisseminationMode, currentSmode SuspicionMode, currentModeInc int, requestType RequestType) []byte {
	b1 := byte(int(currentDmode)<<1 + int(currentSmode) + currentModeInc<<2)
	b2 := byte(requestType)
	return []byte{b1, b2}
}

func simDrop() bool {
	return rand.Float32() < DROP_RATE
}

func (fdm *FailureDetectionModule) PeriodicPingLoop(laddr *net.UDPAddr) {
	fdm.logger.Println("[INFO] Running ping loop")

	fdm.membershipList.mu.RLock()
	pingTargets := fdm.membershipList.ShufflePeers()
	fdm.membershipList.mu.RUnlock()

	responseBuffer := map[string][]byte{}
	responseBufferLock := sync.RWMutex{}

	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		fdm.logger.Printf("[ERROR] Failed to listen on %v: %s\n", laddr.String(), err)
		os.Exit(1)
	}
	defer conn.Close()

	go func() {
		for true {
			buffer := make([]byte, 1024)
			n, raddr, err := conn.ReadFromUDP(buffer)
			if err != nil || n < 2 {
				continue
			}
			fdm.logger.Printf("[INFO] Received unclassified message from %v\n", raddr.String())
			responseBufferLock.Lock()
			responseBuffer[raddr.String()] = make([]byte, n)
			copy(responseBuffer[raddr.String()], buffer[:n])
			fdm.logger.Printf("[INFO] Added to map unclassified message from %v\n", raddr.String())
			responseBufferLock.Unlock()
		}
	}()

	for {
		time.Sleep(PING_INTERVAL)

		fdm.modeLock.RLock()
		currentDMode := fdm.dmode
		currentSMode := fdm.smode
		currentModeIncarnation := fdm.modeIncarnation
		fdm.modeLock.RUnlock()

		fdm.membershipList.mu.Lock()
		fdm.membershipList.UpdateSelf()
		if len(pingTargets) < min(PING_K, fdm.membershipList.Size()-1) {
			pingTargets = append(pingTargets, fdm.membershipList.ShufflePeers()...)
		}
		fdm.membershipList.mu.Unlock()

		if len(pingTargets) == 0 {
			continue
		}

		header := BuildHeader(currentDMode, currentSMode, currentModeIncarnation, PING_TYPE)

		if currentDMode == GOSSIP {
			k := min(PING_K, len(pingTargets))
			for _, target := range pingTargets[:k] {
				raddr, err := net.ResolveUDPAddr("udp", StripTSFromAddr(target)+":"+strconv.Itoa(FD_IN_PORT))
				if err != nil {
					continue
				}
				fdm.logger.Printf("[INFO] Randomly pinging %s in (%s, %s) mode\n", raddr.String(),
					DisseminationModeToString[currentDMode], SuspicionModeToString[currentSMode])

				responseBufferLock.Lock()
				delete(responseBuffer, raddr.String())
				responseBufferLock.Unlock()

				fdm.membershipList.mu.RLock()
				membershipData, err := fdm.membershipList.Serialize()
				fdm.membershipList.mu.RUnlock()
				if err != nil {
					continue
				}
				data := append(header, membershipData...)
				_, err = conn.WriteToUDP(data, raddr)
				if err != nil {
					continue
				}
			}
			pingTargets = pingTargets[k:]
		} else if currentDMode == PINGACK {
			k := min(PING_K, len(pingTargets))
			for _, target := range pingTargets[:k] {
				go func(target string) {
					raddr, err := net.ResolveUDPAddr("udp", StripTSFromAddr(target)+":"+strconv.Itoa(FD_IN_PORT))
					if err != nil {
						return
					}
					fdm.logger.Printf("[INFO] Randomly pinging %s in (%s, %s) mode\n", raddr.String(),
						DisseminationModeToString[currentDMode], SuspicionModeToString[currentSMode])

					fdm.membershipList.mu.RLock()
					membershipData, err := fdm.membershipList.Serialize()
					fdm.membershipList.mu.RUnlock()
					if err != nil {
						return
					}
					data := append(header, membershipData...)
					_, err = conn.WriteToUDP(data, raddr)
					if err != nil {
						return
					}
					var pingTime = time.Now()
					var buffer = make([]byte, 0)
					var responseFound = false
					for !responseFound && time.Since(pingTime) < T_PING_WAIT {
						responseBufferLock.RLock()
						if val, exists := responseBuffer[raddr.String()]; exists {
							buffer = val
							responseFound = true
						}
						responseBufferLock.RUnlock()
					}
					if !responseFound {
						fdm.logger.Printf("[WARN] No ack received from %s\n", raddr.String())
						fdm.membershipList.mu.Lock()
						fdm.membershipList.MarkNonResponsive(target, currentSMode == SUSPICION)
						fdm.membershipList.mu.Unlock()
						return
					} else if err != nil || len(buffer) < 2 {
						fdm.logger.Printf("[WARN] Error reading ack from %s: %s\n", raddr.String(), err)
						return
					} else if simDrop() {
						fdm.logger.Printf("[WARN] %s request dropped from %v\n", raddr.String(), RequestType(buffer[1]))
						return
					}
					fdm.logger.Printf("[INFO] Received ack received from %s\n", raddr.String())
					fdm.ConditionalModeUpdateFromHeader(buffer[:2])
					requestType := RequestType(buffer[1])
					if requestType != ACK_TYPE {
						return
					}
					fdm.membershipList.mu.Lock()
					err = fdm.membershipList.MergeFromBytes(buffer[2:])
					fdm.membershipList.mu.Unlock()
					if err != nil {
						return
					}
				}(target)
			}
			pingTargets = pingTargets[k:]
		}
	}
}

func (fdm *FailureDetectionModule) SetupUDPListenServer(laddr *net.UDPAddr) {
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		fdm.logger.Println("[ERROR] Error starting server:", err)
		fmt.Fprintln(os.Stderr, "Error starting server:", err)
		os.Exit(1)
	}
	defer conn.Close()
	fdm.logger.Println("[INFO] UDP server listening on port:", FD_IN_PORT)
	buffer := make([]byte, 1024)
	for {
		for i := range buffer {
			buffer[i] = 0
		}
		n, raddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fdm.logger.Println("[ERROR] Error reading from UDP connection:", err)
			continue
		}
		if simDrop() && RequestType(buffer[1]) != JOIN_TYPE {
			fdm.logger.Printf("[WARN] %s request dropped from %v\n", raddr.String(), RequestType(buffer[1]))
			continue
		}
		fdm.ConditionalModeUpdateFromHeader(buffer[:2])
		fdm.modeLock.RLock()
		currentDMode := fdm.dmode
		currentSMode := fdm.smode
		currentModeIncarnation := fdm.modeIncarnation
		fdm.modeLock.RUnlock()
		requestType := RequestType(buffer[1])
		fdm.logger.Printf("[INFO] Received %s request from %v in (%s, %s) mode\n",
			RequestTypeToString[requestType], raddr.String(),
			DisseminationModeToString[currentDMode], SuspicionModeToString[currentSMode])
		if requestType == PING_TYPE {
			if currentDMode == GOSSIP {
				fdm.membershipList.mu.Lock()
				err := fdm.membershipList.MergeFromBytes(buffer[2:n])
				fdm.membershipList.mu.Unlock()
				if err != nil {
					fdm.logger.Printf("[WARN] Failed to merge membership list from %v: %s\n", raddr.String(), err)
					continue
				}
			} else if currentDMode == PINGACK {
				fdm.membershipList.mu.Lock()
				err1 := fdm.membershipList.MergeFromBytes(buffer[2:n])
				membershipData, err2 := fdm.membershipList.Serialize()
				fdm.membershipList.mu.Unlock()
				if err1 != nil || err2 != nil {
					fdm.logger.Printf("[WARN] Failed to merge/serialize membership list from %v: %s, %s\n", raddr.String(), err1, err2)
					continue
				}
				header := BuildHeader(currentDMode, currentSMode, currentModeIncarnation, ACK_TYPE)
				responseData := append(header, membershipData...)
				_, err = conn.WriteToUDP(responseData, raddr)
				if err != nil {
					fdm.logger.Printf("[WARN] Failed to send ACK to %v: %s\n", raddr.String(), err)
					continue
				}
				fdm.logger.Printf("[INFO] Sent ACK to %v\n", raddr.String())
			}
		} else if requestType == JOIN_TYPE {
			fdm.membershipList.mu.Lock()
			err1 := fdm.membershipList.MergeFromBytes(buffer[2:n])
			membershipData, err2 := fdm.membershipList.Serialize()
			fdm.membershipList.mu.Unlock()
			if err1 != nil || err2 != nil {
				fdm.logger.Printf("[WARN] Failed to merge/serialize membership list from %v: %s, %s\n", raddr.String(), err1, err2)
				continue
			}
			header := BuildHeader(currentDMode, currentSMode, currentModeIncarnation, ACK_TYPE)
			responseData := append(header, membershipData...)
			_, err = conn.WriteToUDP(responseData, raddr)
			if err != nil {
				fdm.logger.Printf("[WARN] Failed to send ACK to %v: %s\n", raddr.String(), err)
				continue
			}
		} else if requestType == LEAVE_TYPE {
			fdm.membershipList.mu.Lock()
			err := fdm.membershipList.MergeFromBytes(buffer[2:n])
			fdm.membershipList.mu.Unlock()
			if err != nil {
				fdm.logger.Printf("[WARN] Failed to merge membership list from %v: %s\n", raddr.String(), err)
				continue
			}
		}
	}
}

func (fdm *FailureDetectionModule) Join(introducerAddr *net.UDPAddr, laddr_out *net.UDPAddr) error {
	fdm.membershipList.mu.RLock()
	membershipData, err := fdm.membershipList.Serialize()
	fdm.membershipList.mu.RUnlock()
	if err != nil {
		return err
	}
	outConn, err := net.DialUDP("udp", laddr_out, introducerAddr)
	if err != nil {
		return err
	}
	defer outConn.Close()
	fdm.logger.Printf("[INFO] Sending join request to %v\n", introducerAddr.String())
	header := BuildHeader(fdm.dmode, fdm.smode, fdm.modeIncarnation, JOIN_TYPE)
	responseData := append(header, membershipData...)
	outConn.Write(responseData)
	buffer := make([]byte, 1024)
	outConn.SetDeadline(time.Now().Add(T_PING_WAIT))
	n, err := outConn.Read(buffer)
	if err != nil || n < 2 {
		fdm.logger.Printf("[ERROR] Failed to read join response from %v: %s\n", introducerAddr.String(), err)
		return err
	}
	fdm.ConditionalModeUpdateFromHeader(buffer[:2])
	fdm.membershipList.mu.Lock()
	fdm.membershipList.MergeFromBytes(buffer[2:n])
	fdm.membershipList.mu.Unlock()
	return nil
}

func (fdm *FailureDetectionModule) Leave(laddr_leave *net.UDPAddr) error {
	fdm.membershipList.mu.Lock()
	fdm.membershipList.MarkSelfLeft()
	membershipData, err := fdm.membershipList.Serialize()
	fdm.membershipList.mu.Unlock()
	if err != nil {
		return err
	}
	for _, m := range fdm.membershipList.GetAllValidPeers() {
		raddr, err := net.ResolveUDPAddr("udp", StripTSFromAddr(m.Id)+":"+strconv.Itoa(FD_IN_PORT))
		if err != nil {
			fdm.logger.Printf("[ERROR] Failed to resolve %v: %s\n", StripTSFromAddr(m.Id), err)
			continue
		}
		outConn, err := net.DialUDP("udp", laddr_leave, raddr)
		if err != nil {
			fdm.logger.Printf("[ERROR] Failed to dial %v: %s\n", raddr.String(), err)
			continue
		}
		header := BuildHeader(fdm.dmode, fdm.smode, fdm.modeIncarnation, LEAVE_TYPE)
		responseData := append(header, membershipData...)
		_, err = outConn.Write(responseData)
		if err != nil {
			fdm.logger.Printf("[ERROR] Failed to send leave to %v: %s\n", raddr.String(), err)
			continue
		}
		outConn.Close()
		fdm.logger.Printf("[INFO] Sent leave to %v\n", raddr.String())
	}
	return nil
}
