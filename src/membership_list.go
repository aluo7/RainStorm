package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

type MemberStatus int // defines the state of a node

const (
	ALIVE MemberStatus = iota
	SUSPECTED
	FAILED
	LEFT
)

// string representation of MemberStatus
func (s MemberStatus) String() string {
	return [...]string{"ALIVE", "SUSPECTED", "FAILED", "LEFT"}[s]
}

type Member struct {
	Id               string
	Status           MemberStatus
	HeartbeatCounter int
	IncarnationNum   int
	LastUpdateTime   time.Time
}

// manages the list of members. thread-safe using a RWMutex
type MembershipList struct {
	mu       sync.RWMutex       // can only read/write the members map when mu.Lock() is acquired
	members  map[string]*Member // map from node addr (string) to Member struct
	selfId   string             // self node id (with join timestamp)
	logger   *log.Logger
	tfail    time.Duration // time duration after which a node is considered failed
	tcleanup time.Duration // time duration after which a failed node is removed from the membership list
}

// creates and initializes a new membership list corresponding to each machine
func NewMembershipList(selfId string, incNum int, logger *log.Logger, tfail time.Duration, tcleanup time.Duration) *MembershipList {
	ml := &MembershipList{
		mu:       sync.RWMutex{},
		members:  make(map[string]*Member),
		selfId:   selfId,
		logger:   logger,
		tfail:    tfail,
		tcleanup: tcleanup,
	}

	// add self to list
	ml.members[selfId] = &Member{
		Id:               selfId,
		Status:           ALIVE,
		HeartbeatCounter: 0,
		IncarnationNum:   0,
		LastUpdateTime:   time.Now(),
	}

	return ml
}

// retrieves a member given host address as input with logging
func (ml *MembershipList) Get(addr string) (*Member, bool) {
	// ml.mu.RLock()
	// defer ml.mu.RUnlock()
	member, found := ml.members[addr]
	// ml.logger.Printf("[INFO] Retrieving %s entry from membership list\n", addr)
	return member, found
}

// removes a member from the membership list if it has failed/left with logging
func (ml *MembershipList) Delete(addr string) {
	ml.logger.Printf("[INFO] Deleting %s entry from membership list\n", addr)
	delete(ml.members, addr)
}

// update node before gossip
func (ml *MembershipList) UpdateSelf() {
	// ml.mu.Lock()  // must acquire lock before any r/w operations
	// defer ml.mu.Unlock()

	// ml.logger.Println("[INFO] Updating self entry in membership list")
	if self, found := ml.Get(ml.selfId); found { // search for key ml.selfAddr in the membership list
		self.HeartbeatCounter++
		self.LastUpdateTime = time.Now()
	}
}

// handling for suspicion, increments own incarnation number to override a false rumor
func (ml *MembershipList) RefuteSuspicion() {
	// ml.mu.Lock()
	// defer ml.mu.Unlock()
	if self, found := ml.Get(ml.selfId); found {
		self.IncarnationNum++
		self.HeartbeatCounter++ // Also increment heartbeat to ensure it's fresh
		self.LastUpdateTime = time.Now()
		ml.logger.Printf("[INFO] Refuting suspicion! New incarnation: %d", self.IncarnationNum)
	}
}

// loops through membership list and checks for timeouts
func (ml *MembershipList) CheckTimeouts(suspicion bool) {
	now := time.Now()
	for addr, member := range ml.members {
		if addr == ml.selfId {
			continue // don't check timeout for self
		}

		// transition from ALIVE to SUSPECTED
		if suspicion && member.Status == ALIVE && now.Sub(member.LastUpdateTime) > ml.tfail {
			member.Status = SUSPECTED
			member.LastUpdateTime = now
			ml.logger.Printf("[INFO] Node %s marked as SUSPECTED", addr)
			fmt.Printf("Marked %s as SUSPECTED\n", addr)
			continue
		}

		// transition from ALIVE to FAILED if not in suspicion mode
		if !suspicion && member.Status == ALIVE && now.Sub(member.LastUpdateTime) > ml.tfail {
			member.Status = FAILED
			member.LastUpdateTime = now
			ml.logger.Printf("[INFO] Node %s marked as FAILED", addr)
			continue
		}

		// transition from SUSPECTED to FAILED
		if member.Status == SUSPECTED && now.Sub(member.LastUpdateTime) > ml.tfail {
			member.Status = FAILED
			member.LastUpdateTime = now
			ml.logger.Printf("[INFO] Node %s marked as FAILED", addr)
			continue
		}

		// remove FAILED nodes after cleanup timeout
		if (member.Status == FAILED || member.Status == LEFT) && now.Sub(member.LastUpdateTime) > ml.tcleanup {
			ml.Delete(addr)
			ml.logger.Printf("[INFO] Node %s has been removed after cleanup timeout.", addr)
		}
	}
}

func (ml *MembershipList) GetRandomPeers(k int) []string {
	var peers []string

	for addr, member := range ml.members {
		if addr != ml.selfId && member.Status == ALIVE {
			peers = append(peers, addr)
		}
	}

	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	if len(peers) <= k {
		return peers
	}
	return peers[:k]
}

func (ml *MembershipList) ShufflePeers() []string {
	var peers []string

	for addr, member := range ml.members {
		if addr != ml.selfId && (member.Status == ALIVE || member.Status == SUSPECTED) {
			peers = append(peers, addr)
		}
	}

	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	return peers
}

// MERGE 2 MEMBERSHIP LISTS LOGIC:
// for each member in the incoming membership list
// if the member is not found in the local membership list, it's a new member. add it to the list
//
// if the member is found in the local membership list
// first, check incarnation number. if the incarnation number of the incoming member is higher, override the entry and update lastentrytime
// if incarnation number is equal, check heartbeat and update heartbeat, status, and lastupdatetime
// failed/left are treated the same way w. respect to update conditions/incarnation numbers
func (ml *MembershipList) Merge(incomingList map[string]*Member) {
	for addr, incomingMember := range incomingList {

		if addr == ml.selfId {
			if incomingMember.Status == SUSPECTED {
				ml.RefuteSuspicion()
			} else if incomingMember.Status == FAILED || incomingMember.Status == LEFT {
				ml.logger.Printf("[WARN] Received %s status for self from another node.", incomingMember.Status.String())
			}
			continue
		}

		localMember, found := ml.Get(addr)

		if !found && (incomingMember.Status == ALIVE || incomingMember.Status == SUSPECTED) {
			incomingMember.LastUpdateTime = time.Now()
			ml.members[addr] = incomingMember
			ml.logger.Printf("[INFO] New member added: %s in %s status", addr, incomingMember.Status.String())
			if incomingMember.Status == SUSPECTED {
				fmt.Printf("Marked %s as SUSPECTED\n", addr)
			}
			continue
		} else if !found {
			continue // do not add failed/left nodes
		}

		if localMember.Status == FAILED || localMember.Status == LEFT { // ensure failed nodes are not restarted regardless
			continue
		}

		if incomingMember.IncarnationNum > localMember.IncarnationNum {
			*localMember = *incomingMember
			localMember.LastUpdateTime = time.Now()
			ml.logger.Printf("[INFO] Member %s updated via higher incarnation.", addr)
			continue
		}

		if incomingMember.IncarnationNum == localMember.IncarnationNum {

			// case where update status
			// if incomingMember.Status > localMember.Status { // left > failed > suspected > alive
			// 	localMember.HeartbeatCounter = incomingMember.HeartbeatCounter
			// 	oldStatus := localMember.Status
			// 	localMember.Status = incomingMember.Status
			// 	localMember.LastUpdateTime = time.Now()
			// 	ml.logger.Printf("[INFO] Member %s updated via more recent heartbeat from %s to %s", addr, oldStatus, incomingMember.Status.String())
			// }

			// case where maintain alive status
			if incomingMember.HeartbeatCounter > localMember.HeartbeatCounter {
				localMember.HeartbeatCounter = incomingMember.HeartbeatCounter // update heartbeat counter

				if incomingMember.Status > localMember.Status { // suspected > alive
					localMember.Status = incomingMember.Status
					// ml.logger.Printf("[INFO] Member %s updated via more recent heartbeat.", addr)
				}

				localMember.LastUpdateTime = time.Now()
				ml.logger.Printf("[INFO] Member %s updated via more recent heartbeat to %s.", addr, localMember.Status.String())
				continue
			}

		}
	}
}

// alternate to merge that takes byte array and deserializes
func (ml *MembershipList) MergeFromBytes(data []byte) error {
	if data == nil || len(data) == 0 {
		return nil
	}
	incomingList, err := Deserialize(data)
	if err != nil {
		return err
	}
	ml.Merge(incomingList)
	return nil
}

// serialize the membership list's map into a bytearray
func (ml *MembershipList) Serialize() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(ml.members)
	if err != nil {
		return nil, fmt.Errorf("Failed to serialize membership list: %w", err)
	}

	return buffer.Bytes(), nil
}

// deserialize the bytearray into corresponding membership list
func Deserialize(data []byte) (map[string]*Member, error) {
	var members map[string]*Member
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)

	err := decoder.Decode(&members)
	// fmt.Println("=============================================")
	// fmt.Println("in deserialize:")
	// for _, v := range members {
	// 	fmt.Println(v.String())
	// }
	// fmt.Println("=============================================")
	if err != nil {
		return nil, fmt.Errorf("Failed to deserialize bytearray to membership list: %w", err)
	}

	return members, nil
}

func (ml *MembershipList) MarkNonResponsive(nodeId string, suspicion bool) {
	if m, found := ml.Get(nodeId); found {
		if m.Status == FAILED || m.Status == LEFT {
			return // do nothing if already failed/left
		}

		if suspicion {
			m.Status = SUSPECTED
			ml.logger.Printf("[INFO] Marked %s as SUSPECTED", nodeId)
			if m.Status == SUSPECTED {
				return
			}
			fmt.Printf("Marked %s as SUSPECTED\n", nodeId)
		} else {
			m.Status = FAILED
			ml.logger.Printf("[INFO] Marked %s as FAILED", nodeId)
		}
	}
}

func (ml *MembershipList) MarkSelfLeft() {
	if self, found := ml.Get(ml.selfId); found {
		self.Status = LEFT
		self.IncarnationNum++
		ml.logger.Printf("[INFO] Marked self as LEFT")
	}
}

func (ml *MembershipList) GetAllValidPeers() []*Member {

	var members []*Member
	for addr, member := range ml.members {
		if addr != ml.selfId && (member.Status == ALIVE || member.Status == SUSPECTED) {
			members = append(members, member)
		}
	}
	return members

}

func (ml *MembershipList) GetSuspectedMembers() []*Member {

	var members []*Member
	for _, member := range ml.members {
		if member.Status == SUSPECTED {
			members = append(members, member)
		}
	}
	return members

}

func (ml *MembershipList) GetAllMembers() []*Member {

	var members []*Member
	for _, member := range ml.members {
		members = append(members, member)
	}
	return members

}

func (ml *MembershipList) ClearSuspicions() {
	for addr, member := range ml.members {
		if addr != ml.selfId && member.Status == SUSPECTED {
			member.Status = ALIVE
			ml.logger.Printf("[INFO] Cleared SUSPECTED for %s", addr)
		}
	}
}

func (ml *MembershipList) Size() int {
	return len(ml.members)
}

type hashedNode struct {
	hash uint16
	id   string
}

func (ml *MembershipList) GetSortedAlive() []hashedNode { // returns sorted list of all alive nodes by hash
	ml.mu.RLock()
	defer ml.mu.RUnlock()

	var nodes []hashedNode
	for id, m := range ml.members {
		if m.Status == ALIVE {
			nodes = append(nodes, hashedNode{hash: hashKey(id), id: id})
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].hash < nodes[j].hash })
	return nodes
}

func (ml *MembershipList) GetReplicas(filename string, n int) []string { // finds files responsible for a file using consistent hashing
	fileHash := hashKey(filename)
	sorted := ml.GetSortedAlive()
	if len(sorted) == 0 {
		return nil
	}

	// find first node >= fileHash (immediate successor)
	searchFcn := func(i int) bool {
		return sorted[i].hash >= fileHash
	}
	idx := sort.Search(len(sorted), searchFcn) // binary search

	// retrieve primary (idx) and secondary nodes (idx+1), (idx+2)
	var replicas []string
	for i := 0; i < n; i++ {
		repIdx := (idx + i) % len(sorted) // wrap around the ring if it hits the end of the list
		replicas = append(replicas, sorted[repIdx].id)
	}
	return replicas
}

func (ml *MembershipList) GetPrimary(filename string) string {
	replicas := ml.GetReplicas(filename, 3)
	if replicas == nil || len(replicas) < 3 {
		return ""
	}
	return replicas[0]
}

func (m *Member) String() string {
	return fmt.Sprintf("%30s, %10s, %10s, %10d, %10d, %20s", m.Id, strconv.Itoa(int(hashKey(m.Id))),
		m.Status.String(), m.HeartbeatCounter, m.IncarnationNum, m.LastUpdateTime.Format("15:04:05"))

}

func (ml *MembershipList) String() string {

	keys := make([]int, 0, len(ml.members))
	keysToId := make(map[int]string)
	for k := range ml.members {
		keys = append(keys, int(hashKey(k)))
		keysToId[int(hashKey(k))] = k
	}
	sort.Ints(keys)

	s := ""
	s += fmt.Sprintf("%30s, %10s, %10s, %10s, %10s, %20s\n", "Node ID", "Key", "Status", "Heartbeat", "Incarnation", "Last Update Time")
	s += fmt.Sprintln("----------------------------------------------------------------------------------------------------")

	// s += fmt.Sprintln(ml.members[ml.selfId].String()) // always print self first
	for _, k := range keys {
		s += fmt.Sprintln(ml.members[keysToId[k]].String())
	}
	s += fmt.Sprintln("----------------------------------------------------------------------------------------------------")

	return s
}
