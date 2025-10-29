package node

import (
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"sync"
	"time"
)

var ErrNoPeers = errors.New("[ERROR] No peers in the member list")

type MemberStatus int

const (
	Alive MemberStatus = iota
	Suspect
	Dead
)

func (memStatus MemberStatus) String() string {
	switch(memStatus) {
	case Alive:
		return "Alive"
	case Suspect:
		return "Suspect"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

type Member struct {
	Addr string
	Status MemberStatus	
	HeartbeatCounter int
	StatusChangeTime time.Time
}

func (m Member) String() string {
	return fmt.Sprintf("{Addr: %s, Status: %s, Heartbeat: %d}", m.Addr, m.Status.String(), m.HeartbeatCounter)
}

type MemberList struct {
	members map[string]*Member
	mutex sync.RWMutex
	selfAddr string
	peers []string //members map keys list for O(1) random access
	vectorClock map[string]int
}

func (ml *MemberList) PrintMemberList() {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	for _, member := range ml.members {
		log.Printf("%s", member.String())
	}
}

func NewMemberList(selfAddr string) *MemberList {
	selfMember := self(selfAddr)
	return &MemberList{
		members: map[string]*Member{selfAddr: selfMember},		
		selfAddr: selfAddr,
		peers: make([]string, 0),
		vectorClock: map[string]int{selfAddr: selfMember.HeartbeatCounter},
	}
}

func self(selfAddr string) *Member {
	return &Member{Addr: selfAddr, Status: Alive, HeartbeatCounter: 1}
}

func (ml *MemberList) GetVectorClock() map[string]int {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	clockCopy := make(map[string]int, len(ml.vectorClock))
	maps.Copy(clockCopy, ml.vectorClock)

	return clockCopy
}

func (ml *MemberList) Add(member *Member) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if _, exists := ml.members[member.Addr]; !exists {
		ml.members[member.Addr] = member
		if ml.selfAddr != member.Addr && member.Status == Alive {
			ml.peers = append(ml.peers, member.Addr)
		}
	} else {		
		ml.updateMemberStatus(ml.members[member.Addr], member.Status)		
		ml.members[member.Addr].HeartbeatCounter = member.HeartbeatCounter
	}
	ml.vectorClock[member.Addr] = member.HeartbeatCounter
}

func (ml *MemberList) removePeer(addr string) {
	for i, peerAddr := range ml.peers {
		if peerAddr == addr {
			ml.peers = append(ml.peers[:i], ml.peers[i+1:]...)
			break
		}
	}
}

func (ml *MemberList) Remove(addr string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()
	if _, exists := ml.members[addr]; !exists {		
		return
	}	
	delete(ml.members, addr)
	delete(ml.vectorClock, addr)

	for i, peerAddr := range ml.peers {
		if peerAddr == addr {
			ml.peers = append(ml.peers[:i], ml.peers[i+1:]...)
			break
		}
	}
}

func (ml *MemberList) Merge(receivedMembers []Member) {
	log.Printf("[INFO] Current Membership List of node %s", ml.selfAddr)
	ml.PrintMemberList()
	for _, member := range receivedMembers {
		if member.Addr == ml.selfAddr {
			continue
		}

		ml.mutex.RLock()
		existingMem, exists := ml.members[member.Addr]
		ml.mutex.RUnlock()

		if !exists || member.HeartbeatCounter > existingMem.HeartbeatCounter {
			ml.Add(&member)
		} else if exists && member.HeartbeatCounter == existingMem.HeartbeatCounter {
			if member.Status > existingMem.Status {
				ml.UpdateStatus(member.Addr, member.Status)				
			}
		}

		// no need to update the status change time for Suspect and Dead nodes
		if member.Status == Suspect || member.Status == Dead {
			ml.members[member.Addr].StatusChangeTime = member.StatusChangeTime
		}
	}
	log.Printf("[INFO] Updated Membership List of node %s", ml.selfAddr)
	ml.PrintMemberList()
}

func (ml *MemberList) SyncMembers(receivedMembers []Member) {
	receivedSet := make(map[string]struct{})
	for _, member := range receivedMembers{
		receivedSet[member.Addr] = struct{}{}
		ml.Add(&member)
	}

	currentMembers := ml.GetMembers()
	for _, member := range currentMembers {
		if member.Addr == ml.selfAddr {
			continue
		}

		if _, exists := receivedSet[member.Addr]; !exists {
			ml.Remove(member.Addr)
		}
	}
}

func (ml *MemberList) GetDelta(remoteClock map[string]int) []Member {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	var delta []Member
	for addr, localVersion := range ml.vectorClock {
		remoteVersion, ok := remoteClock[addr]
		if !ok || remoteVersion < localVersion {
			if member, exists := ml.members[addr]; exists {
				delta = append(delta, *member)
				log.Printf("[INFO] Delta member added: %s", member.String())
			}
		}
	}
	return delta
}

func (ml *MemberList) GetMember(addr string) (Member, bool) {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()
	mem, exists := ml.members[addr]
	if !exists {
		return Member{}, false
	}
	return *mem, true
}

func (ml *MemberList) GetMembers() []Member {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	members := make([]Member, 0, len(ml.members))
	for _, m := range ml.members {
		members = append(members, *m)
	}

	return members
}

func (ml *MemberList) GetRandomPeer() (*Member, error) {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	if len(ml.peers) == 0 {		
		return nil, ErrNoPeers
	}

	randIdx := rand.Intn(len(ml.peers))
	peerAddr := ml.peers[randIdx]

	return ml.members[peerAddr], nil
}

func (ml *MemberList) updateMemberStatus(member *Member, status MemberStatus ) {
	oldStatus := member.Status
	member.Status = status
	member.StatusChangeTime = time.Now()

	if oldStatus == Alive && status == Suspect {
		ml.removePeer(member.Addr)
	} else if oldStatus == Suspect && status == Alive {
		ml.peers = append(ml.peers, member.Addr)
	}
}

func (ml *MemberList) UpdateStatus(addr string, status MemberStatus) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if member, exists := ml.members[addr]; exists {
		ml.updateMemberStatus(member, status)
	}
}

func (ml *MemberList) RemoveMember(addr string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()
	
	ml.removePeer(addr)
	delete(ml.members, addr)
	delete(ml.vectorClock, addr)
}

func (ml *MemberList) IncrementHeartbeat() {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if self, ok := ml.members[ml.selfAddr]; ok {
		self.HeartbeatCounter += 1		
		ml.vectorClock[ml.selfAddr] = self.HeartbeatCounter
	}
}

func (ml *MemberList) GetRandomPeers(k int, excludeAddr string) ([]*Member, error) {
	ml.mutex.RLock()
	defer ml.mutex.RUnlock()

	if len(ml.peers) == 0 {
		return nil, ErrNoPeers
	}

	availablePeers := make([]string, 0, len(ml.peers))
	for _, addr := range ml.peers {
		if addr != excludeAddr {
			availablePeers = append(availablePeers, addr)
		}
	}

	if len(availablePeers) == 0 {
		return nil, ErrNoPeers
	}

	count := min(k, len(availablePeers))

	rand.Shuffle(len(availablePeers), func(i, j int) {
		availablePeers[i], availablePeers[j] = availablePeers[j], availablePeers[i]
	})

	randomMembers := make([]*Member, count)
	for i := range count {
		randomMembers[i] = ml.members[availablePeers[i]]
	}

	return randomMembers, nil
}