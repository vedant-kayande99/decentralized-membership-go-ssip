package node

import (
	"errors"
	"log"
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

type Member struct {
	Addr string
	Status MemberStatus
	LastSeen time.Time
}

type MemberList struct {
	members map[string]*Member
	mutex sync.RWMutex
	selfAddr string
	peers []string //members map keys list for O(1) random access
}

func NewMemberList(selfAddr string) *MemberList {
	return &MemberList{
		members: map[string]*Member{selfAddr: self(selfAddr)},		
		selfAddr: selfAddr,
		peers: make([]string, 0),
	}
}

func self(selfAddr string) *Member {
	return &Member{Addr: selfAddr, Status: Alive, LastSeen: time.Now()}
}

func (ml *MemberList) Add(member *Member) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if _, exists := ml.members[member.Addr]; !exists {
		ml.members[member.Addr] = member
		if ml.selfAddr != member.Addr {
			ml.peers = append(ml.peers, member.Addr)
		}
	} else {
		ml.members[member.Addr].Status = member.Status
		ml.members[member.Addr].LastSeen = member.LastSeen
	}
}

func (ml *MemberList) Remove(addr string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()
	if _, exists := ml.members[addr]; !exists {		
		return
	}	
	delete(ml.members, addr)

	for i, peerAddr := range ml.peers {
		if peerAddr == addr {
			ml.peers = append(ml.peers[:i], ml.peers[i+1:]...)
			break
		}
	}
}

func (ml *MemberList) Merge(receivedMembers []Member) {
	log.Printf("[INFO] Current Membership List of node %s: %v", ml.selfAddr, ml.members)
	for _, member := range receivedMembers {
		if member.Addr == ml.selfAddr {
			continue
		}

		ml.mutex.RLock()
		existingMem, exists := ml.members[member.Addr]
		ml.mutex.RUnlock()

		if !exists || member.LastSeen.After(existingMem.LastSeen) {
			ml.Add(&member)
		}
	}
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

func (ml *MemberList) UpdateStatus(addr string, status MemberStatus) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if member, exists := ml.members[addr]; exists {
		member.Status = status
		member.LastSeen = time.Now()
	}
}
