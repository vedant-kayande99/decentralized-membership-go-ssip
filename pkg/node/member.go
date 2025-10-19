package node

import (
	"sync"
	"time"
)

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
}

func NewMemberList(selfAddr string) *MemberList {
	return &MemberList{
		members: map[string]*Member{selfAddr: self(selfAddr)},		
		selfAddr: selfAddr,
	}
}

func self(selfAddr string) *Member {
	return &Member{Addr: selfAddr, Status: Alive, LastSeen: time.Now()}
}

func (ml *MemberList) Add(addr string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if _, exists := ml.members[addr]; !exists {
		ml.members[addr] = &Member{
			Addr: addr,
			Status: Alive,
			LastSeen: time.Now(),
		}
	}
}

func (ml *MemberList) Remove(addr string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	delete(ml.members, addr)
}

func (ml *MemberList) SyncMembers(receivedMembers []Member) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	receivedMap := make(map[string]Member)
	for _, m := range receivedMembers {
		receivedMap[m.Addr] = m
	}

	// update or add members from received list
	for addr, receivedMember := range receivedMap {
		if existingMember, exists := ml.members[addr]; exists {
			if receivedMember.LastSeen.After(existingMember.LastSeen) {
				ml.members[addr] = &Member{
					Addr: receivedMember.Addr,
					Status: receivedMember.Status,
					LastSeen: receivedMember.LastSeen,
				}
			}
		} else {
			ml.members[addr] = &Member{
					Addr: receivedMember.Addr,
					Status: receivedMember.Status,
					LastSeen: receivedMember.LastSeen,
				}
		}
	}

	// mark members as dead if not in received list
	for addr, member := range ml.members {
		if _, exists := receivedMap[addr]; !exists {
			if addr != ml.selfAddr {
				member.Status = Dead
			}
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

func (ml *MemberList) UpdateStatus(addr string, status MemberStatus) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	if member, exists := ml.members[addr]; exists {
		member.Status = status
		member.LastSeen = time.Now()
	}
}
