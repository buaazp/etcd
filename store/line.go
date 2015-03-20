package store

import (
	"container/list"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	etcdErr "github.com/coreos/etcd/error"
)

type line struct {
	Name        string
	Recycle     time.Duration
	Head        uint64
	FlightHead  uint64
	flights     *list.List
	Flighted    map[string]bool
	FlightStore []message
	parent      *topic
}

type message struct {
	ID  uint64
	Exp time.Time
}

func newLine(name string, head uint64, recycle time.Duration) *line {
	l := new(line)
	l.Name = name
	l.Recycle = recycle
	l.Head = head
	l.FlightHead = head
	l.flights = list.New()
	l.Flighted = make(map[string]bool)
	log.Printf("queue: line created. [%s] recycle: %v", name, recycle)
	return l
}

func (l *line) pop(now time.Time) (uint64, string, error) {
	found := false
	var id uint64
	if l.Recycle > 0 {
		m := l.flights.Front()
		if m != nil {
			msg := m.Value.(*message)
			if now.After(msg.Exp) {
				id = msg.ID
				l.flights.Remove(m)
				found = true
			}
		}
	}

	t := l.parent
	if !found {
		if l.Head >= t.Tail {
			return 0, "", nil
		}

		id = l.Head
		ids := strconv.FormatUint(id, 10)
		l.Flighted[ids] = true
		l.Head++
	}

	key := fmt.Sprintf("%s/%d", t.Name, id)
	value, err := l.parent.DB.Get(key)
	if err != nil {
		log.Printf("queue: line db get %s error: %s", key, err)
		return 0, "", etcdErr.NewError(etcdErr.EcodeKeyNotFound, key, t.parent.parent.CurrentIndex)
	}

	if l.Recycle > 0 {
		msg := new(message)
		msg.ID = id
		msg.Exp = now.Add(l.Recycle)
		l.flights.PushBack(msg)
	}

	return id, value, nil
}

func (l *line) updateFlightHead() {
	for l.FlightHead < l.Head {
		ids := strconv.FormatUint(l.FlightHead, 10)
		fl, ok := l.Flighted[ids]
		if !ok {
			l.FlightHead++
			continue
		}
		if fl {
			return
		} else {
			delete(l.Flighted, ids)
			l.FlightHead++
		}
	}
}

func (l *line) confirm(id uint64) error {
	if l.Recycle <= 0 {
		return etcdErr.NewError(etcdErr.EcodeRootROnly, l.Name, l.parent.parent.parent.CurrentIndex)
	}

	for m := l.flights.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*message)
		if msg.ID == id {
			l.flights.Remove(m)
			ids := strconv.FormatUint(id, 10)
			l.Flighted[ids] = false
			l.updateFlightHead()
			return nil
		}
	}

	key := l.Name + "/" + strconv.FormatUint(id, 10)
	return etcdErr.NewError(etcdErr.EcodeKeyNotFound, key, l.parent.parent.parent.CurrentIndex)
}

func (l *line) destroy() {
	l.parent = nil
	l.flights = nil
	l.Flighted = nil
}

func (l *line) save() ([]byte, error) {
	flightStore := make([]message, l.flights.Len())
	i := 0
	for m := l.flights.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*message)
		flightStore[i] = *msg
		i++
	}
	l.FlightStore = flightStore
	log.Printf("queue: flights save succ. [%s] %v", l.Name, len(flightStore))
	b, err := json.Marshal(l)
	if err != nil {
		log.Printf("queue: flights marshal error. [%s] %v", l.Name, err)
		return nil, err
	}
	return b, nil
}

func (l *line) recovery() {
	flights := list.New()
	for i, _ := range l.FlightStore {
		msg := &l.FlightStore[i]
		flights.PushBack(msg)
	}
	l.flights = flights
	log.Printf("queue: line recovery succ. [%s] %v", l.Name, len(l.FlightStore))
}
