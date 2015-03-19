package store

import (
	"container/list"
	"encoding/json"
	"log"
	"strconv"
	"time"

	etcdErr "github.com/coreos/etcd/error"
)

type line struct {
	Name        string
	Recycle     time.Duration
	Head        uint64
	FlightStore []message
	flights     *list.List
	parent      *topic
}

type message struct {
	ID  uint64
	Exp time.Time
}

func newLine(name string, recycle time.Duration) *line {
	l := new(line)
	l.Name = name
	l.Recycle = recycle
	l.flights = list.New()
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

	if !found {
		tail := uint64(len(l.parent.Messages)) - 1
		if l.Head > tail {
			return 0, "", nil
		}

		id = l.Head
		l.Head++
	}

	value := l.parent.Messages[id]

	if l.Recycle > 0 {
		msg := new(message)
		msg.ID = id
		msg.Exp = now.Add(l.Recycle)
		l.flights.PushBack(msg)
	}

	return id, value, nil
}

func (l *line) confirm(id uint64) error {
	if l.Recycle <= 0 {
		return etcdErr.NewError(etcdErr.EcodeRootROnly, l.Name, l.parent.parent.parent.CurrentIndex)
	}

	for m := l.flights.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*message)
		if msg.ID == id {
			l.flights.Remove(m)
			return nil
		}
	}

	key := l.Name + "/" + strconv.FormatUint(id, 10)
	return etcdErr.NewError(etcdErr.EcodeKeyNotFound, key, l.parent.parent.parent.CurrentIndex)
}

func (l *line) destroy() {
	l.parent = nil
	l.flights = nil
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
	return json.Marshal(l)
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
