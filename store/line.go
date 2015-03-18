package store

import (
	"container/list"
	"log"
	"strconv"
	"time"

	etcdErr "github.com/coreos/etcd/error"
)

type line struct {
	Name    string
	Recycle time.Duration
	Head    uint64
	Flights *list.List
	parent  *topic
}

type message struct {
	ID  uint64
	Exp time.Time
}

func newLine(name string, recycle time.Duration) *line {
	l := new(line)
	l.Name = name
	l.Recycle = recycle
	l.Flights = list.New()
	log.Printf("line[%s] created. l.recycle: %v", name, recycle)
	return l
}

func (l *line) pop(now time.Time) (uint64, string, error) {
	found := false
	var id uint64
	if l.Recycle > 0 {
		m := l.Flights.Front()
		if m != nil {
			msg := m.Value.(*message)
			if now.After(msg.Exp) {
				id = msg.ID
				l.Flights.Remove(m)
				found = true
				// log.Printf("found in flights: %v", id)
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
		// log.Printf("found in topic: %v", id)
	}

	value := l.parent.Messages[id]

	if l.Recycle > 0 {
		msg := new(message)
		msg.ID = id
		msg.Exp = now.Add(l.Recycle)
		l.Flights.PushBack(msg)
	}

	return id, value, nil
}

func (l *line) confirm(id uint64) error {
	if l.Recycle <= 0 {
		return etcdErr.NewError(etcdErr.EcodeRootROnly, l.Name, l.parent.parent.parent.CurrentIndex)
	}

	for m := l.Flights.Front(); m != nil; m = m.Next() {
		msg := m.Value.(*message)
		if msg.ID == id {
			l.Flights.Remove(m)
			// log.Printf("confirm in flights: %v", id)
			return nil
		}
	}

	key := l.Name + "/" + strconv.FormatUint(id, 10)
	return etcdErr.NewError(etcdErr.EcodeKeyNotFound, key, l.parent.parent.parent.CurrentIndex)
}
