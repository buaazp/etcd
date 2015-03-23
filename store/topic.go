package store

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/coreos/etcd/db"
	etcdErr "github.com/coreos/etcd/error"
)

const (
	MaxUint uint64 = ^uint64(0)
	MinUint uint64 = 0
)

type topic struct {
	Name      string
	Head      uint64
	Tail      uint64
	LineStore map[string][]byte
	lines     map[string]*line
	SDB       *db.MDB
	mdb       db.DB
	parent    *queue
}

func newTopic(name, dbpath string) (*topic, error) {
	t := new(topic)
	t.Name = name
	if dbpath != "" {
		ldb, err := db.NewLDB(dbpath)
		if err != nil {
			return nil, err
		}
		t.mdb = ldb
	} else {
		t.mdb = db.NewMDB()
	}
	t.lines = make(map[string]*line)
	log.Printf("queue: topic created. [%s] ", name)
	return t, nil
}

func (t *topic) addLine(name string, recycle time.Duration) error {
	if _, ok := t.lines[name]; ok {
		return etcdErr.NewError(etcdErr.EcodeNodeExist, name, t.parent.parent.CurrentIndex)
	}

	l := newLine(name, t.Head, recycle)
	l.parent = t
	t.lines[name] = l
	return nil
}

func (t *topic) push(value string) error {
	key := fmt.Sprintf("%s/%d", t.Name, t.Tail)
	err := t.mdb.Set(key, value)
	if err != nil {
		return err
	}
	t.Tail++
	return nil
}

func (t *topic) pop(name string, now time.Time) (uint64, string, error) {
	l, ok := t.lines[name]
	if !ok {
		return 0, "", etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, t.parent.parent.CurrentIndex)
	}
	return l.pop(now)
}

func (t *topic) confirm(name string, id uint64) error {
	l, ok := t.lines[name]
	if !ok {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, t.parent.parent.CurrentIndex)
	}
	return l.confirm(id)
}

func (t *topic) delLine(name string) error {
	l, ok := t.lines[name]
	if !ok {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, t.parent.parent.CurrentIndex)
	}

	l.destroy()
	delete(t.lines, name)
	log.Printf("queue: line removed. [%s]", name)
	return nil
}

func (t *topic) delLines() {
	for name, l := range t.lines {
		l.destroy()
		delete(t.lines, name)
		log.Printf("queue: line removed. [%s]", name)
	}
}

func (t *topic) destroy() {
	t.delLines()
	t.lines = nil
	t.parent = nil
	t.mdb.Close()
}

func (t *topic) save() ([]byte, error) {
	lineStore := make(map[string][]byte)
	for name, l := range t.lines {
		b, err := l.save()
		if err != nil {
			return nil, err
		}
		lineStore[name] = b
		log.Printf("queue: line save succ. [%s] %d", name, len(b))
	}
	t.LineStore = lineStore
	if t.mdb.Type() == db.TypeMDB {
		t.SDB = t.mdb.(*db.MDB)
	}
	return json.Marshal(t)
}

func (t *topic) recovery(dbpath string) error {
	if dbpath != "" {
		ldb, err := db.NewLDB(dbpath)
		if err != nil {
			log.Printf("queue: topic recovery error. %s %v", t.Name, err)
			return err
		}
		t.mdb = ldb
		log.Printf("queue: topic load ldb: [%s] %s", t.Name, dbpath)
	} else if t.SDB != nil {
		t.mdb = t.SDB
		log.Printf("queue: topic load mdb: [%s]", t.Name)
	} else {
		t.mdb = db.NewMDB()
		log.Printf("queue: topic new mdb: [%s]", t.Name)
	}

	lines := make(map[string]*line)
	for name, b := range t.LineStore {
		l := new(line)
		err := json.Unmarshal(b, l)
		if err != nil {
			continue
		}
		l.recovery()
		l.parent = t
		lines[name] = l
	}
	t.lines = lines
	log.Printf("queue: topic recovery succ. [%s] %v", t.Name, lines)
	return nil
}

func (t *topic) getHead() uint64 {
	var head uint64
	if len(t.lines) == 0 {
		head = t.Head
	} else {
		head = t.Tail
		for _, l := range t.lines {
			if l.FlightHead < head {
				head = l.FlightHead
			}
		}
	}
	return head
}

func (t *topic) clean() {
	begin := t.Head
	defer func() {
		if begin != t.Head {
			log.Printf("queue: topic %s cleaned: %d - %d", t.Name, begin, t.Head)
		}
	}()

	head := t.getHead()
	for t.Head < head {
		key := fmt.Sprintf("%s/%d", t.Name, t.Head)
		err := t.mdb.Del(key)
		if err != nil {
			log.Printf("queue: topic db delete %s error: %v", key, err)
			return
		}
		t.Head++
	}
}
