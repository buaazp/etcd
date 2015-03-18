package store

import (
	"encoding/json"
	"log"
	"time"

	etcdErr "github.com/coreos/etcd/error"
)

type topic struct {
	Name      string
	Messages  []string
	LineStore map[string][]byte
	lines     map[string]*line
	parent    *queue
}

func newTopic(name string) *topic {
	t := new(topic)
	t.Name = name
	t.Messages = make([]string, 0)
	t.lines = make(map[string]*line)
	log.Printf("topic[%s] created.", name)
	return t
}

func (t *topic) addLine(name string, recycle time.Duration) error {
	if _, ok := t.lines[name]; ok {
		return etcdErr.NewError(etcdErr.EcodeNodeExist, name, t.parent.parent.CurrentIndex)
	}

	l := newLine(name, recycle)
	l.parent = t
	t.lines[name] = l
	return nil
}

func (t *topic) push(value string) error {
	t.Messages = append(t.Messages, value)
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
	log.Printf("line[%s] removed.", name)
	return nil
}

func (t *topic) delLines() {
	for name, l := range t.lines {
		l.destroy()
		delete(t.lines, name)
		log.Printf("line[%s] removed.", name)
	}
}

func (t *topic) destroy() {
	t.delLines()
	t.lines = nil
	t.parent = nil
	t.Messages = nil
}

func (t *topic) save() ([]byte, error) {
	lineStore := make(map[string][]byte)
	for name, l := range t.lines {
		b, err := l.save()
		if err != nil {
			return nil, err
		}
		lineStore[name] = b
	}
	t.LineStore = lineStore
	return json.Marshal(t)
}

func (t *topic) recovery() {
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
}
