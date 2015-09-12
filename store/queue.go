// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	etcdErr "github.com/coreos/etcd/error"
)

const (
	confirmedValue string = "confirmed"
)

type queue struct {
	TopicStore map[string][]byte
	topics     map[string]*topic
	parent     *store
}

func (s *store) newQueue() *queue {
	q := new(queue)
	q.topics = make(map[string]*topic)
	q.parent = s
	return q
}

func (q *queue) addTopic(name string) error {
	if _, ok := q.topics[name]; ok {
		return etcdErr.NewError(etcdErr.EcodeNodeExist, name, q.parent.CurrentIndex)
	}

	t := newTopic(name)
	t.parent = q
	q.topics[name] = t
	return nil
}

func (q *queue) add(name string, value string) error {
	parts := strings.Split(name, "/")

	switch len(parts) {
	case 2:
		return q.addTopic(parts[1])
	case 3:
		tname := parts[1]
		lname := parts[2]
		t, ok := q.topics[tname]
		if !ok {
			return etcdErr.NewError(etcdErr.EcodeKeyNotFound, tname, q.parent.CurrentIndex)
		}
		var recycle time.Duration
		if value != "" {
			var err error
			recycle, err = time.ParseDuration(value)
			if err != nil {
				return etcdErr.NewError(etcdErr.EcodeInvalidForm, value, q.parent.CurrentIndex)
			}
		}
		return t.addLine(lname, recycle)
	}
	return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
}

func (q *queue) push(name string, value string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 2 {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
	}
	tname := parts[1]
	t, ok := q.topics[tname]
	if !ok {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
	}
	return t.push(value)
}

func (q *queue) pop(name string, now time.Time) (uint64, string, error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 {
		return 0, "", etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
	}
	tname := parts[1]
	lname := parts[2]
	t, ok := q.topics[tname]
	if !ok {
		return 0, "", etcdErr.NewError(etcdErr.EcodeKeyNotFound, tname, q.parent.CurrentIndex)
	}
	return t.pop(lname, now)
}

func (q *queue) confirm(name string) error {
	parts := strings.Split(name, "/")
	if len(parts) != 4 {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
	}
	tname := parts[1]
	lname := parts[2]
	ids := parts[3]
	id, err := strconv.ParseUint(ids, 10, 64)
	if err != nil {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, ids, q.parent.CurrentIndex)
	}
	t, ok := q.topics[tname]
	if !ok {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, tname, q.parent.CurrentIndex)
	}
	return t.confirm(lname, id)
}

func (q *queue) delTopic(name string) error {
	t, ok := q.topics[name]
	if !ok {
		return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
	}

	t.destroy()
	delete(q.topics, name)
	log.Printf("queue: topic removed. [%s]", name)
	return nil
}

func (q *queue) remove(name string) error {
	parts := strings.Split(name, "/")

	switch len(parts) {
	case 2:
		return q.delTopic(parts[1])
	case 3:
		tname := parts[1]
		lname := parts[2]
		t, ok := q.topics[tname]
		if !ok {
			return etcdErr.NewError(etcdErr.EcodeKeyNotFound, tname, q.parent.CurrentIndex)
		}
		return t.delLine(lname)
	}
	return etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, q.parent.CurrentIndex)
}

func (q *queue) save() ([]byte, error) {
	topicStore := make(map[string][]byte)
	for name, t := range q.topics {
		b, err := t.save()
		if err != nil {
			return nil, err
		}
		topicStore[name] = b
		log.Printf("queue: topic save succ. [%s] %d", name, len(b))
	}
	q.TopicStore = topicStore
	return json.Marshal(q)
}

func (q *queue) recovery() {
	topics := make(map[string]*topic)
	for name, b := range q.TopicStore {
		t := new(topic)
		err := json.Unmarshal(b, t)
		if err != nil {
			continue
		}
		t.recovery()
		t.parent = q
		topics[name] = t
	}
	q.topics = topics
	log.Printf("queue: queue recovery succ. %v", topics)
}

func (q *queue) clean() {
	for _, t := range q.topics {
		t.clean()
	}
}
