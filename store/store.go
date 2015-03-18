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
	"fmt"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/Godeps/_workspace/src/github.com/jonboulle/clockwork"
	etcdErr "github.com/coreos/etcd/error"
	"github.com/coreos/etcd/pkg/types"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

type Store interface {
	Version() int
	Index() uint64

	Get(nodePath string, recursive, sorted bool) (*Event, error)
	Set(nodePath string, dir bool, value string, expireTime time.Time) (*Event, error)
	Update(nodePath string, newValue string, expireTime time.Time) (*Event, error)
	Create(nodePath string, dir bool, value string, unique bool,
		expireTime time.Time) (*Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireTime time.Time) (*Event, error)
	Delete(nodePath string, dir, recursive bool) (*Event, error)
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error)

	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error)

	Save() ([]byte, error)
	Recovery(state []byte) error

	Clone() Store
	SaveNoCopy() ([]byte, error)

	JsonStats() []byte
	DeleteExpiredKeys(cutoff time.Time)

	Add(name string, value string) (*Event, error)
	Push(name string, value string) (*Event, error)
	Pop(name string, now time.Time) (*Event, error)
	Confirm(name string) (*Event, error)
	Remove(name string) (*Event, error)
}

type store struct {
	Root           *node
	WatcherHub     *watcherHub
	CurrentIndex   uint64
	Stats          *Stats
	CurrentVersion int
	QueueStore     []byte
	ttlKeyHeap     *ttlKeyHeap  // need to recovery manually
	worldLock      sync.RWMutex // stop the world lock
	clock          clockwork.Clock
	readonlySet    types.Set
	queue          *queue
}

// The given namespaces will be created as initial directories in the returned store.
func New(namespaces ...string) Store {
	s := newStore(namespaces...)
	s.clock = clockwork.NewRealClock()
	return s
}

func newStore(namespaces ...string) *store {
	s := new(store)
	s.CurrentVersion = defaultVersion
	s.Root = newDir(s, "/", s.CurrentIndex, nil, Permanent)
	for _, namespace := range namespaces {
		s.Root.Add(newDir(s, namespace, s.CurrentIndex, s.Root, Permanent))
	}
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)
	s.ttlKeyHeap = newTtlKeyHeap()
	s.readonlySet = types.NewUnsafeSet(append(namespaces, "/")...)
	s.queue = s.newQueue()
	return s
}

// Version retrieves current version of the store.
func (s *store) Version() int {
	return s.CurrentVersion
}

// Retrieves current of the store
func (s *store) Index() uint64 {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get returns a get event.
// If recursive is true, it will return all the content under the node path.
// If sorted is true, it will sort the content by keys.
func (s *store) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)

	if err != nil {
		s.Stats.Inc(GetFail)
		if recursive {
			reportReadFailure(GetRecursive)
		} else {
			reportReadFailure(Get)
		}
		return nil, err
	}

	e := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.Node.loadInternalNode(n, recursive, sorted, s.clock)

	s.Stats.Inc(GetSuccess)
	if recursive {
		reportReadSuccess(GetRecursive)
	} else {
		reportReadSuccess(Get)
	}

	return e, nil
}

// Create creates the node at nodePath. Create will help to create intermediate directories with no ttl.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
func (s *store) Create(nodePath string, dir bool, value string, unique bool, expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireTime, Create)

	if err == nil {
		e.EtcdIndex = s.CurrentIndex
		s.WatcherHub.notify(e)
		s.Stats.Inc(CreateSuccess)
		reportWriteSuccess(Create)
	} else {
		s.Stats.Inc(CreateFail)
		reportWriteFailure(Create)
	}

	return e, err
}

// Set creates or replace the node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireTime time.Time) (*Event, error) {
	var err error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(SetSuccess)
			reportWriteSuccess(Set)
		} else {
			s.Stats.Inc(SetFail)
			reportWriteFailure(Set)
		}
	}()

	// Get prevNode value
	n, getErr := s.internalGet(nodePath)
	if getErr != nil && getErr.ErrorCode != etcdErr.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	// Set new value
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireTime, Set)
	if err != nil {
		return nil, err
	}
	e.EtcdIndex = s.CurrentIndex

	// Put prevNode into event
	if getErr == nil {
		prev := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		prev.Node.loadInternalNode(n, false, false, s.clock)
		e.PrevNode = prev.Node
	}

	s.WatcherHub.notify(e)

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}

func (s *store) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
	value string, expireTime time.Time) (*Event, error) {

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	n, err := s.internalGet(nodePath)

	if err != nil {
		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndSwap)
		return nil, err
	}

	if n.IsDir() { // can only compare and swap file
		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndSwap)
		return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndSwap)
		return nil, etcdErr.NewError(etcdErr.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	// if test succeed, write the value
	n.Write(value, s.CurrentIndex)
	n.UpdateTTL(expireTime)

	// copy the value for safety
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	s.WatcherHub.notify(e)
	s.Stats.Inc(CompareAndSwapSuccess)
	reportWriteSuccess(CompareAndSwap)

	return e, nil
}

// Delete deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// recursive implies dir
	if recursive == true {
		dir = true
	}

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(DeleteFail)
		reportWriteFailure(Delete)
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	e := newEvent(Delete, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if n.IsDir() {
		eNode.Dir = true
	}

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(dir, recursive, callback)

	if err != nil {
		s.Stats.Inc(DeleteFail)
		reportWriteFailure(Delete)
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	s.WatcherHub.notify(e)

	s.Stats.Inc(DeleteSuccess)
	reportWriteSuccess(Delete)

	return e, nil
}

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(CompareAndDeleteFail)
		reportWriteFailure(CompareAndDelete)
		return nil, err
	}

	if n.IsDir() { // can only compare and delete file
		s.Stats.Inc(CompareAndSwapFail)
		reportWriteFailure(CompareAndDelete)
		return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		s.Stats.Inc(CompareAndDeleteFail)
		reportWriteFailure(CompareAndDelete)
		return nil, etcdErr.NewError(etcdErr.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.EtcdIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false, s.clock)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(false, false, callback)
	if err != nil {
		return nil, err
	}

	s.WatcherHub.notify(e)
	s.Stats.Inc(CompareAndDeleteSuccess)
	reportWriteSuccess(CompareAndDelete)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))
	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}
	// WatchHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk walks all the nodePath and apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *node, component string) (*node, *etcdErr.Error)) (*node, *etcdErr.Error) {
	components := strings.Split(nodePath, "/")

	curr := s.Root
	var err *etcdErr.Error

	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // ignore empty string
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}

	}

	return curr, nil
}

// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(UpdateFail)
		reportWriteFailure(Update)
		return nil, err
	}

	e := newEvent(Update, nodePath, nextIndex, n.CreatedIndex)
	e.EtcdIndex = nextIndex
	e.PrevNode = n.Repr(false, false, s.clock)
	eNode := e.Node

	if n.IsDir() && len(newValue) != 0 {
		// if the node is a directory, we cannot update value to non-empty
		s.Stats.Inc(UpdateFail)
		reportWriteFailure(Update)
		return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, currIndex)
	}

	n.Write(newValue, nextIndex)

	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	n.UpdateTTL(expireTime)

	eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)

	s.WatcherHub.notify(e)

	s.Stats.Inc(UpdateSuccess)
	reportWriteSuccess(Update)

	s.CurrentIndex = nextIndex

	return e, nil
}

func (s *store) internalCreate(nodePath string, dir bool, value string, unique, replace bool,
	expireTime time.Time, action string) (*Event, error) {

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique { // append unique item under the node path
		nodePath += "/" + fmt.Sprintf("%020s", strconv.FormatUint(nextIndex, 10))
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// we do not allow the user to change "/"
	if s.readonlySet.Contains(nodePath) {
		return nil, etcdErr.NewError(etcdErr.EcodeRootROnly, "/", currIndex)
	}

	// Assume expire times that are way in the past are
	// This can occur when the time is serialized to JS
	if expireTime.Before(minExpireTime) {
		expireTime = Permanent
	}

	dirName, nodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dirName, s.checkDir)

	if err != nil {
		s.Stats.Inc(SetFail)
		reportWriteFailure(action)
		err.Index = currIndex
		return nil, err
	}

	e := newEvent(action, nodePath, nextIndex, nextIndex)
	eNode := e.Node

	n, _ := d.GetChild(nodeName)

	// force will try to replace a existing file
	if n != nil {
		if replace {
			if n.IsDir() {
				return nil, etcdErr.NewError(etcdErr.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevNode = n.Repr(false, false, s.clock)

			n.Remove(false, false, nil)
		} else {
			return nil, etcdErr.NewError(etcdErr.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if !dir { // create file
		// copy the value for safety
		valueCopy := value
		eNode.Value = &valueCopy

		n = newKV(s, nodePath, value, nextIndex, d, expireTime)

	} else { // create directory
		eNode.Dir = true

		n = newDir(s, nodePath, nextIndex, d, expireTime)
	}

	// we are sure d is a directory and does not have the children with name n.Name
	d.Add(n)

	// node with TTL
	if !n.IsPermanent() {
		s.ttlKeyHeap.push(n)

		eNode.Expiration, eNode.TTL = n.expirationAndTTL(s.clock)
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// InternalGet gets the node of the given nodePath.
func (s *store) internalGet(nodePath string) (*node, *etcdErr.Error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	walkFunc := func(parent *node, name string) (*node, *etcdErr.Error) {

		if !parent.IsDir() {
			err := etcdErr.NewError(etcdErr.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, etcdErr.NewError(etcdErr.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}

	f, err := s.walk(nodePath, walkFunc)

	if err != nil {
		return nil, err
	}
	return f, nil
}

// deleteExpiredKyes will delete all
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	for {
		node := s.ttlKeyHeap.top()
		if node == nil || node.ExpireTime.After(cutoff) {
			break
		}

		s.CurrentIndex++
		e := newEvent(Expire, node.Path, s.CurrentIndex, node.CreatedIndex)
		e.EtcdIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false, s.clock)

		callback := func(path string) { // notify function
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		s.ttlKeyHeap.pop()
		node.Remove(true, true, callback)

		reportExpiredKey()
		s.Stats.Inc(ExpireCount)

		s.WatcherHub.notify(e)
	}

}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (s *store) checkDir(parent *node, dirName string) (*node, *etcdErr.Error) {
	node, ok := parent.Children[dirName]

	if ok {
		if node.IsDir() {
			return node, nil
		}

		return nil, etcdErr.NewError(etcdErr.EcodeNotDir, node.Path, s.CurrentIndex)
	}

	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, Permanent)

	parent.Children[dirName] = n

	return n, nil
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node. Or there will
// be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	b, err := json.Marshal(s.Clone())
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) SaveNoCopy() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Clone() Store {
	s.worldLock.Lock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	qs, err := s.queue.save()
	if err == nil {
		clonedStore.QueueStore = qs
	}

	s.worldLock.Unlock()
	return clonedStore
}

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring go routines.
func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	log.Printf("state: %d", len(state))
	err := json.Unmarshal(state, s)

	if err != nil {
		return err
	}

	s.ttlKeyHeap = newTtlKeyHeap()

	s.Root.recoverAndclean()

	q := new(queue)
	err = json.Unmarshal(s.QueueStore, q)
	if err != nil {
		return err
	}
	q.parent = s
	q.recovery()
	s.queue = q

	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) Add(name string, value string) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	name = path.Clean(path.Join("/", name))

	err := s.queue.add(name, value)
	if err != nil {
		s.Stats.Inc(CreateFail)
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(Create, name, s.CurrentIndex, s.CurrentIndex)
	e.EtcdIndex = s.CurrentIndex

	s.Stats.Inc(CreateSuccess)

	return e, nil
}

func (s *store) Push(name string, value string) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	var err error

	name = path.Clean(path.Join("/", name))

	err = s.queue.push(name, value)
	if err != nil {
		s.Stats.Inc(SetFail)
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++
	e := newEvent(Set, name, s.CurrentIndex, s.CurrentIndex)
	e.EtcdIndex = s.CurrentIndex

	s.Stats.Inc(SetSuccess)

	return e, nil
}

func (s *store) Pop(name string, now time.Time) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	name = path.Clean(path.Join("/", name))

	id, value, err := s.queue.pop(name, now)

	if err != nil {
		s.Stats.Inc(GetFail)
		return nil, err
	}

	if value == "" {
		return nil, etcdErr.NewError(etcdErr.EcodeKeyNotFound, name, s.CurrentIndex)
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(Get, name, s.CurrentIndex, s.CurrentIndex)
	e.EtcdIndex = s.CurrentIndex
	e.Node.Dir = false
	e.Node.Key = name + "/" + strconv.FormatUint(id, 10)
	e.Node.Value = &value
	e.Node.CreatedIndex = s.CurrentIndex
	e.Node.ModifiedIndex = s.CurrentIndex

	s.Stats.Inc(GetSuccess)

	return e, nil
}

func (s *store) Confirm(name string) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	name = path.Clean(path.Join("/", name))

	err := s.queue.confirm(name)

	if err != nil {
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(Delete, name, s.CurrentIndex, s.CurrentIndex)
	e.EtcdIndex = s.CurrentIndex

	s.Stats.Inc(DeleteSuccess)

	return e, nil
}

func (s *store) Remove(name string) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	name = path.Clean(path.Join("/", name))

	err := s.queue.remove(name)

	if err != nil {
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	// update etcd index
	s.CurrentIndex++

	e := newEvent(Delete, name, s.CurrentIndex, s.CurrentIndex)
	e.EtcdIndex = s.CurrentIndex

	s.Stats.Inc(DeleteSuccess)

	return e, nil
}
