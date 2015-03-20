package store

import (
	"errors"
	"log"
	"sync"
)

const (
	ErrNotExisted string = "Data Not Existed"
)

type memdb struct {
	mu sync.RWMutex
	DB map[string]string
}

func NewMemdb() *memdb {
	db := make(map[string]string)
	ms := new(memdb)
	ms.DB = db

	return ms
}

func (m *memdb) Get(key string) (string, error) {
	data, ok := m.DB[key]
	if !ok {
		return "", errors.New(ErrNotExisted)
	}
	return data, nil
}

func (m *memdb) Set(key string, data string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DB[key] = data
	return nil
}

func (m *memdb) Del(key string) error {
	_, ok := m.DB[key]
	if !ok {
		return errors.New(ErrNotExisted)
	}

	delete(m.DB, key)
	log.Printf("key[%s] deleted.", key)
	return nil
}

func (m *memdb) Close() {
	m.DB = nil
}
