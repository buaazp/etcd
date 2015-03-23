package db

import (
	"errors"
	"log"
	"sync"
)

type MDB struct {
	mu sync.RWMutex
	DB map[string]string
}

func NewMDB() *MDB {
	db := make(map[string]string)
	ms := new(MDB)
	ms.DB = db

	return ms
}

func (m *MDB) Get(key string) (string, error) {
	data, ok := m.DB[key]
	if !ok {
		return "", errors.New(ErrNotExisted)
	}
	return data, nil
}

func (m *MDB) Set(key string, data string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DB[key] = data
	return nil
}

func (m *MDB) Del(key string) error {
	_, ok := m.DB[key]
	if !ok {
		return errors.New(ErrNotExisted)
	}

	delete(m.DB, key)
	log.Printf("key[%s] deleted.", key)
	return nil
}

func (m *MDB) Type() int {
	return TypeMDB
}

func (m *MDB) Close() {
	m.DB = nil
}
