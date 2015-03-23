package db

import (
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LDB struct {
	path    string
	db      *leveldb.DB
	timeout time.Duration
}

func NewLDB(path string) (*LDB, error) {
	option := &opt.Options{Compression: opt.SnappyCompression}
	db, err := leveldb.OpenFile(path, option)
	if err != nil {
		return nil, err
	}
	ls := new(LDB)
	ls.path = path
	ls.db = db

	return ls, nil
}

func (l *LDB) Set(key string, data string) error {
	return l.db.Put([]byte(key), []byte(data), nil)

	// err := l.db.Put(keyByte, data, nil)
	// if err != nil {
	// 	return err
	// }

	// return nil
}

func (l *LDB) Get(key string) (string, error) {
	keybyte := []byte(key)

	data, err := l.db.Get(keybyte, nil)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func (l *LDB) Del(key string) error {
	return l.db.Delete([]byte(key), nil)

	// err := l.db.Delete(keyByte, nil)
	// if err != nil {
	// 	return err
	// }
	// return nil
}

func (l *LDB) Type() int {
	return TypeLDB
}

func (l *LDB) Close() {
	err := l.db.Close()
	if err != nil {
		log.Printf("ldb close error: %s", err)
	}
}
