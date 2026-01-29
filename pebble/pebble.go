package pebble

import (
	"github.com/cockroachdb/pebble"
	"go.kvsh.ch/kvdb"
)

// Implements the kvdb.BinaryKeyValueStore interface
var _ kvdb.BinaryKeyValueStore = (*PebbleKeyValueStore)(nil)

type PebbleKeyValueStore struct {
	db *pebble.DB
}

func NewPebbleKeyValueStore(path string) (*PebbleKeyValueStore, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleKeyValueStore{db: db}, nil
}

func (p *PebbleKeyValueStore) Close() error {
	return p.db.Close()
}

func (p *PebbleKeyValueStore) Put(key []byte, value []byte) error {
	return p.db.Set(key, value, pebble.Sync)
}

func (p *PebbleKeyValueStore) Get(key []byte) ([]byte, error) {
	value, closer, err := p.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return value, nil
}

func (p *PebbleKeyValueStore) Has(key []byte) (bool, error) {
	_, closer, err := p.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	if closer != nil {
		closer.Close()
	}
	return true, nil
}

func (p *PebbleKeyValueStore) Delete(key []byte) error {
	return p.db.Delete(key, pebble.Sync)
}

func (p *PebbleKeyValueStore) Range(from, to []byte, fn func(key []byte, value []byte) error) error {
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: from,
		UpperBound: to,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		err := fn(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
	}
	return nil
}
