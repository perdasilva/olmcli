package store

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
)

type IdentifiableEntry interface {
	ID() string
}

type IterationFunction[E IdentifiableEntry] func(*E) error
type FilterFunction[E IdentifiableEntry] func(*E) (bool, error)

type BoltDBTable[E IdentifiableEntry] struct {
	tableName []byte
	database  *bolt.DB
}

func NewBoltDBTable[E IdentifiableEntry](database *bolt.DB, tableName string) (*BoltDBTable[E], error) {
	if database == nil {
		return nil, fmt.Errorf("database parameter is nil")
	}

	if tableName == "" {
		return nil, fmt.Errorf("tableName parameter is empty")
	}

	return &BoltDBTable[E]{
		tableName: []byte(tableName),
		database:  database,
	}, nil
}

func (b *BoltDBTable[E]) Get(key string) (*E, error) {
	var entry *E = nil
	err := b.database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.tableName)
		valueBytes := bucket.Get([]byte(key))
		if valueBytes == nil {
			return nil
		}
		var err error
		entry, err = b.decode(valueBytes)
		if err != nil {
			return err
		}
		return nil
	})
	return entry, err
}

func (b *BoltDBTable[E]) Has(key string) (bool, error) {
	entry, err := b.Get(key)
	return entry != nil, err
}

func (b *BoltDBTable[E]) Create() error {
	return b.database.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(b.tableName)
		return err
	})
}

func (b *BoltDBTable[E]) Delete() error {
	return b.database.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(b.tableName)
	})
}

func (b *BoltDBTable[E]) List() ([]E, error) {
	var entries []E
	err := b.database.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.tableName)
		return bucket.ForEach(func(key, value []byte) error {
			entry, err := b.decode(value)
			if err != nil {
				return err
			}
			entries = append(entries, *entry)
			return nil
		})
	})
	return entries, err
}

func (b *BoltDBTable[E]) Search(fn FilterFunction[E]) ([]E, error) {
	var entries []E
	err := b.Iterate(func(entry *E) error {
		keep, err := fn(entry)
		if err != nil {
			return err
		}
		if keep {
			entries = append(entries, *entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil

}

func (b *BoltDBTable[E]) Insert(entry *E) error {
	return b.database.Update(func(tx *bolt.Tx) error {
		return b.InsertInTransaction(tx, entry)
	})
}

func (b *BoltDBTable[E]) InsertInTransaction(tx *bolt.Tx, entry *E) error {
	if !(tx.Writable()) {
		return fmt.Errorf("transaction is not writable")
	}
	bucket := tx.Bucket(b.tableName)
	valueBytes, err := b.encode(entry)
	if err != nil {
		return err
	}
	return bucket.Put([]byte((*entry).ID()), valueBytes)
}

func (b *BoltDBTable[E]) DeleteEntryWithKey(key string) error {
	return b.database.Update(func(tx *bolt.Tx) error {
		return b.DeleteEntryWithKeyInTransaction(tx, key)
	})
}

func (b *BoltDBTable[E]) DeleteEntryWithKeyInTransaction(tx *bolt.Tx, key string) error {
	if !(tx.Writable()) {
		return fmt.Errorf("transaction is not writable")
	}
	bucket := tx.Bucket(b.tableName)
	return bucket.Delete([]byte(key))
}

func (b *BoltDBTable[E]) DeleteEntriesWithPrefix(prefix string) error {
	return b.database.Update(func(tx *bolt.Tx) error {
		return b.DeleteEntriesWithPrefixInTransaction(tx, prefix)
	})
}

func (b *BoltDBTable[E]) DeleteEntriesWithPrefixInTransaction(tx *bolt.Tx, prefix string) error {
	if !(tx.Writable()) {
		return fmt.Errorf("transaction is not writable")
	}

	bucket := tx.Bucket(b.tableName)
	cursor := bucket.Cursor()
	prefixBytes := []byte(prefix)
	for key, _ := cursor.Seek(prefixBytes); key != nil && bytes.HasPrefix(key, prefixBytes); key, _ = cursor.Next() {
		if err := bucket.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *BoltDBTable[E]) Iterate(fn IterationFunction[E]) error {
	return b.database.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.tableName)
		return b.ForEach(func(key, value []byte) error {
			entry := new(E)
			if err := json.Unmarshal(value, entry); err != nil {
				return err
			}
			return fn(entry)
		})
	})
}

func (b *BoltDBTable[E]) encode(entry *E) ([]byte, error) {
	return json.Marshal(entry)
}

func (b *BoltDBTable[E]) decode(data []byte) (*E, error) {
	entry := new(E)
	if err := json.Unmarshal(data, entry); err != nil {
		return nil, err
	}
	return entry, nil

}
