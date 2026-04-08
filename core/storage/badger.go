package storage

import (
	"encoding/binary"
	baseerr "errors"

	"github.com/Skliar-Il/broker-message/core/apperrors"
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
)

var ErrStopScan = baseerr.New("stop scan")

type Badger struct {
	db *badger.DB
}

func NewBadger(db *badger.DB) *Badger {
	return &Badger{db: db}
}

func OpenBadger(dir string) (*Badger, error) {
	db, err := badger.Open(
		badger.DefaultOptions(dir).
			WithLogger(nil).
			WithSyncWrites(true).
			WithNumVersionsToKeep(1),
	)
	if err != nil {
		return nil, errors.Wrap(err, "open badger storage")
	}
	return &Badger{db: db}, nil
}

func (b *Badger) Close() error {
	return errors.Wrap(b.db.Close(), "close badger storage")
}

func (b *Badger) Set(key, value []byte) error {
	return errors.Wrap(b.db.Update(func(txn *badger.Txn) error {
		return errors.Wrap(txn.Set(key, value), "set key")
	}), "set key badger storage")
}

func (b *Badger) Get(key []byte) ([]byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if baseerr.Is(err, badger.ErrKeyNotFound) {
				return apperrors.ErrKeyNotFound
			}
			return errors.Wrap(err, "get badger item storage")
		}
		value, err = item.ValueCopy(nil)
		if err != nil {
			return errors.Wrap(err, "copy value from badger item")
		}
		return nil
	})
	if err != nil {
		if baseerr.Is(err, apperrors.ErrKeyNotFound) {
			return nil, err
		}
		return nil, errors.Wrap(err, "get badger storage")
	}
	return value, nil
}

func (b *Badger) Delete(key []byte) error {
	return errors.Wrap(b.db.Update(func(txn *badger.Txn) error {
		return errors.Wrap(txn.Delete(key), "delete key")
	}), "delete badger storage")
}

func (b *Badger) Scan(prefix []byte, fn func(key, value []byte) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return errors.Wrap(err, "scan: value copy")
			}
			if err := fn(k, v); err != nil {
				if baseerr.Is(err, ErrStopScan) {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

func (b *Badger) ScanFrom(startKey, prefix []byte, fn func(key, value []byte) error) error {
	return b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return errors.Wrap(err, "scan from: value copy")
			}
			if err := fn(k, v); err != nil {
				if baseerr.Is(err, ErrStopScan) {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

func (b *Badger) GetUint64(key []byte) (uint64, error) {
	v, err := b.Get(key)
	if err != nil {
		if baseerr.Is(err, apperrors.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(v) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}

func (b *Badger) SetUint64(key []byte, val uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, val)
	return b.Set(key, buf)
}
