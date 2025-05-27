package storage

import (
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStore 包装 Pebble 数据库实现持久化存储
type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(path string) (*PebbleStore, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Configure Pebble options with better retry logic
	opts := &pebble.Options{
		// Add retry logic for locks
		MaxOpenFiles: 100,
		// Add appropriate cache size
		Cache: pebble.NewCache(32 << 20), // 32MB cache
	}

	// Try a few times with backoff
	var db *pebble.DB
	var err error
	for i := 0; i < 3; i++ {
		db, err = pebble.Open(path, opts)
		if err == nil {
			break
		}
		// Wait a bit before retrying
		time.Sleep(time.Duration(i+1) * 500 * time.Millisecond)
	}

	if err != nil {
		return nil, err
	}

	return &PebbleStore{
		db: db,
	}, nil
}

// Get 从存储中获取值
func (s *PebbleStore) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil // 不将 key 不存在视为错误
		}
		return nil, err
	}
	defer closer.Close()

	// 复制值，因为一旦 closer 关闭，原始值可能无效
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Put 将键值对写入存储
func (s *PebbleStore) Put(key, value []byte) error {
	return s.db.Set(key, value, pebble.Sync)
}

// Delete 从存储中删除键
func (s *PebbleStore) Delete(key []byte) error {
	return s.db.Delete(key, pebble.Sync)
}

// NewBatch 创建一个新的批处理
func (s *PebbleStore) NewBatch() *pebble.Batch {
	return s.db.NewBatch()
}

// ApplyBatch 应用批处理
func (s *PebbleStore) ApplyBatch(batch *pebble.Batch) error {
	return batch.Commit(pebble.Sync)
}

// Close 关闭存储
func (s *PebbleStore) Close() error {
	return s.db.Close()
}

// NewIterator 创建一个迭代器用于范围扫描
func (s *PebbleStore) NewIterator(lower, upper []byte) (*pebble.Iterator, error) {
	return s.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
}
