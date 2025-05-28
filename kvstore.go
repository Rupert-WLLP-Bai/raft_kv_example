// Copyright 2015 The etcd Authors
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

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rupert-wllp-bai/kvstore/cache"
	"github.com/rupert-wllp-bai/kvstore/storage"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]string    // current committed key-value pairs
	pebbleStore *storage.PebbleStore // 持久化存储
	snapshotter *snap.Snapshotter
	redisCache  *cache.RedisCache // Redis 缓存
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error, nodeID int, config *Config) *kvstore {
	// 初始化 Pebble 存储
	// 使用传入的 nodeID 参数
	pebblePath := fmt.Sprintf("data/pebble/pebble-%d", nodeID)

	// 确保目录存在
	if err := os.MkdirAll("data/pebble", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	pstore, err := storage.NewPebbleStore(pebblePath)
	if err != nil {
		log.Fatalf("Failed to create pebble store: %v", err)
	}

	// 根据配置初始化 Redis 缓存
	var redisCache *cache.RedisCache
	if config.Redis.Enabled {
		redisCache, err = cache.NewRedisCache(cache.RedisConfig{
			Addr:         config.Redis.Addr,
			Password:     config.Redis.Password,
			DB:           config.Redis.DB,
			Expiration:   time.Duration(config.Redis.Expiration),
			KeyPrefix:    config.Redis.KeyPrefix,
			PoolSize:     config.Redis.PoolSize,
			MinIdleConns: config.Redis.MinIdleConn,
		})

		if err != nil {
			log.Printf("Warning: Failed to connect to Redis: %v. Continuing without cache.", err)
			redisCache = nil
		} else {
			log.Printf("Connected to Redis cache at %s", config.Redis.Addr)
		}
	}

	s := &kvstore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string), // 内存缓存
		pebbleStore: pstore,
		snapshotter: snapshotter,
		redisCache:  redisCache,
	}

	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

// Lookup 函数添加 Pebble 查询
func (s *kvstore) Lookup(key string) (string, bool) {
	// 如果 Redis 缓存可用，优先查询缓存
	if s.redisCache != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		if val, found, err := s.redisCache.Get(ctx, key); err == nil && found {
			return val, true
		}
		// 缓存未命中或发生错误，继续查询本地
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	// 查询内存缓存
	if v, ok := s.kvStore[key]; ok {
		// 如果 Redis 可用，将结果放入缓存
		if s.redisCache != nil {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer cancel()

				// 异步更新缓存，不阻塞主流程
				if err := s.redisCache.Set(ctx, key, v); err != nil {
					log.Printf("Warning: Failed to update Redis cache: %v", err)
				}
			}()
		}
		return v, true
	}

	// 内存中不存在，查询 Pebble
	if s.pebbleStore != nil {
		if val, err := s.pebbleStore.Get([]byte(key)); err == nil && val != nil {
			v := string(val)
			// 更新内存缓存
			s.kvStore[key] = v // 已有读锁，安全

			// 如果 Redis 可用，将结果放入缓存
			if s.redisCache != nil {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()

					// 异步更新缓存，不阻塞主流程
					if err := s.redisCache.Set(ctx, key, v); err != nil {
						log.Printf("Warning: Failed to update Redis cache: %v", err)
					}
				}()
			}
			return v, true
		}
	}

	return "", false
}

func (s *kvstore) Propose(k string, v string) {
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}

			s.mu.Lock()
			// 更新内存
			s.kvStore[dataKv.Key] = dataKv.Val
			// 同步到 Pebble
			if s.pebbleStore != nil {
				if err := s.pebbleStore.Put([]byte(dataKv.Key), []byte(dataKv.Val)); err != nil {
					log.Printf("Warning: failed to write to Pebble: %v", err)
				}
			}
			s.mu.Unlock()

			// 更新 Redis 缓存
			if s.redisCache != nil {
				go func(key, val string) {
					ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					defer cancel()

					if err := s.redisCache.Set(ctx, key, val); err != nil {
						log.Printf("Warning: Failed to update Redis cache: %v", err)
					}
				}(dataKv.Key, dataKv.Val)
			}
		}

		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if errors.Is(err, snap.ErrNoSnapshot) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// 添加功能: 恢复内存缓存
	s.kvStore = store

	// 同步到 Pebble
	if s.pebbleStore != nil {
		for k, v := range store {
			if err := s.pebbleStore.Put([]byte(k), []byte(v)); err != nil {
				log.Printf("Warning: failed to restore key %s to Pebble: %v", k, err)
			}
		}
	}

	return nil
}

// 关闭资源
func (s *kvstore) Close() error {
	var err error

	// 关闭 Pebble
	if s.pebbleStore != nil {
		if e := s.pebbleStore.Close(); e != nil {
			err = e
		}
	}

	// 关闭 Redis
	if s.redisCache != nil {
		if e := s.redisCache.Close(); e != nil && err == nil {
			err = e
		}
	}

	return err
}
