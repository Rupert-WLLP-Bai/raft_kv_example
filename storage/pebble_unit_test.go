package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPebbleStore(t *testing.T) {
	// 创建一个临时测试目录
	testDir := "test_pebble_store"
	defer os.RemoveAll(testDir)

	// 创建 Pebble 存储
	store, err := NewPebbleStore(testDir)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer store.Close()

	// 测试基本的 Put/Get 操作
	key := []byte("test-key")
	value := []byte("test-value")

	// 存储键值对
	err = store.Put(key, value)
	assert.NoError(t, err)

	// 读取键值对
	retrievedValue, err := store.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// 测试不存在的键
	nonExistentKey := []byte("non-existent-key")
	nonExistentValue, err := store.Get(nonExistentKey)
	assert.NoError(t, err)
	assert.Nil(t, nonExistentValue)

	// 测试删除操作
	err = store.Delete(key)
	assert.NoError(t, err)

	// 确认键已被删除
	deletedValue, err := store.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, deletedValue)

	// 测试批处理操作
	batch := store.NewBatch()
	assert.NotNil(t, batch)

	for i := 0; i < 10; i++ {
		batchKey := []byte(fmt.Sprintf("batch-key-%d", i))
		batchValue := []byte(fmt.Sprintf("batch-value-%d", i))
		err = batch.Set(batchKey, batchValue, nil)
		assert.NoError(t, err)
	}

	err = store.ApplyBatch(batch)
	assert.NoError(t, err)

	// 验证批处理写入
	for i := 0; i < 10; i++ {
		batchKey := []byte(fmt.Sprintf("batch-key-%d", i))
		expectedValue := []byte(fmt.Sprintf("batch-value-%d", i))

		actualValue, err := store.Get(batchKey)
		assert.NoError(t, err)
		assert.Equal(t, expectedValue, actualValue)
	}
}
