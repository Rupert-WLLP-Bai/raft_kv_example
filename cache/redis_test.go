package cache

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func TestRedisCache(t *testing.T) {
	// 使用 miniredis 模拟 Redis 服务器
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer s.Close()

	// 创建 Redis 缓存
	cache, err := NewRedisCache(RedisConfig{
		Addr:       s.Addr(),
		KeyPrefix:  "test",
		Expiration: 1 * time.Second,
	})
	assert.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	// 测试 Set 和 Get
	err = cache.Set(ctx, "key1", "value1")
	assert.NoError(t, err)

	val, found, err := cache.Get(ctx, "key1")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value1", val)

	// 测试键前缀
	exists := s.Exists("test:key1")
	assert.True(t, exists)

	// 测试不存在的键
	val, found, err = cache.Get(ctx, "nonexistent")
	assert.NoError(t, err)
	assert.False(t, found)
	assert.Equal(t, "", val)

	// 测试删除
	err = cache.Delete(ctx, "key1")
	assert.NoError(t, err)

	val, found, err = cache.Get(ctx, "key1")
	log.Printf("val: %s, found: %v, err: %v", val, found, err)
	assert.NoError(t, err)
	assert.False(t, found)

	// 测试过期 - 使用 miniredis 的 SetTTL 来确保过期功能正常工作
	err = cache.Set(ctx, "key2", "value2")
	assert.NoError(t, err)

	// 直接在 miniredis 中设置 TTL
	s.SetTTL("test:key2", 500*time.Millisecond)

	// 验证键存在
	val, found, err = cache.Get(ctx, "key2")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "value2", val)

	// 等待过期
	time.Sleep(600 * time.Millisecond)

	// 在 miniredis 中手动快进时间来触发过期
	s.FastForward(600 * time.Millisecond)

	// 现在键应该已经过期
	val, found, err = cache.Get(ctx, "key2")
	log.Printf("val: %s, found: %v, err: %v", val, found, err)
	assert.NoError(t, err)
	assert.False(t, found)
}
