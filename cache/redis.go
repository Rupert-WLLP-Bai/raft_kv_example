package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache 封装 Redis 缓存操作
type RedisCache struct {
	client     *redis.Client
	expiration time.Duration // 缓存过期时间
	keyPrefix  string        // 键前缀，用于隔离不同应用的缓存
}

// RedisConfig Redis 配置
type RedisConfig struct {
	Addr         string        // Redis 地址，如 "localhost:6379"
	Password     string        // Redis 密码，没有则为空
	DB           int           // Redis 数据库索引
	Expiration   time.Duration // 缓存默认过期时间
	KeyPrefix    string        // 键前缀
	PoolSize     int           // 连接池大小
	MinIdleConns int           // 最小空闲连接数
}

// NewRedisCache 创建一个新的 Redis 缓存
func NewRedisCache(config RedisConfig) (*RedisCache, error) {
	// 使用默认值
	if config.Expiration == 0 {
		config.Expiration = 10 * time.Minute
	}
	if config.PoolSize == 0 {
		config.PoolSize = 10
	}
	if config.MinIdleConns == 0 {
		config.MinIdleConns = 3
	}

	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisCache{
		client:     client,
		expiration: config.Expiration,
		keyPrefix:  config.KeyPrefix,
	}, nil
}

// prefixKey 为键添加前缀
func (rc *RedisCache) prefixKey(key string) string {
	if rc.keyPrefix == "" {
		return key
	}
	return rc.keyPrefix + ":" + key
}

// Get 从缓存中获取值
func (rc *RedisCache) Get(ctx context.Context, key string) (string, bool, error) {
	prefixedKey := rc.prefixKey(key)
	val, err := rc.client.Get(ctx, prefixedKey).Result()
	if err == redis.Nil {
		// 缓存未命中
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

// Set 将值设置到缓存中
func (rc *RedisCache) Set(ctx context.Context, key, value string) error {
	prefixedKey := rc.prefixKey(key)
	return rc.client.Set(ctx, prefixedKey, value, rc.expiration).Err()
}

// SetWithExpiration 将值设置到缓存中，并指定过期时间
func (rc *RedisCache) SetWithExpiration(ctx context.Context, key, value string, expiration time.Duration) error {
	prefixedKey := rc.prefixKey(key)
	return rc.client.Set(ctx, prefixedKey, value, expiration).Err()
}

// Delete 从缓存中删除键
func (rc *RedisCache) Delete(ctx context.Context, key string) error {
	prefixedKey := rc.prefixKey(key)
	return rc.client.Del(ctx, prefixedKey).Err()
}

// DeleteAll 删除所有匹配指定模式的键
func (rc *RedisCache) DeleteAll(ctx context.Context, pattern string) error {
	prefixedPattern := rc.prefixKey(pattern)
	iter := rc.client.Scan(ctx, 0, prefixedPattern, 0).Iterator()
	for iter.Next(ctx) {
		if err := rc.client.Del(ctx, iter.Val()).Err(); err != nil {
			return err
		}
	}
	return iter.Err()
}

// Close 关闭 Redis 连接
func (rc *RedisCache) Close() error {
	return rc.client.Close()
}
