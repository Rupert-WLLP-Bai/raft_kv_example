package main

import (
	"encoding/json"
	"os"
	"time"
)

// Config 表示应用程序配置
type Config struct {
	// Redis 配置
	Redis struct {
		Enabled     bool     `json:"enabled"`
		Addr        string   `json:"addr"`
		Password    string   `json:"password"`
		DB          int      `json:"db"`
		Expiration  Duration `json:"expiration"`
		KeyPrefix   string   `json:"keyPrefix"`
		PoolSize    int      `json:"poolSize"`
		MinIdleConn int      `json:"minIdleConn"`
	} `json:"redis"`

	// 其他配置项可以根据需要添加
}

// Duration 是一个可以从 JSON 解析的 time.Duration
type Duration time.Duration

// UnmarshalJSON 实现从 JSON 解析 Duration
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value) * time.Second)
		return nil
	case string:
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(parsed)
		return nil
	default:
		return nil
	}
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	cfg := &Config{}

	// 设置默认 Redis 配置
	cfg.Redis.Enabled = true
	cfg.Redis.Addr = "localhost:6379"
	cfg.Redis.DB = 0
	cfg.Redis.Expiration = Duration(10 * time.Minute)
	cfg.Redis.PoolSize = 10
	cfg.Redis.MinIdleConn = 3

	return cfg
}

// LoadConfig 从文件加载配置
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// 配置文件不存在，使用默认配置
			return cfg, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
