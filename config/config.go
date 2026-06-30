package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Redis     RedisConfig
	Server    ServerConfig
	Worker    WorkerConfig
	Dashboard DashboardConfig
}

// RedisConfig holds Redis connection settings.
// If Addr is empty, the application falls back to the in-memory backend.
type RedisConfig struct {
	Addr     string // REDIS_ADDR — leave empty to use in-memory backend
	Password string // REDIS_PASSWORD
	DB       int    // REDIS_DB
}

func (r RedisConfig) Enabled() bool { return r.Addr != "" }

type ServerConfig struct {
	Addr string // HTTP_ADDR — default :8080
}

type DashboardConfig struct {
	APIURL string // API_URL — default http://localhost:8080
}

type WorkerConfig struct {
	PoolID     string        // POOL_ID — defaults to hostname-pid
	Workers    int           // WORKER_COUNT — default 5
	RetryBase  time.Duration // RETRY_BASE_SECONDS — default 1s
	RetryMax   time.Duration // RETRY_MAX_SECONDS — default 30s
}

// Load reads all configuration from environment variables.
func Load() Config {
	return Config{
		Redis: RedisConfig{
			Addr:     os.Getenv("REDIS_ADDR"),
			Password: os.Getenv("REDIS_PASSWORD"),
			DB:       getInt("REDIS_DB", 0),
		},
		Server: ServerConfig{
			Addr: get("HTTP_ADDR", ":8080"),
		},
		Dashboard: DashboardConfig{
			APIURL: get("API_URL", "http://localhost:8080"),
		},
		Worker: WorkerConfig{
			PoolID:    os.Getenv("POOL_ID"),
			Workers:   getInt("WORKER_COUNT", 5),
			RetryBase: time.Duration(getInt("RETRY_BASE_SECONDS", 1)) * time.Second,
			RetryMax:  time.Duration(getInt("RETRY_MAX_SECONDS", 30)) * time.Second,
		},
	}
}

func get(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
