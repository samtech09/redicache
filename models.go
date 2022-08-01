package redicache

import (
	"time"

	"github.com/go-redis/redis/v8"
)

//RedisConfig is only used cache LabST items
type RedisConfig struct {
	//RedisServerHost is name of machine to connect for Redis Cache
	Host string
	//RedisServerPort is port of machine to connect for Redis Cache
	Port               int
	DB                 int // from 0 to 16
	Pwd                string
	KeyPrefix          string
	ExpirationInMinute int
	//Debug if set to true, will print debug messages at console.
	Debug bool
}

// RedisSession defines connection to specific redis server an exposes methods to save/retrive data.
type RedisSession struct {
	CacheExpiration time.Duration
	client          *redis.Client
	config          RedisConfig
	candidates      map[string]string
	debug           bool
}
