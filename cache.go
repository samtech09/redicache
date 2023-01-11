package redicache

import (
	"context"
	"log"
	"strconv"

	"github.com/go-redis/redis/v8"

	"fmt"
	"time"
)

// InitSession initialize session with redis server with given config
func InitSession(config RedisConfig) *RedisSession {
	s := RedisSession{}
	s.debug = config.Debug
	s.config = config
	s.candidates = make(map[string]string)
	clnt, err := s.getClient()
	if err != nil {
		panic(err.Error())
	}
	s.client = clnt
	return &s
}

// getClient isused to get Redis client
func (s *RedisSession) getClient() (*redis.Client, error) {
	if s.client != nil {
		return s.client, nil
	}
	s.client = redis.NewClient(&redis.Options{
		Addr:        s.config.Host + ":" + strconv.Itoa(s.config.Port),
		Password:    s.config.Pwd, // no password set
		DB:          s.config.DB,  // use default DB
		MaxRetries:  3,
		IdleTimeout: 2 * time.Minute,
	})

	s.CacheExpiration = time.Minute * time.Duration(s.config.ExpirationInMinute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*2))
	defer cancel()

	_, err := s.client.Ping(ctx).Result()
	return s.client, err
}

// Raw allow to execute all commands on redis without any pre-check, use with responsibility
func (s *RedisSession) Raw() *redis.Client {
	return s.client
}

// CounterSet sets int with given key and expiration
func (s *RedisSession) CounterSet(value int, key string, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.Set(ctx, key, value, expiration).Err()
	if err != nil {
		s.logMsg("cache.CounterSet", "Error setting %s to cache: %v", key, err)
	}
	return err
}

// Counter sets raw int with given key and expiration
func (s *RedisSession) CounterGet(value int, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return 0, err
	}
	key = s.config.KeyPrefix + key
	str, err := c.Get(ctx, key).Result()
	if err != nil {
		s.logMsg("cache.CounterGet", "Error setting %s to cache: %v", key, err)
	}
	return strconv.ParseInt(str, 10, 64)
}

func (s *RedisSession) CounterINCR(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.Incr(ctx, key).Err()
	if err != nil {
		s.logMsg("cache.SetInt", "Error setting %s to cache: %v", key, err)
	}
	return err
}

func (s *RedisSession) CounterINCRBy(incrby int, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.IncrBy(ctx, key, int64(incrby)).Err()
	if err != nil {
		s.logMsg("cache.SetInt", "Error setting %s to cache: %v", key, err)
	}
	return err
}

func (s *RedisSession) CounterDECR(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.Incr(ctx, key).Err()
	if err != nil {
		s.logMsg("cache.SetInt", "Error setting %s to cache: %v", key, err)
	}
	return err
}

func (s *RedisSession) CounterDECRBy(decrby int, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.DecrBy(ctx, key, int64(decrby)).Err()
	if err != nil {
		s.logMsg("cache.SetInt", "Error setting %s to cache: %v", key, err)
	}
	return err
}

// SetStr sets raw string with given key and expiration
func (s *RedisSession) SetStr(value, key string, expiration time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return err
	}
	key = s.config.KeyPrefix + key
	err = c.Set(ctx, key, value, expiration).Err()
	if err != nil {
		s.logMsg("cache.SetStr", "Error setting %s to cache: %v", key, err)
	}
	return err
}

// Set either add new record or update existing with default expiration set in CacheCandidate
func (s *RedisSession) Set(value CacheCandidate, parentid1, parentid2 string) error {
	return s.SetWithExp(value, value.GetExpiration(), parentid1, parentid2)
}

// SetWithExp set struct to cache with custom expiration
func (s *RedisSession) SetWithExp(value CacheCandidate, expiration time.Duration, parentid1, parentid2 string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	if value == nil {
		return fmt.Errorf("nil value")
	}

	ok := s.isRegistered(value.GetMasterKey())
	if !ok {
		return fmt.Errorf("invalid or unregistered candidate")
	}
	buf, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c, err := s.getClient()
	if err != nil {
		return err
	}
	_key := s.config.KeyPrefix + value.GetKey(parentid1, parentid2)
	err = c.Set(ctx, _key, string(buf), expiration).Err()
	if err != nil {
		s.logMsg("cache.SetExp", "Error setting %s to cache: %v", _key, err)
	}
	//fmt.Printf("Set with key: %s\n", _key)
	return err
}

// SetSlice set []struct to cache with default expiration set in CacheCandidate
func (s *RedisSession) SetSlice(value interface{}, parentid1, parentid2 string, basetype CacheCandidate) error {
	if value == nil {
		return fmt.Errorf("nil value")
	}
	// cdall := value.([]CacheCandidate)
	// cd := cdall[0]
	// //cd := value[0]
	ok := s.isRegistered(basetype.GetMasterKey())
	if !ok {
		return fmt.Errorf("invalid or unregistered candidate")
	}
	key := s.config.KeyPrefix + basetype.GetKey(parentid1, parentid2)
	return s.setSliceWithExpiration(value, basetype.GetExpiration(), parentid1, parentid2, key)
}

// SetSliceWithExp set []struct to cache with given expiration
func (s *RedisSession) SetSliceWithExp(value interface{}, parentid1, parentid2 string, expiration time.Duration, basetype CacheCandidate) error {
	// cdall := value.([]CacheCandidate)
	// cd := cdall[0]
	// //cd := value[0]
	ok := s.isRegistered(basetype.GetMasterKey())
	if !ok {
		return fmt.Errorf("invalid or unregistered candidate")
	}
	key := s.config.KeyPrefix + basetype.GetKey(parentid1, parentid2)
	return s.setSliceWithExpiration(value, expiration, parentid1, parentid2, key)
}

// setSliceWithExpiration set []struct to cache with custom expiration
func (s *RedisSession) setSliceWithExpiration(value interface{}, expiration time.Duration, parentid1, parentid2, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	buf, err := json.Marshal(value)
	if err != nil {
		return err
	}
	c, err := s.getClient()
	if err != nil {
		return err
	}
	err = c.Set(ctx, key, string(buf), expiration).Err()
	if err != nil {
		s.logMsg("cache.setSliceWithExpiration", "Error setting '%s' to cache: %v", key, err)
	}
	return err
}

// Get return value of given key from cache
func (s *RedisSession) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return "", err
	}
	_key := s.config.KeyPrefix + key
	str, err := c.Get(ctx, _key).Result()
	if err != nil {
		s.logMsg("cache.Get", "Error getting '%s' from cache: %v", _key, err)
	}
	return str, err
}

// GetScanByKey retrive value from cache then deserialize to given dest type
func (s *RedisSession) GetScanByKey(key string, dest interface{}) error {
	value, err := s.Get(key)
	if err != nil {
		s.logMsg("cache.GetScan", "Error getting '%s' from cache: %v", key, err)
		return err
	}
	err = json.Unmarshal([]byte(value), dest)
	if err != nil {
		s.logMsg("cache.GetScan", "Error Unmarshaling '%s' to %t. %v", key, dest, err)
		return err
	}
	return nil
}

// GetScan retrive value from cache then deserialize to given dest type
func (s *RedisSession) GetScan(parentid1, parentid2 string, dest interface{}, basetype CacheCandidate) error {
	_key := basetype.GetKey(parentid1, parentid2)
	//fmt.Println(_key)
	return s.GetScanByKey(_key, dest)
}

// GetKeys find and return keys beginning with given pattern
func (s *RedisSession) GetKeys(pattern string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return nil, err
	}
	// only list keys with prefix for this app
	keys, err := c.Keys(ctx, s.config.KeyPrefix+pattern).Result()
	s.logMsg("cache.GetKeys", "'%s' result: %v", pattern, keys)
	if err != nil {
		return nil, err
	}
	return keys, nil
}

// DelKey delete single given key
func (s *RedisSession) DelKey(key string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return 0, err
	}
	cnt, err := c.Del(ctx, []string{s.config.KeyPrefix + key}...).Result()
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

// DelKeys - delete multiple keys as geven []string
func (s *RedisSession) DelKeys(keys ...string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*10))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return 0, err
	}
	cnt, err := c.Del(ctx, keys...).Result()
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

// DelByPattern - first fils keys begins with given pattern then delete them
func (s *RedisSession) DelByPattern(pattern string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*15))
	defer cancel()

	c, err := s.getClient()
	if err != nil {
		return 0, err
	}
	// only list keys with prefix for this app
	keys, err := c.Keys(ctx, s.config.KeyPrefix+pattern).Result()
	s.logMsg("cache.DelByPattern", "'%s' result: %v", pattern, keys)
	if err != nil {
		return 0, err
	}
	cnt, err := c.Del(ctx, keys...).Result()
	return cnt, err
}

func (s *RedisSession) logMsg(methodname, format string, msg ...interface{}) {
	if !s.debug {
		return
	}
	log.Printf("DEBUG: [%s] [%s]\n", methodname, fmt.Sprintf(format, msg...))
}
