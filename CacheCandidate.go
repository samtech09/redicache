package redicache

import (
	"time"

	jsoniter "github.com/json-iterator/go"
)

//CacheCandidate defines base to create structs that can be added to cache
type CacheCandidate interface {
	//GetKey will give actual formatted key to be used to cache data
	GetKey(parentid1, parentid2 string) string

	//GetMasterKey return only patten of key - for display purpose only
	GetMasterKey() string

	//GetExpiration will give Expiration duration
	GetExpiration() time.Duration
}

var candidates map[string]string
var json = jsoniter.ConfigCompatibleWithStandardLibrary

func init() {
	candidates = make(map[string]string)
}

//RegisterCandidate - register cachecandidate to list
func (s *RedisSession) RegisterCandidate(cd CacheCandidate, description string) {
	s.candidates[cd.GetMasterKey()] = description
}

//ListCandidates list all registered candidates
func (s *RedisSession) ListCandidates() map[string]string {
	return s.candidates
}

func (s *RedisSession) isRegistered(masterkey string) bool {
	_, ok := s.candidates[masterkey]
	return ok
}
