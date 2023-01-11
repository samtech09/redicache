package redicache

import (
	"fmt"
	"log"
	"testing"
	"time"
)

type testCache struct {
	ID   int
	Name string
}

var ses *RedisSession

func (x testCache) GetKey(parentid1, parentid2 string) string {
	return "TEST:KEY:" + x.Name
}
func (x testCache) GetMasterKey() string {
	return "TEST:KEY:"
}
func (x testCache) GetExpiration() time.Duration {
	return time.Duration(5) * time.Minute
}

func TestInit(t *testing.T) {
	config := RedisConfig{}
	config.DB = 4
	config.Host = "127.0.0.1"
	config.KeyPrefix = "test:"
	config.Port = 6379
	config.Pwd = ""
	config.Debug = true

	ses = InitSession(config)
	ses.RegisterCandidate(testCache{}, "test item for cache")

}

func TestEncodeDecode(t *testing.T) {
	tst := &testCache{1, "AAA"}
	err := ses.Set(tst, "p1", "p2")
	if err != nil {
		t.Error(err)
		return
	}

	var tst2 testCache
	err = ses.GetScan("p1", "p2", &tst2, tst)
	if err != nil {
		fmt.Printf("Err: %s\n", err.Error())
		t.Error(err)
	}
	fmt.Println("GetScan: ")
	log.Println(tst2)

	var tst3 testCache
	key := tst.GetKey("p1", "p2")
	err = ses.GetScanByKey(key, &tst3)
	if err != nil {
		t.Error(err)
	}
	fmt.Println("GetScanByKey: ")
	log.Println(tst3)
}
