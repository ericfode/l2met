package piping

import (
	"testing"
	"time"
)

func TestSingleRedis(t *testing.T) {
	testSource := NewRandomSource(50)
	numPartitions := uint64(1)
	lockTTL := uint64(30)
	fetchInterval := uint64(1)
	redisOutlet := NewRedisOutlet(testSource.sender.NewOutputChannel("redis", 50), numPartitions, lockTTL, "testBox")
	redisSource := NewRedisSource(fetchInterval, numPartitions, lockTTL, "testBox")
	testOutlet := NewRandomOutlet(50, testSource.testList, redisSource.sender.NewOutputChannel("verifier", 50))
	testSource.Start()
	time.Sleep(20)
	redisOutlet.Start()
	redisSource.Start()
	testOutlet.Start()

	pass := <-testOutlet.GetSuccessChan()
	if !pass {
		t.FailNow()
	}
}
