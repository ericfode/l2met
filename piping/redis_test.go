package piping

import (
	"testing"
	"time"
)

func TestSingleRedis(t *testing.T) {
	testSource := NewRandomSource(50)
	numPartitions := uint64(1)
	lockTTL := uint64(30)
	fetchInterval := uint64(5)
	redisOutlet := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisSource := NewRedisSource(fetchInterval, numPartitions, lockTTL, "testBox")
	testOutlet := NewRandomOutlet(50, testSource.testList, redisSource.sender.NewOutputChannel("verifier", 50))
	testSource.Start()
	time.Sleep(20)
	redisOutlet.Start()
	redisSource.Start()
	testOutlet.Start()

	passes := 0
	fails := 0
	for p := range testOutlet.GetSuccessChan() {
		if p {
			passes++
			println("GoodBucket")
			println(passes)
		} else {
			fails++
			println("BadBucket")
		}
		if fails+passes > 49 {
			t.Logf("goodBuckets: %v", passes)
			t.Logf("badBuckets: %v", fails)

			if fails > 0 {
				t.FailNow()
			}
			//		println("testoutlet stop")
			///			testOutlet.Stop()
			//			println("redisoutlet stop")
			//		redisOutlet.Stop()
			println("redissource stop")
			redisSource.Stop()
			println("its all stopped...")

			return
		}
	}
}

func TestMultiRedis(t *testing.T) {
	testSource := NewRandomSource(100)
	numPartitions := uint64(2)
	lockTTL := uint64(30)
	fetchInterval := uint64(1)
	redisOutlet2 := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisOutlet := NewRedisOutlet(testSource.sender.GetOutput(), numPartitions, lockTTL, "testBox")
	redisSource := NewRedisSource(fetchInterval, numPartitions, lockTTL, "testBox")
	testOutlet := NewRandomOutlet(100, testSource.testList, redisSource.sender.GetOutput())
	testSource.Start()
	time.Sleep(20)
	redisOutlet2.Start()
	redisOutlet.Start()
	redisSource.Start()
	testOutlet.Start()
	passes := 0
	fails := 0
	for p := range testOutlet.GetSuccessChan() {
		if p {
			passes++
			println("GoodBucket")
			println(passes)
		} else {
			fails++
			println("BadBucket")
		}
		if fails+passes == 100 {
			t.Logf("goodBuckets: %v", passes)
			t.Logf("badBuckets: %v", fails)

			if fails > 0 {
				t.FailNow()
			}
			//		println("testoutlet stop")
			///			testOutlet.Stop()
			//			println("redisoutlet stop")
			//		redisOutlet.Stop()
			//		println("redissource stop")
			//			redisSource.Stop()
			println("its all stopped...")

			return
		}
	}
}
