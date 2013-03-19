package piping

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"l2met/utils"
	"os"
	"time"
)

//Partition
type RedisPartitioner struct {
	mailbox          string
	numPartitions    uint64
	lockTTL          uint64
	currentPartition uint64
}

func NewRedisPartitioner(numPartitions uint64, lockTTL uint64, mailbox string) (p *RedisPartitioner) {
	p = &RedisPartitioner{
		mailbox:       mailbox,
		numPartitions: numPartitions,
		lockTTL:       lockTTL}
	return p
}

func (s *RedisPartitioner) GetCurrentPartition() uint64 {
	return s.currentPartition
}

func (s *RedisPartitioner) GetNumPartitions() uint64 {
	return s.numPartitions
}

func (s *RedisPartitioner) GetLockTTL() uint64 {
	return s.lockTTL
}

func (s *RedisPartitioner) GetLockString(mailbox string, partition uint64) string {
	lockString := fmt.Sprintf("lock.%s.%d", mailbox, partition)
	return lockString
}

func (s *RedisPartitioner) LockPartition() uint64 {
	partition, err := s.lockPartition()
	if err != nil {
		fmt.Printf("Unable to lock partition.")
		os.Exit(1)
	}
	s.currentPartition = partition
	return partition
}

func (s *RedisPartitioner) lockPartition() (uint64, error) {
	for {
		for p := uint64(0); p < s.numPartitions; p++ {
			lockString := s.GetLockString(s.mailbox, p)
			locked, err := s.tryLock(lockString, s.lockTTL)
			utils.MeasureI("redis.partitioner.locked.id", int64(p))
			if err != nil {
				return 0, err
			}
			if locked {
				return p, nil
			}
		}
		time.Sleep(time.Second * 5)
	}
	return 0, errors.New("LockPartition impossible broke the loop.")
}

func (s *RedisPartitioner) tryLock(lockString string, ttl uint64) (bool, error) {
	rc := redisPool.Get()
	defer rc.Close()

	new := time.Now().Unix() + int64(ttl) + 1
	old, err := redis.Int(rc.Do("GETSET", lockString, new))
	// If the ErrNil is present, the old value is set to 0.
	if err != nil && err != redis.ErrNil && old == 0 {
		return false, err
	}
	// If the new value is greater than the old
	// value, then the old lock is expired.
	return new > int64(old), nil
}
