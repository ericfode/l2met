package piping

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"l2met/store"
	"l2met/utils"
	"strconv"
	"strings"
	"time"
)

//This does not belong here, but i am not sure where it should be
const keySep = "→"

type RedisSource struct {
	sender        *SingleSender
	control       chan bool
	mailbox       string
	fetchInterval uint64
	Eager         bool
	partitioner   *RedisPartitioner
}

func NewRedisSource(fetchInterval uint64, numPartitions uint64, lockTTL uint64, mailbox string) (r *RedisSource) {
	r = &RedisSource{
		sender:        NewSingleSender(),
		control:       make(chan bool),
		Eager:         false,
		fetchInterval: fetchInterval,
		partitioner:   NewRedisPartitioner(numPartitions, lockTTL, mailbox)}
	return r
}

func (s *RedisSource) Start() {
	go s.runScanBuckets()
	go s.sender.Start()
}

func (s *RedisSource) Stop() {
	s.control <- true
	s.sender.Stop()
}

func (s *RedisSource) runScanBuckets() {
	for t := range time.Tick(time.Second) {
		select {
		case <-s.control:
			utils.MeasureI("redis.source.stop.count", 1)
			return
		case <-time.Tick(time.Second * time.Duration(s.fetchInterval)):
			utils.MeasureI("redis.source.tick.fetch.count", 1)
			s.fetch(t)
		}
	}
}

func (s *RedisSource) NewOutputChannel(name string, bufferSize int) chan *store.Bucket {
	return s.sender.NewOutputChannel(name, uint64(bufferSize))
}

func (s *RedisSource) fetch(t time.Time) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	mailbox := fmt.Sprintf("%s.%d", s.mailbox, s.partitioner.LockPartition())
	s.scanBuckets(mailbox)
}

func (s *RedisSource) scanBuckets(mailbox string) {
	sc := s.sender.GetSenderChannel()
	defer utils.MeasureT("redis.scan-buckets.time", time.Now())

	rc := redisPool.Get()
	defer rc.Close()

	rc.Send("MULTI")
	rc.Send("SMEMBERS", mailbox)
	rc.Send("DEL", mailbox)
	reply, err := redis.Values(rc.Do("EXEC"))

	if err != nil {
		fmt.Printf("at=%q error%s\n", "redset-smembers", err)
		return
	}

	var delCount int64
	var members []string
	redis.Scan(reply, &members, &delCount)
	for _, member := range members {
		k, err := ParseKey(member)
		if err != nil {
			fmt.Printf("at=parse-key error=%s\n", err)
			continue
		}
		b := &store.Bucket{Key: *k}
		if s.Eager {
			t := time.Now()
			b.Get()
			utils.MeasureT("redis.source.get.bucket.time", t)
		}
		sc <- b
	}
}

func ParseKey(s string) (*store.BKey, error) {
	parts := strings.Split(s, keySep)
	if len(parts) < 3 {
		return nil, errors.New("bucket: Unable to parse bucket key.")
	}

	t, err := strconv.ParseInt(parts[0], 10, 54)
	if err != nil {
		return nil, err
	}

	time := time.Unix(t, 0)
	if err != nil {
		return nil, err
	}

	key := new(store.BKey)
	key.Time = time
	key.Token = parts[1]
	key.Name = parts[2]
	if len(parts) > 3 {
		key.Source = parts[3]
	}
	return key, nil
}
