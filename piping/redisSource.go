package piping

import (
	"l2met/store"
	"l2met/utils"
	"time"
)

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
		mailbox:       mailbox,
		partitioner:   NewRedisPartitioner(numPartitions, lockTTL, mailbox)}
	return r
}

func (s *RedisSource) Start() {
	go s.runLoop()
	go s.sender.Start()
}

func (s *RedisSource) Stop() {
	s.control <- true
	s.sender.Stop()
}

func (s *RedisSource) runLoop() {
	for {
		select {
		case <-s.control:
			utils.MeasureI("redis.source.stop.count", 1)
			return
		case <-time.Tick(time.Second * time.Duration(s.fetchInterval)):
			utils.MeasureI("redis.source.tick.fetch.count", 1)
			s.getMail(s.mailbox)
		}
	}
}

func (s *RedisSource) GetOutput() chan *store.Bucket {
	return s.sender.GetOutput()
}

func (s *RedisSource) getMail(mailbox string) {
	sc := s.sender.GetSenderChannel()
	defer utils.MeasureT("redis.scan-buckets.time", time.Now())
	buckets, _ := store.EmptyMailboxPartition(mailbox, int(s.partitioner.LockPartition()))
	for _, b := range buckets {
		if s.Eager {
			b.Get()
		}
		sc <- b
	}
	utils.MeasureI("redis.source.sender.channel.len", int64(len(s.sender.GetOutput())))
}
