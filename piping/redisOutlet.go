package piping

import (
	"fmt"
	"l2met/store"
	"l2met/utils"
	"time"
)

type RedisOutlet struct {
	reciver     *SingleReciver
	control     chan bool
	partition   uint64
	mailbox     string
	partitioner *RedisPartitioner
}

func NewRedisOutlet(input chan *store.Bucket, numPartitions uint64, lockTTL uint64, mailbox string) (r *RedisOutlet) {
	r = &RedisOutlet{
		reciver:     NewSingleReciver(input),
		control:     make(chan bool),
		partitioner: NewRedisPartitioner(numPartitions, lockTTL, mailbox)}
	return r
}

func (r *RedisOutlet) Start() {
	go r.runPutBuckets()
	go r.reciver.Start()
}

func (r *RedisOutlet) Stop() {
	r.control <- true
	r.reciver.Stop()
}

func (r *RedisOutlet) runPutBuckets() {
	for {
		select {
		case <-r.control:
			return
		default:
			r.PutBucket(<-r.reciver.input)
		}
	}
}

func (r *RedisOutlet) PutBucket(b *store.Bucket) {
	defer utils.MeasureT("bucket.put", time.Now())

	b.Lock()
	vals := b.Vals
	key := b.String()
	//It might make since for this to be relegated to the RedisPartitioner class
	partition := b.Partition([]byte(key), r.partitioner.GetNumPartitions())
	b.Unlock()

	rc := redisPool.Get()
	defer rc.Close()
	mailBox := fmt.Sprintf("%s.%d", r.mailbox, partition)

	rc.Send("MULTI")
	rc.Send("RPUSH", key, vals)
	rc.Send("EXPIRE", key, 300)
	rc.Send("SADD", mailBox, key)
	rc.Send("EXPIRE", mailBox, 300)
	rc.Do("EXEC")

	//Some sort of reporting should be happening here
	//if err != nil {   
	//}
}
