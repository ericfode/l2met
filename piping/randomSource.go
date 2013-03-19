package piping

import (
	"fmt"
	"l2met/store"
	"math/rand"
	"os"
	"time"
)

var chars = "abcdefghijklmonpqrstuvwxyz"

func NewRandomString(length int) (r string) {
	if length < 1 {
		return
	}
	b := make([]byte, length)
	for i, _ := range b {
		b[i] = chars[rand.Intn(24)]
	}
	r = string(b)
	return r
}

func NewUUID() string {
	f, _ := os.Open("/dev/urandom")
	b := make([]byte, 16)
	f.Read(b)
	f.Close()
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

func NewRandomFloatSlice(length int) (r []float64) {
	if length < 1 {
		return
	}
	r = make([]float64, length)
	for idx, _ := range r {
		r[idx] = rand.Float64()
	}
	return r
}

type RandomSource struct {
	sender   *SingleSender
	testList map[string]*store.Bucket
	control  chan bool
	count    int
}

func GenerateBucket() (b *store.Bucket) {
	b = &store.Bucket{
		Key: store.BKey{
			Token:  NewUUID(),
			Name:   NewRandomString(5),
			Source: NewRandomString(10),
			Time:   time.Now()},
		Vals: NewRandomFloatSlice(50)}
	return b
}

func NewRandomSource(count int) (t *RandomSource) {
	t = &RandomSource{
		sender:   NewSingleSender(),
		testList: make(map[string]*store.Bucket, 100),
		count:    count}
	return t
}

func (t *RandomSource) Start() {
	go t.runGenerateBuckets()
	go t.sender.Start()
}

func (t *RandomSource) Stop() {
	t.control <- true
	t.sender.Stop()
}

func (t *RandomSource) Slice(count int) []*store.Bucket {
	buckets := make([]*store.Bucket, count)
	for i := 0; i < count; i = i + 1 {
		buckets[i] = GenerateBucket()
	}
	return buckets
}

func (t *RandomSource) runGenerateBuckets() {
	i := 0
	for {
		select {
		case <-t.control:
			return
		default:
			if i < t.count {
				b := GenerateBucket()
				t.PutBucket(b)
				i++
			} else {
				return
			}
		}
	}
}

func (t *RandomSource) PutBucket(b *store.Bucket) {
	t.testList[b.Key.Token] = b
	t.sender.GetSenderChannel() <- b
}
