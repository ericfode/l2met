package main

import (
	"math/rand"
	"testing"
	"time"
)

var chars = "abcdefghijklmonpqrstuvwxyz"

func TestPgSend(t *testing.T) {
	testSource := NewTestSource(10)
	pgOutlet := NewPostgresOutlet(testSource.sender.NewOutputChannel("pg"), "postgres://jfczhmrrguuaeo:ggqYuliOfDhhSdETHgcVfjt9WA@ec2-54-243-229-57.compute-1.amazonaws.com:5432/d5eq5tbr3ob6an")
  
	testSource.Start()
	pgOutlet.Start()
	pgOutlet.Flush()
	pgOutlet.Stop()

	if pgOutlet.GetMetrics()["errors"] < 10 {
		t.FailNow()
	}

}

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

type TestSource struct {
	sender   *SingleSender
	testList map[string]*Bucket
	control  chan bool
	count    int
}

func NewTestSource(count int) (t *TestSource) {
	t = &TestSource{
		sender:   NewSingleSender(),
		testList: make(map[string]*Bucket, 100),
		count:    count}
	return t
}

func (t *TestSource) Start() {
	go t.runGenerateBuckets()
	go t.sender.Start()
}

func (t *TestSource) Stop() {
	t.control <- true
	t.sender.Stop()
}

func (t *TestSource) runGenerateBuckets() {
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

func (t *TestSource) PutBucket(b *Bucket) {
	t.testList[b.Key.Token] = b
	t.sender.GetSenderChannel() <- b
}

type TestOutlet struct {
	receiver *SingleReciver
	testList map[string]*Bucket
	success  chan bool
	control  chan bool
	expected int
}

func NewTestOutlet(expected int, testList map[string]*Bucket, input chan *Bucket) (t *TestOutlet) {
	t = &TestOutlet{
		receiver: NewSingleReciver(input),
		testList: testList,
		success:  make(chan bool, 1),
		control:  make(chan bool),
		expected: expected}
	return t
}

func (t *TestOutlet) Start() {
	go t.receiver.Start()
	go t.runCheckBuckets()
}

func (t *TestOutlet) Stop() {
	t.control <- true
}

func (t *TestOutlet) runCheckBuckets() {
	found := 0
	for {
		select {
		case <-t.control:
			return
		case next, _ := <-t.receiver.input:
			//nice place for benchmark
			//check to see if the bucket is the same as the one just sent
			_, hok := t.testList[next.Key.Token]
			if !hok {
				print("   Got Bad bucket\n")
				t.success <- false
			}
			found++
			if found == t.expected {
				t.success <- true
			}
		}
	}
}

func (t *TestOutlet) GetSuccessChan() (suc chan bool) {
	return t.success
}

func GenerateBucket() (b *Bucket) {
	b = &Bucket{
		Key: BKey{
			Token:  NewRandomString(26),
			Name:   NewRandomString(5),
			Source: NewRandomString(10),
			Time:   time.Now()},
		Vals: NewRandomFloatSlice(50)}
	return b
}

func (b *Bucket) Compare(other *Bucket) bool {
	if CompareFloatSlice(b.Vals, other.Vals) &&
		b.Key.Name == other.Key.Name &&
		b.Key.Time == other.Key.Time &&
		b.Key.Source == other.Key.Source &&
		b.Key.Token == other.Key.Token {
		return true
	}
	return false
}

func CompareFloatSlice(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, c := range a {
		if c != b[i] {
			return false
		}
	}
	return true
}
