package piping

import (
	"fmt"
	"l2met/store"
	"l2met/utils"
)

type RandomOutlet struct {
	receiver *SingleReciver
	testList map[string]*store.Bucket
	success  chan bool
	control  chan bool
	expected int
}

func NewRandomOutlet(expected int, testList map[string]*store.Bucket, input chan *store.Bucket) (t *RandomOutlet) {
	t = &RandomOutlet{
		receiver: NewSingleReciver(input),
		testList: testList,
		success:  make(chan bool, 1),
		control:  make(chan bool),
		expected: expected}
	return t
}

func (t *RandomOutlet) Start() {
	go t.receiver.Start()
	go t.runCheckBuckets()
}

func (t *RandomOutlet) Stop() {
	t.control <- true
}

func (t *RandomOutlet) runCheckBuckets() {
	found := 0
	for {
		select {

		case <-t.control:
			return

		case next := <-t.receiver.input:
			utils.MeasureI("randomOutlet.input.channel.length", int64(len(t.receiver.input)))
			//nice place for benchmark
			//check to see if the bucket is the same as the one just sent
			found++
			other, hok := t.testList[next.Key.Token]

			if !hok {
				print("Bucket Not found in list\n")
				t.success <- false

			} else {

				if !Compare(next, other) {
					print("Buckets values wrong\n")
					t.success <- false
				}

				t.success <- true
				println(found)
			}

			if found == t.expected {
				print("got them all!\n")
			}

		}

	}
}

func Compare(b, other *store.Bucket) bool {
	b.Get()
	other.Get()
	if CompareFloatSlice(b.Vals, other.Vals) {
		fmt.Printf("values munged\n")
		fmt.Printf("expected:%v \n got:%v", b.Vals, other.Vals)
	}
	if b.Key.Name == other.Key.Name &&
		b.Key.Source == other.Key.Source &&
		b.Key.Token == other.Key.Token {
		return true
	}
	fmt.Printf("Bkey munged\n")
	fmt.Printf("expect:%s, got:%s", b.String(), other.String())
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

func (t *RandomOutlet) GetSuccessChan() (suc chan bool) {
	return t.success
}
