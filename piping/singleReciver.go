package piping

import (
	"l2met/store"
)

type SingleReciver struct {
	input chan *store.Bucket
}

func NewSingleReciver(input chan *store.Bucket) (s *SingleReciver) {
	s = &SingleReciver{
		input}
	return s
}

func (s *SingleReciver) Start() {
}

func (s *SingleReciver) Stop() {
}
