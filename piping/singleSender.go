package piping

import (
	"l2met/store"
)

type SingleSender struct {
	output        chan *store.Bucket
	senderChannel chan *store.Bucket
	*CopyTask
}

func NewSingleSender() (s *SingleSender) {
	s = &SingleSender{
		output:        make(chan *store.Bucket, 1000),
		senderChannel: make(chan *store.Bucket, 1000)}
	s.CopyTask = NewCopyTask(s)
	return s
}

func (s *SingleSender) NewOutputChannel(name string, size uint64) chan *store.Bucket {
	s.output = make(chan *store.Bucket, size)
	return s.output
}

func (s *SingleSender) DeleteOutputChannel(name string) {
	s.output = nil
}

//Hackish
func (s *SingleSender) GetOutputChannels() map[string]chan *store.Bucket {
	chanMap := map[string]chan *store.Bucket{"Primary": s.output}
	return chanMap
}

func (s *SingleSender) GetOutput() chan *store.Bucket {
	return s.output
}
func (s *SingleSender) GetSenderChannel() chan *store.Bucket {
	return s.senderChannel
}
