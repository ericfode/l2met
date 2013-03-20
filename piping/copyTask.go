package piping

import (
	"l2met/store"
)

type CopyTask struct {
	sender  Sender
	control chan bool
}

func NewCopyTask(sender Sender) (cp *CopyTask) {
	cp = &CopyTask{
		sender:  sender,
		control: make(chan bool)}
	return cp
}

func (cp *CopyTask) copy(b *store.Bucket) {
	for _, channel := range cp.sender.GetOutputChannels() {
		channel <- b
	}
}

func (cp *CopyTask) Start() {
	for {
		select {
		case <-cp.control:
			return
		case b := <-cp.sender.GetSenderChannel():
			cp.copy(b)
		}
	}
}

func (cp *CopyTask) Stop() {
	cp.control <- true
}
