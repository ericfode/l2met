package piping

import (
	"l2met/store"
	"testing"
)

func TestSingleSender(t *testing.T) {
	s := NewSingleSender()
	schan := s.GetSenderChannel()
	ichan := s.NewOutputChannel("test", 5)
	schan <- &store.Bucket{
		Key: store.BKey{Name: "test"}}

	go s.Start()
	select {
	case <-ichan:
		return
	}
}
