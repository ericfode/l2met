package piping

import (
	"l2met/store"
)

type Task interface {
	Start()
	Stop()
}

type Partitioner interface {
	//Get a Lock on a partition 
	LockPartition() uint64
	GetMailbox() string
	GetNumPartitions() uint64
}

//A Sender is responsible for the aggregation of Buckets to
//all of the output channels that it has a handel to.
//The SenderChannel is the interface into the Sender
type Sender interface {
	NewOutputChannel(name string, size uint64) chan *store.Bucket
	DeleteOutputChannel(name string)
	GetOutputChannels() map[string]chan *store.Bucket
	//Channel that feeds into the sender
	GetSenderChannel() chan *store.Bucket
}

//A Receiver is responsible for holding on to  
type Receiver interface {
	SetInput(input chan *store.Bucket)
	//Channel that feeds out of the reciver into your application logic
	GetReciverChannel() chan *store.Bucket
}

//A component that both recives and sends.
//An example would be a partitioner (takes a stream of objects and devides them up for work)
type Aggregator interface {
	Task
	Receiver
	Sender
}

//An outlet to something out side of the application
//PG_outlet is an example
type Outlet interface {
	Task
	Receiver
}

//A source creates buckets and partitions them
//to all of the Outlets registered with it.
type Source interface {
	Task
	Sender
}
