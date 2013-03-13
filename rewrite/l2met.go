package rewrite

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"hash/crc64"
	"l2met/encoding"
	"l2met/utils"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	PartitionTable  = crc64.MakeTable(crc64.ISO)
	metricsPat      = regexp.MustCompile(`\/metrics\/(.*)\??`)
	workers         int
	port            string
	registerLocker  sync.Mutex
	numPartitions   uint64
	reqBuffer       int
	asd             int
	flushInterval   int
	redisPool       *redis.Pool
	processInterval int
)

const keySep = "→"

func init() {
	processInterval = 30
	runtime.GOMAXPROCS(runtime.NumCPU())
	port = utils.EnvString("PORT", "8000")
	workers = utils.EnvInt("LOCAL_WORKERS", 2)
	reqBuffer = utils.EnvInt("REQUEST_BUFFER", 1000)
	flushInterval = utils.EnvInt("FLUSH_INTERVAL", 1)
	numPartitions = utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	u, err := url.Parse(os.Getenv("REDIS_URL"))
	if err != nil {
		fmt.Printf("error=%q\n", "Missing REDIS_URL.")
		os.Exit(1)
	}
	server := u.Host
	password, set := u.User.Password()
	if !set {
		fmt.Printf("at=error error=%q\n", "password not set")
		os.Exit(1)
	}
	redisPool = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialTimeout("tcp", server, time.Second, time.Second, time.Second)
			if err != nil {
				return nil, err
			}
			c.Do("AUTH", password)
			return c, err
		},
	}
}

type Task interface {
	Start()
	Stop()
}

type Partitioner interface {
	//Lock a specific Partition.
	LockPartition(index int)
	//Get the next avilabe lock.
	GetPartition() (index int)
	//Set the number of partitions.
	SetPartitionCount(size int)

	SetPartitionSpace(name string)
}

//A Sender is responsible for the aggregation of Buckets to
//all of the output channels that it has a handel to.
//The SenderChannel is the interface into the Sender
type Sender interface {
	NewOutputChannel(name string) chan *Bucket
	DeleteOutputChannel(name string)
	GetOutputChannels() map[string]chan *Bucket
	//Channel that feeds into the sender
	GetSenderChannel() chan *Bucket
}

//A Receiver is responsible for holding on to  
type Receiver interface {
	SetInput(input chan *Bucket)
	//Channel that feeds out of the reciver into your application logic
	GetReciverChannel() chan *Bucket
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

type BKey struct {
	Token  string
	Name   string
	Source string
	Time   time.Time
}

// time:token:name:source
func ParseKey(s string) (*BKey, error) {
	parts := strings.Split(s, keySep)
	if len(parts) < 3 {
		return nil, errors.New("bucket: Unable to parse bucket key.")
	}

	t, err := strconv.ParseInt(parts[0], 10, 54)
	if err != nil {
		return nil, err
	}

	time := time.Unix(t, 0)
	if err != nil {
		return nil, err
	}

	key := new(BKey)
	key.Time = time
	key.Token = parts[1]
	key.Name = parts[2]
	if len(parts) > 3 {
		key.Source = parts[3]
	}
	return key, nil
}

type Bucket struct {
	sync.Mutex
	Key  BKey
	Vals []float64 `json:"vals,omitempty"`
}

// time:token:name:source
func (b *Bucket) String() (res string) {
	res += strconv.FormatInt(b.Key.Time.Unix(), 10) + keySep
	res += b.Key.Token + keySep
	res += b.Key.Name
	if len(b.Key.Source) > 0 {
		res += keySep + b.Key.Source
	}
	return
}

func (b *Bucket) Get() error {
	defer utils.MeasureT("bucket.get", time.Now())

	rc := redisPool.Get()
	defer rc.Close()

	//Fill in the vals.
	reply, err := redis.Values(rc.Do("LRANGE", b.String(), 0, -1))
	if err != nil {
		return err
	}
	for _, item := range reply {
		v, ok := item.([]byte)
		if !ok {
			continue
		}
		err = encoding.DecodeArray(v, &b.Vals, '[', ']', ' ')
	}
	return nil
}

func (b *Bucket) Partition(id []byte, partitions uint64) uint64 {
	check := crc64.Checksum(id, PartitionTable)
	return check % partitions
}

type LogRequest struct {
	Token string
	Body  []byte
}

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

func (cp *CopyTask) copy() {
	outputs := cp.sender.GetOutputChannels()
	sc := cp.sender.GetSenderChannel()
	first := <-sc
	for _, channel := range outputs {
		channel <- first
	}
}

func (cp *CopyTask) Start() {
	for {
		select {
		case <-cp.control:
			return
		default:
			cp.copy()
		}
	}
}

func (cp *CopyTask) Stop() {
	cp.control <- true
}

type SingleSender struct {
	output        chan *Bucket
	senderChannel chan *Bucket
	*CopyTask
}

func NewSingleSender() (s *SingleSender) {
	s = &SingleSender{
		output:        make(chan *Bucket, 1000),
		senderChannel: make(chan *Bucket, 1000)}
	s.CopyTask = NewCopyTask(s)
	return s
}

func (s *SingleSender) NewOutputChannel(name string) chan *Bucket {
	s.output = make(chan *Bucket, 1000)
	return s.output
}

func (s *SingleSender) DeleteOutputChannel(name string) {
	s.output = nil
}

func (s *SingleSender) GetOutputChannels() map[string]chan *Bucket {
	chanMap := map[string]chan *Bucket{"Primary": s.output}
	return chanMap
}

func (s *SingleSender) GetSenderChannel() chan *Bucket {
	return s.senderChannel
}

type SingleReciver struct {
	input chan *Bucket
}

func NewSingleReciver(input chan *Bucket) (s *SingleReciver) {
	s = &SingleReciver{
		input}
	return s
}

func (s *SingleReciver) Start() {
}

func (s *SingleReciver) Stop() {
}

type RedisSource struct {
	sender        *SingleSender
	control       chan bool
	mailbox       string
	partition     uint64
	fetchInterval int
}

func NewRedisSource(fetchInterval int, partition uint64, mailbox string) (r *RedisSource) {
	r = &RedisSource{
		sender:        NewSingleSender(),
		control:       make(chan bool),
		mailbox:       mailbox,
		partition:     partition,
		fetchInterval: fetchInterval}
	return r
}

func (s *RedisSource) Start() {
	go s.runScanBuckets()
	go s.sender.Start()
}

func (s *RedisSource) Stop() {
	s.control <- true
	s.sender.Stop()
}

func (s *RedisSource) runScanBuckets() {
	for t := range time.Tick(time.Second) {

		select {
		case <-s.control:
			return
		default:
			if t.Second()%s.fetchInterval == 0 {
				s.fetch(t)
			}
		}
	}
}

func (s *RedisSource) fetch(t time.Time) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	mailbox := fmt.Sprintf("%s.%d", s.mailbox, s.partition)
	s.scanBuckets(mailbox)
}

func (s *RedisSource) scanBuckets(mailbox string) {
	sc := s.sender.GetSenderChannel()
	defer utils.MeasureT("redis.scan-buckets", time.Now())

	rc := redisPool.Get()
	defer rc.Close()

	rc.Send("MULTI")
	rc.Send("SMEMBERS", mailbox)
	rc.Send("DEL", mailbox)
	reply, err := redis.Values(rc.Do("EXEC"))

	if err != nil {
		fmt.Printf("at=%q error%s\n", "redset-smembers", err)
		return
	}

	var delCount int64
	var members []string
	redis.Scan(reply, &members, &delCount)
	for _, member := range members {
		k, err := ParseKey(member)
		if err != nil {
			fmt.Printf("at=parse-key error=%s\n", err)
			continue
		}
		b := &Bucket{Key: *k}
		b.Get()
		sc <- b
	}
}

type RedisOutlet struct {
	reciver   *SingleReciver
	control   chan bool
	partition uint64
	mailbox   string
}

func NewRedisOutlet(input chan *Bucket, partition uint64, mailbox string) (r *RedisOutlet) {
	r = &RedisOutlet{
		reciver:   NewSingleReciver(input),
		control:   make(chan bool),
		partition: partition,
		mailbox:   mailbox}
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

func (r *RedisOutlet) PutBucket(b *Bucket) {
	defer utils.MeasureT("bucket.put", time.Now())

	b.Lock()
	vals := b.Vals
	key := b.String()
	partition := b.Partition([]byte(key), numPartitions)
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
