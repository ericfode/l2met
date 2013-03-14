package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/bmizerany/pq"
	"github.com/garyburd/redigo/redis"
	"hash/crc64"
	"l2met/encoding"
	"l2met/utils"
	"log"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var pg *sql.DB

var (
	PartitionTable = crc64.MakeTable(crc64.ISO)
	//kept this line so that I only have to make minimal changes to bucket code
	metricsPat = regexp.MustCompile(`\/metrics\/(.*)\??`)
	redisPool  *redis.Pool
)

const keySep = "→"

func setupRedis(redisUrl string) {
	u, err := url.Parse(redisUrl)
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

func init() {
	setupRedis(os.Getenv("REDIS_URL"))
}

type Task interface {
	Start()
	Stop()
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

//An outlet to something out side of the application
//PG_outlet is an example
type Outlet interface {
	Task
	Receiver
	Flush() int
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

type RedisRefSource struct {
	sender        *SingleSender
	control       chan bool
	mailbox       string
	numPartitions uint64
	lockTTL       uint64
	fetchInterval int
}

func NewRedisRefSource(fetchInterval int, numPartitions uint64, lockTTL uint64, mailbox string) (r *RedisRefSource) {
	r = &RedisRefSource{
		sender:        NewSingleSender(),
		control:       make(chan bool),
		mailbox:       mailbox,
		numPartitions: numPartitions,
		lockTTL:       lockTTL,
		fetchInterval: fetchInterval}
	return r
}

func (s *RedisRefSource) Start() {
	go s.runScanBuckets()
	go s.sender.Start()
}

func (s *RedisRefSource) Stop() {
	s.control <- true
	s.sender.Stop()
}

func (s *RedisRefSource) runScanBuckets() {
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

func (s *RedisRefSource) fetch(t time.Time) {
	fmt.Printf("at=start_fetch minute=%d\n", t.Minute())
	//kludgey
	partition, err := utils.LockPartition(s.mailbox, s.numPartitions, s.lockTTL)
	if err != nil {
		fmt.Printf("Unable to lock partition.")
	}
	mailbox := fmt.Sprintf("%s.%d", s.mailbox, partition)
	s.scanBuckets(mailbox)
}

func (s *RedisRefSource) scanBuckets(mailbox string) {
	sc := s.sender.GetSenderChannel()
	rc := redisPool.Get()
	defer utils.MeasureT("redis.scan-buckets", time.Now())
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
		sc <- b
	}
}

type PostgresOutlet struct {
	reciver *SingleReciver
	control chan bool
	pgConn  *sql.DB
	metrics map[string]uint
}

func NewPostgresOutlet(input chan *Bucket, pgUrl string) (p *PostgresOutlet) {
	p = &PostgresOutlet{
		reciver: NewSingleReciver(input),
		control: make(chan bool)}
	p.initPg(pgUrl)
	p.initMetrics()
	return p
}

func (p *PostgresOutlet) initPg(pgUrl string) {
	pgUrlParsed, err := pq.ParseURL(pgUrl)
	if err != nil {
		log.Fatal("Unable to parse DATABASE_URL.")
	}
	pg, err2 := sql.Open("postgres", pgUrlParsed)
	if err2 != nil {
		log.Fatal("Unable to open connection to PostgreSQL.")
	}
	p.pgConn = pg
	pg.Exec("set application_name = 'l2met-next_postgres'")
}

func (p *PostgresOutlet) initMetrics() {
	p.metrics = make(map[string]uint)
	p.metrics["errors"] = 0
	p.metrics["commitAttemps"] = 0
	p.metrics["commits"] = 0
}

func (p *PostgresOutlet) RestartMetrics() {
	p.initMetrics()
}

func (p *PostgresOutlet) Start() {
	go p.runPutBuckets()
	go p.reciver.Start()
}

func (p *PostgresOutlet) Stop() {
	p.reciver.Stop()
	p.control <- true
}

func (p *PostgresOutlet) Flush() {
	p.control <- true
	for len(p.reciver.input) > 0 {
		err := p.writeToPostgres(<-p.reciver.input)
		if err != nil {
			fmt.Printf("error=%q\n", err)
			p.metrics["errors"]++
		} else {
			p.metrics["commits"]++
		}
	}
	go p.runPutBuckets()
}

func (p *PostgresOutlet) GetMetrics() map[string]uint {
	return p.metrics
}

func (p *PostgresOutlet) runPutBuckets() {
	for {
		p.metrics["commitAttempts"]++
		select {
		case <-p.control:
			return
		default:
			err := p.writeToPostgres(<-p.reciver.input)
			if err != nil {
				fmt.Printf("error=%q\n", err)
				p.metrics["errors"]++
			} else {
				p.metrics["commits"]++
			}
		}
	}
}

func (p *PostgresOutlet) writeToPostgres(bucket *Bucket) error {
	tx, err := p.pgConn.Begin()
	print("created transaction\n")
	if err != nil {
		return err
	}

	err = bucket.Get()
	print("gitted bucket\n")
	if err != nil {
		return err
	}
	print(bucket.String())
	print("\n")
	vals := string(encoding.EncodeArray(bucket.Vals, '{', '}', ','))
	row := tx.QueryRow(`
		SELECT id
		FROM buckets
		WHERE token = $1 AND measure = $2 AND source = $3 AND time = $4`,
		bucket.Key.Token, bucket.Key.Name, bucket.Key.Source, bucket.Key.Time)
	print("did queryRow\n")
	var id sql.NullInt64
	row.Scan(&id)

	if id.Valid {
		_, err = tx.Exec("UPDATE buckets SET vals = $1::FLOAT8[] WHERE id = $2",
			vals, id)
		if err != nil {
			tx.Rollback()
			return err
		}
	} else {
		_, err = tx.Exec(`
			INSERT INTO buckets(token, measure, source, time, vals)
			VALUES($1, $2, $3, $4, $5::FLOAT8[])`,
			bucket.Key.Token, bucket.Key.Name, bucket.Key.Source,
			bucket.Key.Time, vals)
		if err != nil {
			print("shit blew up on insert\n")
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
