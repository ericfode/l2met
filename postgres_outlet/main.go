package main

import (
	"l2met/piping"
	"l2met/store"
	"l2met/utils"
	"log"
	"runtime"
	"time"
)

var (
	partitionId     uint64
	numPartitions   uint64
	workers         int
	processInterval int
	lockTTL         uint64
	database_url    string
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	workers = utils.EnvInt("LOCAL_WORKERS", 2)
	processInterval = utils.EnvInt("POSTGRES_INTERVAL", 5)
	numPartitions = utils.EnvUint64("NUM_OUTLET_PARTITIONS", 1)
	lockTTL = utils.EnvUint64("LOCK_TTL", 10)
	database_url = utils.EnvString("DATABASE_URL", "")
}

func main() {
	var err error
	if err != nil {
		log.Fatal("Unable to lock partition.")
	}
	redisSource := piping.NewRedisSource(60, numPartitions, lockTTL, "postgres_outlet")
	redisSource.Start()
	outlets := make([]*piping.PostgresOutlet, workers)
	redisOutbox := redisSource.NewOutputChannel("postgres", 10000)
	for i := 0; i < workers; i++ {
		outlets[i] = piping.NewPostgresOutlet(redisOutbox, 1000, 60, database_url)
		outlets[i].Start()
	}

	// Print chanel metrics & live forever.
	report(redisOutbox)
}

func report(o chan *store.Bucket) {
	for _ = range time.Tick(time.Second * 5) {
		utils.MeasureI("postgres_outlet.outbox", int64(len(o)))
	}
}
