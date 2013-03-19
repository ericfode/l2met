package piping

import (
	"database/sql"
	"errors"
	"github.com/bmizerany/pq"
	"l2met/encoding"
	"l2met/store"
	"l2met/utils"
	"log"
	"time"
)

type PostgresOutlet struct {
	reciver   *SingleReciver
	control   chan bool
	pgConn    *sql.DB
	metrics   map[string]uint
	batch     []*store.Bucket
	delay     uint
	batchSize uint
	flush     chan bool
	batchPos  int
	ticker    chan bool
}

func NewPostgresOutlet(input chan *store.Bucket, batchSize uint, batchDelay uint, pgUrl string) (p *PostgresOutlet) {
	p = &PostgresOutlet{
		reciver:   NewSingleReciver(input),
		control:   make(chan bool),
		batch:     make([]*store.Bucket, batchSize),
		delay:     batchDelay,
		batchSize: batchSize,
		flush:     make(chan bool),
		batchPos:  0,
		ticker:    make(chan bool)}
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
	utils.MeasureI("postgres.start.count", 1)
	go p.runPutBuckets()
	go p.runBatcher()
	go p.reciver.Start()
}

func (p *PostgresOutlet) Stop() {
	p.reciver.Stop()
	p.control <- true
	utils.MeasureI("postgres.stop.count", 1)
}

func (p *PostgresOutlet) GetMetrics() map[string]uint {
	return p.metrics
}

func (p *PostgresOutlet) runBatcher() {
	for {
		select {
		case <-p.control:
			//persist signal
			p.control <- true
			return
		case <-time.Tick(time.Duration(p.delay) * time.Second):
			utils.MeasureI("postgres.batcher.time.tick.count", 1)
			p.flush <- true
		case <-p.tick():
			utils.MeasureI("postgres.batcher.filled.tick.count", 1)
			p.flush <- true
		}
	}
}

func (p *PostgresOutlet) tick() chan bool {
	return p.ticker
}

func (p *PostgresOutlet) Flush() {
	if (p.batchPos) == p.WriteBatchToPostgres(p.batch, p.batchPos) {
		p.batchPos = 0
	}
}

func (p *PostgresOutlet) AddToBatch(bucket *store.Bucket) {
	p.batch[p.batchPos] = bucket
	p.batchPos++
	if uint(p.batchPos) > p.batchSize {
		p.ticker <- true
	}
}
func (p *PostgresOutlet) runPutBuckets() {
	for {
		select {
		case <-p.control:
			p.control <- true
			utils.MeasureI("postgres.control.signal.count", 1)
			return
		case next := <-p.reciver.input:
			p.AddToBatch(next)
			utils.MeasureI("postgres.batch.size", int64(p.batchPos))
		case <-p.flush:
			utils.MeasureI("postgres.batch.flush.count", 1)
			p.Flush()
		}
	}
}

func (p *PostgresOutlet) WriteBatchToPostgres(batch []*store.Bucket, count int) int {
	defer utils.MeasureT("postgres.write.batch.time", time.Now())
	dropped := 0
	for pos := count - 1; pos >= 0; pos-- {
		bucket := batch[pos]
		err := p.WriteBucketToPostgres(bucket)
		if err != nil {
			utils.MeasureI("postgres.write.drop.count", 1)
			dropped++
		}
	}
	utils.MeasureI("postgres.write.attempted.count", int64(count))
	utils.MeasureI("postgres.write.dropped.count", int64(dropped))
	utils.MeasureI("postgres.write.success.count", int64(count-dropped))
	return count
}

func (p *PostgresOutlet) WriteBucketToPostgres(bucket *store.Bucket) error {
	defer utils.MeasureT("postgres.write.bucket.time", time.Now())
	if bucket == nil {
		utils.MeasureI("postgres.write.nilBucket.count", 1)
		return errors.New("got nil bucket")
	}
	tx, err := p.pgConn.Begin()
	if err != nil {
		utils.MeasureI("postgres.write.transaction.start.error.count", 1)
		return err
	}
	if bucket.Vals == nil {
		err = bucket.Get()
		if err != nil {
			utils.MeasureI("postgres.write.getBucket.error.count", 1)
			return err
		}
	}
	vals := string(encoding.EncodeArray(bucket.Vals, '{', '}', ','))

	row := tx.QueryRow(`
		SELECT id
		FROM buckets
		WHERE token = $1 AND measure = $2 AND source = $3 AND time = $4`,
		bucket.Key.Token, bucket.Key.Name, bucket.Key.Source, bucket.Key.Time)
	var id sql.NullInt64
	row.Scan(&id)

	if id.Valid {
		_, err = tx.Exec("UPDATE buckets SET vals = $1::FLOAT8[] WHERE id = $2",
			vals, id)
		if err != nil {
			tx.Rollback()
			utils.MeasureI("postgres.write.upsertBucket.error.count", 1)
			return err
		}
		utils.MeasureI("postgres.write.upsertBucket.success.count", 1)
	} else {
		_, err = tx.Exec(`
			INSERT INTO buckets(token, measure, source, time, vals)
			VALUES($1, $2, $3, $4, $5::FLOAT8[])`,
			bucket.Key.Token, bucket.Key.Name, bucket.Key.Source,
			bucket.Key.Time, vals)
		if err != nil {
			tx.Rollback()
			utils.MeasureI("postgres.write.newBucket.fail", 1)
			return err
		}
		utils.MeasureI("postgres.write.newBucket.success.count", 1)
	}

	err = tx.Commit()
	if err != nil {
		utils.MeasureI("postgres.write.transaction.close.error.count", 1)
		return err
	}
	p.metrics["commits"]++
	return nil
}
