package piping

import (
	"l2met/store"
	"os/exec"
	"testing"
	"time"
)

func getNewTestDatabase() string {
	app := "curl"
	arg0 := "http://api.postgression.com"
	cmd := exec.Command(app, arg0)
	out, err := cmd.Output()
	if err != nil {
		panic("Can't get DB")
	}
	return string(out)
}

func TestPgSendBucket(t *testing.T) {
	err := store.WriteBucketToPostgres(GenerateBucket())

	if err != nil {
		t.Errorf("postgres returned error %v", err)
		t.Fail()
	}
}

func TestPgSendBatch(t *testing.T) {
	bslice := NewBucketSlice(4)
	count := store.WriteSliceToPostgres(bslice, 4)

	if count != 4 {
		t.Errorf("wrong number of items written to pg")
	}

}

func TestPgSendMulti(t *testing.T) {
	testSource := NewRandomSource(8)
	pgChan := testSource.sender.NewOutputChannel("pgOut", 50)
	pgOutlet := NewPostgresOutlet(pgChan, 4, 4)
	pgOutlet2 := NewPostgresOutlet(pgChan, 4, 4)
	testSource.Start()
	pgOutlet.Start()
	pgOutlet2.Start()
	for pgOutlet.GetMetrics()["commits"]+pgOutlet2.GetMetrics()["commits"] < 8 {
		println(pgOutlet.GetMetrics()["commits"])
		println(pgOutlet2.GetMetrics()["commits"])
		time.Sleep(time.Second)
	}
	pgOutlet2.Stop()
	pgOutlet.Stop()
	t.Logf("pg1: %v \n pg2: %v", pgOutlet.GetMetrics(), pgOutlet2.GetMetrics())
	if pgOutlet.GetMetrics()["commits"]+pgOutlet2.GetMetrics()["commits"] < 8 {
		t.FailNow()
	}
	if len(pgChan) != 0 {
		t.FailNow()
	}
}
