package piping

import (
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

func getTestDatabase() string {
	return "postgres://sbpfwxwgnaxlma:N2HV0bx6i4AM6lb_MWMjw2qxm_@ec2-107-21-250-130.compute-1.amazonaws.com:5432/da9lpndc48d6r5"
}

func TestPgSendBucket(t *testing.T) {
	testSource := NewRandomSource(4)
	pg_url := getTestDatabase()
	pgChan := testSource.sender.NewOutputChannel("pg", 50)
	pgOutlet := NewPostgresOutlet(pgChan, 10, 10, pg_url)
	err := pgOutlet.WriteBucketToPostgres(GenerateBucket())

	if err != nil {
		t.Errorf("postgres returned error %v", err)
		t.Fail()
	}
}

func TestPgSendBatch(t *testing.T) {
	testSource := NewRandomSource(0)
	pg_url := getTestDatabase()
	pgChan := testSource.sender.NewOutputChannel("pg", 50)
	pgOutlet := NewPostgresOutlet(pgChan, 10, 10, pg_url)
	bslice := testSource.Slice(4)
	count := pgOutlet.WriteBatchToPostgres(bslice, 4)

	if count != 4 {
		t.Errorf("wrong number of items written to pg")
	}

}
func TestPgSendMulti(t *testing.T) {
	testSource := NewRandomSource(8)
	pg_url := getTestDatabase()
	pgChan := testSource.sender.NewOutputChannel("pgOut", 50)
	pgOutlet := NewPostgresOutlet(pgChan, 4, 4, pg_url)
	pgOutlet2 := NewPostgresOutlet(pgChan, 4, 4, pg_url)
	testSource.Start()
	pgOutlet.Start()
	pgOutlet2.Start()
	for pgOutlet.GetMetrics()["commits"]+pgOutlet2.GetMetrics()["commits"] < 8 {
		time.Sleep(100)
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
