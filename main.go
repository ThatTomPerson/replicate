package main // import "ttp.sh/replicate"

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"

	_ "github.com/go-sql-driver/mysql"
)

const (
	MaxSize int64 = 50000000
)

type Table struct {
	Database  string
	TableName string
	Size      int64
	Rows      int64
	Columns   map[string]string
}

func main() {
	// repliacte root[:password]@remote[:port] root[:password]@local[:port] agedcareguide ndis craft
	remote := open(os.Args[1])
	local := open(os.Args[2])
	databases := os.Args[3:]

	workers := 20

	_ = remote
	_ = local
	_ = databases

	c := make(chan *Table, workers*2)

	wg := new(sync.WaitGroup)
	wg.Add(workers)
	p := mpb.New(mpb.WithWaitGroup(wg))
	logrus.Info("Starting Workers")
	for i := 0; i < workers; i++ {
		go worker(p, c, wg, remote, local)
	}

	for _, dbname := range databases {
		// logrus.Infof("Getting %s table list", dbname)
		tables := getTableList(remote, dbname)
		for _, table := range tables {
			c <- table
		}
	}

	close(c)
	p.Wait()

	remote.Close()
	local.Close()
}

func worker(p *mpb.Progress, c chan *Table, wg *sync.WaitGroup, remote, local *sql.DB) {
	var bar *mpb.Bar

	for t := range c {
		tname := fmt.Sprintf("%s.%s", t.Database, t.TableName)
		bar = newBar(p, bar, 1, tname, "Migrate")
		bar.Increment()

		if t.Size > MaxSize || t.Rows == 0 {
			continue
		}

		bar = newBar(p, bar, t.Rows, tname, "   Fill")
		for !bar.Completed() {
			start := time.Now()
			time.Sleep(200 * time.Millisecond)
			bar.IncrBy(2000, time.Since(start))
		}
	}

	wg.Done()
}

func newBar(p *mpb.Progress, bar *mpb.Bar, total int64, title, job string) *mpb.Bar {

	var replace mpb.BarOption
	if bar != nil {
		replace = mpb.BarReplaceOnComplete(bar)
	}

	return p.AddBar(total,
		replace,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(title, decor.WC{W: len(title) + 1, C: decor.DSyncSpaceR}),
			decor.Name(job, decor.WC{W: len(job) + 1, C: decor.DSyncSpaceR}),
			decor.OnComplete(
				// ETA decorator with ewma age of 60
				decor.EwmaETA(decor.ET_STYLE_GO, 60), "done",
			),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(decor.Percentage()),
	)
}

func open(arg string) *sql.DB {
	parts := strings.Split(arg, "@")
	dsn := fmt.Sprintf("%s@tcp(%s)/", parts[0], parts[1])
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		logrus.Fatalf("Could not connect to DB %s: %v", parts[1], err)
	}
	return db
}

func getTableList(db *sql.DB, dbname string) (tables []*Table) {
	rows, err := db.Query("select TABLE_NAME, DATA_LENGTH, TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA = ? and TABLE_TYPE = 'BASE TABLE'", dbname)
	if err != nil {
		logrus.Fatalf("couldn't query the information_schema: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var table string
		var size sql.NullInt64
		var r sql.NullInt64
		if err := rows.Scan(&table, &size, &r); err != nil {
			logrus.Errorf("couldn't scan row: %v", err)
		}

		if r.Int64 == 0 {
			// logrus.Infof("%s.%s reports 0 rows, Double checking", dbname, table)
			// just double check
			row := db.QueryRow(fmt.Sprintf("select count(*) c from %s.%s", dbname, table))
			if err := row.Scan(&r); err != nil {
				logrus.Errorf("couldn't scan row: %v", err)
			}
		}

		tables = append(tables, &Table{
			Database:  dbname,
			TableName: table,
			Size:      size.Int64,
			Rows:      r.Int64,
		})
	}

	if err := rows.Err(); err != nil {
		logrus.Errorf("can't scan any more: %v", err)
	}
	return tables
}

func clone(remote, local *sql.DB, dbname string) {
	tables := getTableList(remote, dbname)
	spew.Dump(tables)

	for table, size := range tables {
		_ = table
		_ = size
	}
}
