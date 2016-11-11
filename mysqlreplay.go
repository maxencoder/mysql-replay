package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/siddontang/go-mysql/client"
)

const configFile = "mysql-replay.conf.json"

type ReplayStatement struct {
	session int
	epoch   float64
	stmt    string
	cmd     uint8
}

type Configuration struct {
	Addr     string
	User     string
	Password string
	DbName   string
}

type Stats struct {
	sync.Mutex
	wallclocks []time.Duration
}

func (s *Stats) append(d time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.wallclocks = append(s.wallclocks, d)
}

func (s *Stats) len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.wallclocks)
}

func (s *Stats) sort() {
	s.Lock()
	defer s.Unlock()
	sort.Sort(Durations(s.wallclocks))
}

func (s *Stats) percentile(n float64) time.Duration {
	s.Lock()
	defer s.Unlock()
	return s.wallclocks[int(float64(len(s.wallclocks))*n/100.0)]
}

func (s *Stats) mean() time.Duration {
	s.Lock()
	defer s.Unlock()
	var sum time.Duration
	for _, d := range s.wallclocks {
		sum += d
	}
	return sum / time.Duration(len(s.wallclocks))
}

func (s *Stats) stddev() float64 {
	mean := s.mean()
	s.Lock()
	defer s.Unlock()
	total := 0.0
	for _, d := range s.wallclocks {
		total += math.Pow(float64(d-mean), 2)
	}
	variance := total / float64(len(s.wallclocks)-1)
	return math.Sqrt(variance)
}

type Durations []time.Duration

func (a Durations) Len() int           { return len(a) }
func (a Durations) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Durations) Less(i, j int) bool { return a[i] < a[j] }

func timefromfloat(epoch float64) time.Time {
	epoch_base := math.Floor(epoch)
	epoch_frac := epoch - epoch_base
	epoch_time := time.Unix(int64(epoch_base), int64(epoch_frac*1000000000))
	return epoch_time
}

var stats = &Stats{}

func mysqlsession(c <-chan ReplayStatement, session int, firstepoch float64,
	starttime time.Time, conf Configuration) {

	log.Printf("[session %d] NEW SESSION\n", session)

	db, err := client.Connect(conf.Addr, conf.User, conf.Password, conf.DbName)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer db.Close()

	last_stmt_epoch := firstepoch
	for {
		pkt := <-c
		if last_stmt_epoch != 0.0 {
			firsttime := timefromfloat(firstepoch)
			pkttime := timefromfloat(pkt.epoch)
			delaytime_orig := pkttime.Sub(firsttime)
			mydelay := time.Since(starttime)
			delaytime_new := delaytime_orig - mydelay

			log.Printf("[session %d] Sleeping %s\n", session, delaytime_new)
			time.Sleep(delaytime_new)
		}
		last_stmt_epoch = pkt.epoch
		switch pkt.cmd {
		case 14: // Ping
			continue
		case 1: // Quit
			log.Printf("[session %d] COMMAND REPLAY: QUIT\n", session)
			db.Close()
			db = nil
		case 3: // Query
			if db == nil {
				log.Printf("[session %d] Tried to query on a closed session\n", session)
			}

			log.Printf("[session %d] STATEMENT REPLAY: %s\n", session, pkt.stmt)

			t0 := time.Now()
			_, err := db.Execute(pkt.stmt)
			stats.append(time.Since(t0))

			if err != nil {
				log.Println(err.Error())
			}
		}
	}
}

func main() {
	cf, _ := os.Open(configFile)
	dec := json.NewDecoder(cf)
	config := Configuration{}
	err := dec.Decode(&config)
	if err != nil {
		log.Fatalf("Error reading configuration from './%s': %s\n", configFile, err)
	}
	log.Printf("preplaying with config: %#v\n", config)

	filename := flag.String("f", "./test.dat", "Path to datafile for replay")
	flag.Parse()

	datFile, err := os.Open(*filename)
	if err != nil {
		log.Fatal(err.Error())
	}

	reader := csv.NewReader(datFile)
	reader.Comma = '\t'

	var firstepoch float64 = 0.0
	starttime := time.Now()
	sessions := make(map[int]chan ReplayStatement)
	for {
		stmt, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println(err.Error())
			continue
		}
		sessionid, err := strconv.Atoi(stmt[0])
		if err != nil {
			log.Println(err)
		}
		cmd_src, err := strconv.Atoi(stmt[2])
		if err != nil {
			log.Println(err)
		}
		cmd := uint8(cmd_src)
		epoch, err := strconv.ParseFloat(stmt[1], 64)
		if err != nil {
			log.Println(err)
		}
		pkt := ReplayStatement{session: sessionid, epoch: epoch, cmd: cmd, stmt: stmt[3]}
		if firstepoch == 0.0 {
			firstepoch = pkt.epoch
		}
		if sessions[pkt.session] != nil {
			sessions[pkt.session] <- pkt
			continue
		}

		sess := make(chan ReplayStatement)
		sessions[pkt.session] = sess
		go mysqlsession(sessions[pkt.session], pkt.session,
			firstepoch, starttime, config)
		sessions[pkt.session] <- pkt
	}

	stats.sort()
	fmt.Printf("collected %v stats\n", stats.len())
	fmt.Printf("stats: median: %v mean: %v stddev: %v\n",
		stats.percentile(50), stats.mean(), stats.stddev())
	fmt.Printf("percentiles: 90: %v 95: %v 99: %v 99.9: %v\n",
		stats.percentile(90), stats.percentile(95),
		stats.percentile(99), stats.percentile(99.9))
}
