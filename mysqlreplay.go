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
	"strconv"
	"time"

	"github.com/maxencoder/mysql-replay/stats"
	"github.com/siddontang/go-mysql/client"
)

const configFile = "mysql-replay.conf.json"

var st = &stats.Stats{}

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

func timefromfloat(epoch float64) time.Time {
	epoch_base := math.Floor(epoch)
	epoch_frac := epoch - epoch_base
	epoch_time := time.Unix(int64(epoch_base), int64(epoch_frac*1000000000))
	return epoch_time
}

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
			st.Append(time.Since(t0))

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
			log.Println("error reading csv:", err.Error())
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

	st.Sort()
	fmt.Printf("collected %v stats\n", st.Len())
	fmt.Printf("stats: median: %v mean: %v stddev: %v\n",
		st.Percentile(50), st.Mean(), st.Stddev())
	fmt.Printf("percentiles: 90: %v 95: %v 99: %v 99.9: %v\n",
		st.Percentile(90), st.Percentile(95),
		st.Percentile(99), st.Percentile(99.9))
}
