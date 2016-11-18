package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/maxencoder/mixer/proxy"
	"github.com/maxencoder/mixer/sqlparser"
	"github.com/maxencoder/mysql-replay/stats"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
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

func replayAndCmp(c <-chan ReplayStatement, session int, firstepoch float64,
	starttime time.Time, conf *Configuration, conf2 *Configuration) {

	var conn, conn2 *client.Conn
	var err error
	conn, err = client.Connect(conf.Addr, conf.User, conf.Password, conf.DbName)
	if err != nil {
		log.Fatal(err.Error())
	}
	conn.SetCharset("utf8mb4")
	conn2, err = client.Connect(conf2.Addr, conf2.User, conf2.Password, conf2.DbName)
	if err != nil {
		log.Fatal(err.Error())
	}
	conn2.SetCharset("utf8mb4")
	defer conn.Close()
	defer conn2.Close()

	last_stmt_epoch := firstepoch
	for {
		pkt := <-c
		if false && last_stmt_epoch != 0.0 {
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
			continue
			conn.Close()
			conn = nil
			conn2.Close()
			conn2 = nil
		case 3: // Query
			if conn == nil {
				log.Printf("[session %d] Tried to query on a closed session\n", session)
				continue
			}

			r2, err := conn2.Execute(pkt.stmt)
			if err != nil {
				log.Printf("[session %d] exec2 err: %s:\n%s", session, err.Error(), pkt.stmt)
				continue
			}

			stmt, err := sqlparser.Parse(pkt.stmt)
			if err != nil {
				log.Println("[session %d] cannot compare results: failed to parse query:\n", session, pkt.stmt)
				continue
			}

			s, ok := stmt.(*sqlparser.Select)
			if !ok {
				log.Printf("[session %d] statements other than Select are not supported:\n%s\n",
					session, pkt.stmt)
				continue
			}

			t0 := time.Now()
			r1, err := conn.Execute(pkt.stmt)
			st.Append(time.Since(t0))
			if err != nil {
				log.Printf("[session %d] exec1 err: %s:\n%s", session, err.Error(), pkt.stmt)
				continue
			}

			// skip known problems
			if strings.Contains(pkt.stmt, "db_heartbeat") || strings.Contains(pkt.stmt, "slave_master_info") {
				continue
			}
			if !resultsEqual(s, r1, r2) {
				log.Printf("[session %d] results not equal for stmt: \n%s\n", session, pkt.stmt)
			}
		}
	}
}

func resultsEqual(stmt *sqlparser.Select, r1, r2 *mysql.Result) bool {
	if r1 == nil && r2 == nil {
		return true
	} else if r1 == nil || r2 == nil {
		return false
	}

	if false && r1.Status != r2.Status {
		log.Println("result status not equal", r1.Status, r2.Status)
		return false
	}
	if r1.InsertId != r2.InsertId {
		log.Println("result InsertId not equal", r1.InsertId, r2.InsertId)
		return false
	}
	if r1.AffectedRows != r2.AffectedRows {
		log.Println("result AffectedRows not equal", r1.AffectedRows, r2.AffectedRows)
		return false
	}

	rs1 := r1.Resultset
	rs2 := r2.Resultset
	if rs1 == nil && rs2 == nil {
		return true
	} else if rs1 == nil || rs2 == nil {
		return false
	}

	if !reflect.DeepEqual(rs1.Fields, rs2.Fields) {
		log.Println("resultset.Fields not equal", rs1.Fields, rs2.Fields)
		cmp := func(fs1, fs2 []*mysql.Field) {
			if len(fs1) != len(fs2) {
				log.Printf("number of fields not equal", len(fs1), len(fs2))
				return
			}
			for i, f1 := range fs1 {
				f2 := fs2[i]
				switch {
				case false && !bytes.Equal(f1.Data, f2.Data):
					log.Printf("Field.Data not equal for %s, %s\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.Schema, f2.Schema):
					log.Printf("Field.Schema not equal for %v, %v\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.Table, f2.Table):
					log.Printf("Field.Table not equal for %v, %v\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.OrgTable, f2.OrgTable):
					log.Printf("Field.OrgTable not equal for %v, %v\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.Name, f2.Name):
					log.Printf("Field.Name not equal for %v, %v\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.OrgName, f2.OrgName):
					log.Printf("Field.OrgName not equal for %v, %v\n", f1.Name, f2.Name)
				case f1.Charset != f2.Charset:
					log.Printf("Field.Charset not equal for %s, %s\n", f1.Name, f2.Name)
					log.Printf("Field.Charset not equal for %v, %v\n", f1.Charset, f2.Charset)
				case f1.ColumnLength != f2.ColumnLength:
					log.Printf("Field.ColumnLength not equal for %v, %v\n", f1.Name, f2.Name)
				case f1.Type != f2.Type:
					log.Printf("Field.Type not equal for %v, %v\n", f1.Name, f2.Name)
				case f1.Flag != f2.Flag:
					log.Printf("Field.Flag not equal for %v, %v\n", f1.Name, f2.Name)
				case f1.Decimal != f2.Decimal:
					log.Printf("Field.Decimal not equal for %v, %v\n", f1.Name, f2.Name)
				case f1.DefaultValueLength != f2.DefaultValueLength:
					log.Printf("Field.DefaultValueLength not equal for %v, %v\n", f1.Name, f2.Name)
				case !bytes.Equal(f1.DefaultValue, f2.DefaultValue):
					log.Printf("Field.DefaultValue not equal for %v, %v\n", f1.Name, f2.Name)
				}
			}
		}
		cmp(rs1.Fields, rs2.Fields)
		return false
	}
	if !reflect.DeepEqual(rs1.FieldNames, rs2.FieldNames) {
		log.Println("resultset.FieldNames not equal", rs1.FieldNames, rs2.FieldNames)
		return false
	}

	// fully sort result sets unless already sorted
	if true || stmt.OrderBy == nil {
		sk := make([]proxy.SortKey, len(rs1.Fields))

		for i, f := range rs1.Fields {
			sk[i].Name = string(f.Name)
		}

		s, err := proxy.NewResultSetSorter(rs1, sk)
		if err != nil {
			panic(err)
		}
		sort.Sort(s)

		s, err = proxy.NewResultSetSorter(rs2, sk)
		if err != nil {
			panic(err)
		}
		sort.Sort(s)
	}

	// TODO: this is not fully correct, as when result sets
	// are sorted by _some_ columns data in other column may
	// be unsorted and thus not equal in a DeepEqual sense
	// but equal in terms of result sets. We should probably
	// check the correctness of 'order by' ordering separately.
	if !reflect.DeepEqual(rs1.Values, rs2.Values) {
		log.Println("resultset.Values not equal", rs1.Values, rs2.Values)
		return false
	}
	if !reflect.DeepEqual(rs1.RowDatas, rs2.RowDatas) {
		log.Println("resultset.RowDatas not equal", rs1.RowDatas, rs2.RowDatas)
		return false
	}

	return true
}

func main() {
	cf, _ := os.Open(configFile)
	dec := json.NewDecoder(cf)
	conf1 := Configuration{}
	conf2 := Configuration{}
	err := dec.Decode(&conf1)
	if err != nil {
		log.Fatalf("Error reading configuration from './%s': %s\n", configFile, err)
	}
	err = dec.Decode(&conf2)
	if err != nil {
		log.Fatalf("Error reading configuration from './%s': %s\n", configFile, err)
	}
	log.Printf("preplaying with config: %#v\n", conf1)
	log.Printf("preplaying with config: %#v\n", conf2)

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
		go replayAndCmp(sessions[pkt.session], pkt.session,
			firstepoch, starttime, &conf1, &conf2)
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
