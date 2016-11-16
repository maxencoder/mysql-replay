package stats

import (
	"math"
	"sort"
	"sync"
	"time"
)

type Stats struct {
	sync.Mutex
	wallclocks []time.Duration
}

func (s *Stats) Append(d time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.wallclocks = append(s.wallclocks, d)
}

func (s *Stats) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.wallclocks)
}

func (s *Stats) Sort() {
	s.Lock()
	defer s.Unlock()
	sort.Sort(Durations(s.wallclocks))
}

func (s *Stats) Percentile(n float64) time.Duration {
	s.Lock()
	defer s.Unlock()
	return s.wallclocks[int(float64(len(s.wallclocks))*n/100.0)]
}

func (s *Stats) Mean() time.Duration {
	s.Lock()
	defer s.Unlock()
	var sum time.Duration
	for _, d := range s.wallclocks {
		sum += d
	}
	return sum / time.Duration(len(s.wallclocks))
}

func (s *Stats) Stddev() float64 {
	mean := s.Mean()
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
