package main

import (
	"bytes"
	"context"
	"math/rand"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/gravitational/lf/lf"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

// Benchmark specifies benchmark requests to run
type Benchmark struct {
	// Dir is a directory with log database
	Dir string
	// Threads is amount of concurrent execution threads to run
	Threads int
	// Rate is requests per second origination rate
	Rate int
	// Duration is test duration
	Duration time.Duration
	// CompactionPeriod is a compaction period to activate
	CompactionPeriod time.Duration
}

// BenchmarkResult is a result of the benchmark
type BenchmarkResult struct {
	// RequestsOriginated is amount of reuqests originated
	RequestsOriginated int
	// RequestsFailed is amount of requests failed
	RequestsFailed int
	// Histogram is a duration histogram
	Histogram *hdrhistogram.Histogram
	// LastError contains last recorded error
	LastError error
}

func RunBenchmark(ctx context.Context, bench Benchmark) (*BenchmarkResult, error) {
	ctx, cancel := context.WithTimeout(ctx, bench.Duration)
	defer cancel()

	l, err := lf.NewDirLog(lf.DirLogConfig{
		Context:          ctx,
		Dir:              bench.Dir,
		CompactionPeriod: bench.CompactionPeriod,
	})
	if err != nil {
		return nil, trace.Wrap(err)
	}
	defer l.Close()

	requestC := make(chan *benchMeasure)
	responseC := make(chan *benchMeasure, 2*bench.Threads)

	// create goroutines for concurrency
	for i := 0; i < bench.Threads; i++ {
		thread := &benchmarkThread{
			id:        i,
			ctx:       ctx,
			dir:       l,
			receiveC:  requestC,
			sendC:     responseC,
			generator: rand.New(rand.NewSource(time.Now().UnixNano())),
		}
		go thread.run()
	}

	// producer goroutine
	go func() {
		interval := time.Duration(float64(1) / float64(bench.Rate) * float64(time.Second))
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// notice how we start the timer regardless of whether any goroutine can process it
				// this is to account for coordinated omission,
				// http://psy-lob-saw.blogspot.com/2015/03/fixing-ycsb-coordinated-omission.html
				measure := &benchMeasure{
					Start: time.Now(),
				}
				select {
				case requestC <- measure:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	var result BenchmarkResult
	// from one millisecond to 60000 milliseconds (minute) with 3 digits precision
	result.Histogram = hdrhistogram.New(1, 60000, 3)

	var doneThreads int
	var timeoutC <-chan time.Time
	doneC := ctx.Done()
	for {
		select {
		case <-timeoutC:
			result.LastError = trace.BadParameter("several requests hang: timeout waiting for %v threads to finish", bench.Threads-doneThreads)
			return &result, nil
		case <-doneC:
			// give it a couple of seconds to wrap up the goroutines,
			// set up the timer that will fire up if the all goroutines were not finished
			doneC = nil
			waitTime := time.Duration(result.Histogram.Max()) * time.Millisecond
			// going to wait latency + buffer to give requests in flight to wrap up
			waitTime = time.Duration(1.2 * float64(waitTime))
			timeoutC = time.After(waitTime)
		case measure := <-responseC:
			if measure.ThreadCompleted {
				doneThreads += 1
				if doneThreads == bench.Threads {
					return &result, nil
				}
			} else {
				if measure.Error != nil {
					result.RequestsFailed += 1
					result.LastError = measure.Error
				}
				result.RequestsOriginated += 1
				result.Histogram.RecordValue(int64(measure.End.Sub(measure.Start) / time.Millisecond))
			}
		}
	}

}

type benchMeasure struct {
	Start           time.Time
	End             time.Time
	ThreadCompleted bool
	ThreadID        int
	Error           error
}

type benchmarkThread struct {
	generator *rand.Rand
	id        int
	ctx       context.Context
	dir       *lf.DirLog
	receiveC  chan *benchMeasure
	sendC     chan *benchMeasure
}

func (b *benchmarkThread) generate(startSize int) []byte {
	size := b.generator.Intn(startSize) + startSize
	out := make([]byte, size)
	b.generator.Read(out)
	return out
}

func (b *benchmarkThread) execute(measure *benchMeasure) {
	var err error
	defer func() {
		b.sendMeasure(measure)
		measure.Error = err
		measure.End = time.Now()
	}()
	key := b.generate(64)
	val := b.generate(8192)
	newVal := b.generate(8192)

	err = b.dir.Create(lf.Item{
		Key: key,
		Val: val,
	})
	if err != nil {
		return
	}

	var out *lf.Item
	out, err = b.dir.Get(key)
	if err != nil {
		return
	}
	if bytes.Compare(key, out.Key) != 0 {
		err = trace.CompareFailed("keys are not equal")
		return
	}
	if bytes.Compare(val, out.Val) != 0 {
		err = trace.CompareFailed("vals are not equal")
		return
	}
	err = b.dir.Update(lf.Item{Key: key, Val: newVal})
	if err != nil {
		return
	}
	out, err = b.dir.Get(key)
	if err != nil {
		return
	}
	if bytes.Compare(newVal, out.Val) != 0 {
		err = trace.CompareFailed("vals are not equal")
		return
	}
	err = b.dir.Delete(key)
	if err != nil {
		return
	}
	return
}

func (b *benchmarkThread) sendMeasure(measure *benchMeasure) {
	measure.ThreadID = b.id
	select {
	case b.sendC <- measure:
	default:
		log.Warningf("blocked on measure send\n")
	}
}

func (b *benchmarkThread) run() {
	defer func() {
		if r := recover(); r != nil {
			log.Warningf("recover from panic: %v", r)
			b.sendMeasure(&benchMeasure{ThreadCompleted: true})
		}
	}()

	for {
		select {
		case measure := <-b.receiveC:
			b.execute(measure)
		case <-b.ctx.Done():
			b.sendMeasure(&benchMeasure{
				ThreadCompleted: true,
			})
			return
		}
	}
}
