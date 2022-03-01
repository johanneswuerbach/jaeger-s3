package s3spanstore

import (
	"context"
	"math/rand"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
)

type DependenciesPrefetch struct {
	logger        hclog.Logger
	reader        ReaderWithDependencies
	ticker        *time.Ticker
	enabled       bool
	done          chan bool
	ctx           context.Context
	sleepDuration time.Duration
}

type ReaderWithDependencies interface {
	GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error)
}

func NewDependenciesPrefetch(ctx context.Context, logger hclog.Logger, reader ReaderWithDependencies, interval time.Duration, enabled bool) *DependenciesPrefetch {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	return &DependenciesPrefetch{
		logger:        logger,
		reader:        reader,
		ticker:        time.NewTicker(interval),
		enabled:       enabled,
		done:          make(chan bool),
		ctx:           ctx,
		sleepDuration: time.Second * time.Duration(r1.Intn(180)),
	}
}

func (d *DependenciesPrefetch) Start() {
	if !d.enabled {
		return
	}

	go func() {
		// Do an initial prefetch
		d.prefetchDependencies()

		// Schedule background prefetches
		for {
			select {
			case <-d.done:
				return
			case <-d.ticker.C:
				d.prefetchDependencies()
			}
		}
	}()
}

func (d *DependenciesPrefetch) prefetchDependencies() {
	// Ensure different readers don't refresh at the same time
	time.Sleep(d.sleepDuration)

	// GetDependencies to ensure the result is cached
	if _, err := d.reader.GetDependencies(d.ctx, time.Now(), time.Hour*24*7); err != nil {
		d.logger.Error("failed to get dependencies", err)
	}
}

func (d *DependenciesPrefetch) Stop() {
	if !d.enabled {
		return
	}

	d.ticker.Stop()
	d.done <- true
}
