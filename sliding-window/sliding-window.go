package sliding_window

import (
	"context"
	"sync"
	"time"
)

type metricBucket struct {
	*sync.RWMutex

	Success int32
	Failure int32
	Timeout int32
}

func (bucket *metricBucket) increment(kind string) {
	bucket.Lock()
	defer bucket.Unlock()

	if kind == "success" {
		bucket.Success += 1
	} else if kind == "failure" {
		bucket.Failure += 1
	} else if kind == "timeout" {
		bucket.Timeout += 1
	}
}

func (bucket *metricBucket) IsHealth(errorTolerance int32) bool {
	bucket.RLock()
	defer bucket.RUnlock()
	totalRequestCount := bucket.Failure + bucket.Success + bucket.Timeout
	return totalRequestCount == 0 || bucket.Failure/totalRequestCount < errorTolerance
}

type MetricCollector struct {
	*sync.RWMutex

	buckets        map[int64]*metricBucket
	capacity       int64
	timeout        time.Duration
	errorTolerance float64
}

func (c *MetricCollector) GetCurrentBucket() *metricBucket {
	current := time.Now().Unix()
	c.RLock()

	if f, ok := c.buckets[current]; ok {
		defer c.RUnlock()
		return f
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()
	f := &metricBucket{}
	c.buckets[current] = f
	return f
}

func (c *MetricCollector) removeOutdated() {
	outdated := time.Now().Unix() - c.capacity

	c.Lock()
	defer c.Unlock()
	for t := range c.buckets {
		if t <= outdated {
			delete(c.buckets, t)
		}
	}
}

func (c *MetricCollector) Do(fn func(ctx *context.Context) error, ctx *context.Context) {
	outcomeKind := "success"
	initTime := time.Now()
	err := fn(ctx)
	if err != nil {
		outcomeKind = "failure"
	} else if time.Since(initTime) > c.timeout {
		outcomeKind = "timeout"
	}

	func() {
		c.RLock()
		defer c.RUnlock()

		if bucket, ok := c.buckets[initTime.Unix()]; ok {
			bucket.increment(outcomeKind)
		}
	}()

	c.removeOutdated()
}

func (c *MetricCollector) IsHealth() bool {
	bucket := c.GetCurrentBucket()
	return bucket.IsHealth(int32(c.errorTolerance))
}
