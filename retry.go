package rlock

import "time"

type RetryStrategy interface {
	Next() (time.Duration, bool)
}

type FixIntervalRetry struct {
	Interval time.Duration
	Max      int
	cnt      int
}

func (f *FixIntervalRetry) Next() (time.Duration, bool) {
	f.cnt++
	return f.Interval, f.cnt <= f.Max
}
