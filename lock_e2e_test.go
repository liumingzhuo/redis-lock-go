package rlock

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ClientSuite struct {
	suite.Suite
	rdb redis.Cmdable
}

func (s *ClientSuite) SetupSuite() {
	s.rdb = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})
	for s.rdb.Ping(context.Background()).Err() != nil {

	}
}

func TestSuite(t *testing.T) {
	suite.Run(t, &ClientSuite{})
}

func (s *ClientSuite) TestTryLocke2e() {
	t := s.T()
	rdb := s.rdb
	client := NewClient(rdb)

	testCases := []struct {
		name       string
		before     func()
		after      func()
		key        string
		expiration time.Duration
		wantLock   *Lock
		wantErr    error
	}{
		// try lock success
		{
			name:   "locked",
			before: func() {},
			after: func() {
				res, err := rdb.Del(context.Background(), "locked-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			key:        "locked-key",
			expiration: time.Minute,
			wantLock: &Lock{
				key:        "locked-key",
				expiration: time.Minute,
			},
		},
		// failed lock
		{
			name: "failed-lock",
			before: func() {
				res, err := rdb.Set(context.Background(), "failed-key", "1", time.Minute).Result()
				require.NoError(t, err)
				require.Equal(t, "OK", res)
			},
			after: func() {
				res, err := rdb.Del(context.Background(), "failed-key").Result()
				require.NoError(t, err)
				require.Equal(t, int64(1), res)
			},
			key:        "failed-key",
			expiration: time.Minute,
			wantErr:    ErrFailedToSeizeLock,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			l, err := client.TryLock(context.Background(), tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.key, l.key)
			assert.Equal(t, tc.expiration, l.expiration)
			assert.NotEmpty(t, l.val)
			tc.after()
		})
	}
}

func (s *ClientSuite) TestUnlocke2e() {
	t := s.T()
	rdb := s.rdb
	client := NewClient(rdb)
	testCases := []struct {
		name string
		lock *Lock

		before func()
		after  func()

		wantLock *Lock
		wantErr  error
	}{
		{
			name: "unlocked",
			lock: func() *Lock {
				l, err := client.TryLock(context.Background(), "unlocked-key", time.Minute)
				require.NoError(t, err)
				return l
			}(),
			before: func() {},
			after: func() {
				res, err := rdb.Exists(context.Background(), "unlocked-key").Result()
				require.NoError(t, err)
				require.Equal(t, 1, res)
			},
		},
		{
			name:    "lock not hold",
			lock:    newLock(rdb, "not-hold-key", "123", time.Minute),
			wantErr: ErrLockNotHold,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.lock.Unlock(context.Background())
			require.Equal(t, tc.wantErr, err)
		})
	}
}
