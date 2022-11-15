package rlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/golang/mock/gomock"
	"github.com/liumingzhuo/redis-lock-go/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name       string
		mock       func() redis.Cmdable
		key        string
		val        string
		timeout    time.Duration
		expiration time.Duration
		retry      RetryStrategy

		wantLock *Lock
		wantErr  string
	}{
		{
			name: "lock success",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal("OK")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Second,
			retry:      &FixIntervalRetry{Interval: time.Second, Max: 1},
			timeout:    time.Second * 2,
			wantLock: &Lock{
				key:        "lock-key",
				expiration: time.Second,
			},
		},
		{
			name: "not retryable",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetErr(errors.New("network error"))
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry:      &FixIntervalRetry{Interval: time.Second, Max: 1},
			timeout:    time.Second,
			wantErr:    "network error",
		},
		{
			name: "retry over times",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetErr(context.DeadlineExceeded)
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Times(3).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry:      &FixIntervalRetry{Interval: time.Millisecond, Max: 2},
			timeout:    time.Second,
			wantErr:    "rlock exhausted retry times: last retry error: context deadline exceeded",
		},
		{
			name: "retry over times lock held",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal("")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Times(3).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry:      &FixIntervalRetry{Interval: time.Millisecond, Max: 2},
			timeout:    time.Second,
			wantErr:    "rlock exhausted retry times: lock is held: failed to seize lock",
		},
		{
			name: "retry and success",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal("")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Times(2).Return(res)

				second := redis.NewCmd(context.Background())
				second.SetVal("OK")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Return(second)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry:      &FixIntervalRetry{Interval: time.Millisecond, Max: 3},
			timeout:    time.Second,
			wantLock: &Lock{
				key:        "lock-key",
				expiration: time.Minute,
			},
		},
		{
			name: "retry but timeout",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				res := redis.NewCmd(context.Background())
				res.SetVal("")
				client.EXPECT().Eval(gomock.Any(), luaLock, []string{"lock-key"}, gomock.Any()).
					Times(2).Return(res)
				return client
			},
			key:        "lock-key",
			expiration: time.Minute,
			retry:      &FixIntervalRetry{Interval: time.Millisecond * 550, Max: 2},
			timeout:    time.Second,
			wantErr:    "context deadline exceeded",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			redisMock := tc.mock()
			client := NewClient(redisMock)
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			l, err := client.Lock(ctx, tc.key, tc.val, tc.expiration, time.Second, tc.retry)
			if tc.wantErr != "" {
				assert.EqualError(t, err, tc.wantErr)
				return
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, redisMock, l.client)
			assert.Equal(t, tc.key, l.key)
			assert.Equal(t, tc.expiration, l.expiration)
		})
	}
}

func TestClientTryLock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name       string
		key        string
		expiration time.Duration
		mock       func() redis.Cmdable
		wantLock   *Lock
		wantErr    error
	}{
		{
			name:       "locked",
			key:        "lock-key",
			expiration: time.Minute,
			mock: func() redis.Cmdable {
				res := mock.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "lock-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(true, nil))
				return res
			},
			wantLock: &Lock{
				key:        "lock-key",
				expiration: time.Minute,
			},
		},
		{
			name:       "network-err",
			key:        "network-err-key",
			expiration: time.Minute,
			mock: func() redis.Cmdable {
				res := mock.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "network-err-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(false, errors.New("network error")))
				return res
			},
			wantErr: errors.New("network error"),
		},
		{
			name:       "failed",
			key:        "failed-key",
			expiration: time.Minute,
			mock: func() redis.Cmdable {
				res := mock.NewMockCmdable(ctrl)
				res.EXPECT().SetNX(gomock.Any(), "failed-key", gomock.Any(), time.Minute).
					Return(redis.NewBoolResult(false, nil))
				return res
			},
			wantErr: ErrFailedToSeizeLock,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockRedisCmd := tc.mock()
			client := NewClient(mockRedisCmd)
			l, err := client.TryLock(context.Background(), tc.key, tc.expiration)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, mockRedisCmd, l.client)
			assert.Equal(t, tc.key, l.key)
			assert.Equal(t, tc.expiration, l.expiration)
			assert.NotEmpty(t, l.val)
		})
	}
}

func TestLockRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name string
		mock func() redis.Cmdable

		key     string
		value   string
		wantErr error
	}{
		{
			name: "refresh success",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				client.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"refresh-key"}, []any{"refresh-value", float64(60)}).
					Return(cmd)
				return client
			},
			key:   "refresh-key",
			value: "refresh-value",
		},
		{
			name: "failed key not exits",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background(), nil)
				cmd.SetErr(redis.Nil)
				client.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"failed-not-exits-key"}, []any{"123", time.Minute.Milliseconds()}).
					Return(cmd)
				return client
			},
			key:     "failed-not-exits-key",
			value:   "123",
			wantErr: redis.Nil,
		},
		{
			name: "failed network refresh",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background())
				cmd.SetErr(errors.New("network error"))
				client.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"failed-network-key"}, []any{"123", time.Minute.Milliseconds()}).
					Return(cmd)
				return client
			},
			key:     "failed-network-key",
			value:   "123",
			wantErr: errors.New("network error"),
		},
		{
			name: "failed not hold",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(0))
				client.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"failed-not-hold-key"}, []any{"123", time.Minute.Milliseconds()}).
					Return(cmd)
				return client
			},
			key:     "failed-not-hold-key",
			value:   "123",
			wantErr: ErrLockNotHold,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLock(tc.mock(), tc.key, tc.value, time.Minute)
			err := l.Refresh(context.Background())
			require.Equal(t, tc.wantErr, err)
		})
	}
}

func TestUnlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name string
		mock func() redis.Cmdable

		wantErr error
	}{
		{
			name: "unlocked",
			mock: func() redis.Cmdable {
				client := mock.NewMockCmdable(ctrl)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				client.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return client
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLock(tc.mock(), "mock-key", "mock-value", time.Minute)
			err := l.Unlock(context.Background())
			require.Equal(t, tc.wantErr, err)
		})
	}
}
func TestAutoRefresh(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	testCases := []struct {
		name         string
		lock         func() *Lock
		timeout      time.Duration
		interval     time.Duration
		unlockTiming time.Duration
		wantErr      error
	}{
		{
			name: "auto refresh",
			lock: func() *Lock {
				res := redis.NewCmd(context.Background())
				res.SetVal(int64(1))
				rdb := mock.NewMockCmdable(ctrl)
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refresh"}, []any{"123", float64(60)}).
					AnyTimes().Return(res)

				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return &Lock{
					client:     rdb,
					key:        "auto-refresh",
					val:        "123",
					expiration: time.Minute,
					unlock:     make(chan struct{}, 1),
				}
			},
			timeout:      time.Second * 2,
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second,
		},
		{
			name: "auto refresh failed",
			lock: func() *Lock {
				res := redis.NewCmd(context.Background(), nil)
				res.SetErr(errors.New("network error"))
				rdb := mock.NewMockCmdable(ctrl)
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refresh"}, []any{"123", float64(60)}).
					AnyTimes().Return(res)
				cmd := redis.NewCmd(context.Background())
				cmd.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(cmd)
				return &Lock{
					client:     rdb,
					key:        "auto-refresh",
					val:        "123",
					expiration: time.Minute,
					unlock:     make(chan struct{}, 1),
				}
			},
			timeout:      time.Second * 2,
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second,
			wantErr:      errors.New("network error"),
		},
		{
			name: "auto refresh timeout",
			lock: func() *Lock {
				first := redis.NewCmd(context.Background())
				first.SetErr(context.DeadlineExceeded)
				rdb := mock.NewMockCmdable(ctrl)
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refresh"}, []any{"123", float64(60)}).
					Return(first)

				second := redis.NewCmd(context.Background())
				second.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaRefresh, []string{"auto-refresh"}, []any{"123", float64(60)}).
					AnyTimes().Return(second)

				third := redis.NewCmd(context.Background())
				third.SetVal(int64(1))
				rdb.EXPECT().Eval(gomock.Any(), luaUnlock, gomock.Any(), gomock.Any()).
					Return(third)

				return &Lock{
					client:     rdb,
					key:        "auto-refresh",
					val:        "123",
					expiration: time.Minute,
					unlock:     make(chan struct{}, 1),
				}
			},
			interval:     time.Millisecond * 100,
			unlockTiming: time.Second,
			timeout:      time.Second * 2,
		},
	}
	for _, tt := range testCases {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			lock := tc.lock()
			go func() {
				time.Sleep(tc.unlockTiming)
				err := lock.Unlock(context.Background())
				require.NoError(t, err)
			}()
			err := lock.AutoRefresh(tc.interval, tc.timeout)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
