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
