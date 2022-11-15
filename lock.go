package rlock

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
)

var (
	// error failed to seize lock
	ErrFailedToSeizeLock = errors.New("failed to seize lock")
	// error key does not hold
	ErrLockNotHold = errors.New("error lock not hold")

	//go:embed script/lock.lua
	luaLock string

	//go:embed script/unlock.lua
	luaUnlock string

	//go:embed script/refresh.lua
	luaRefresh string
)

type Client struct {
	client redis.Cmdable
}

func NewClient(client redis.Cmdable) *Client {
	return &Client{client: client}
}

// Lock
func (c *Client) Lock(ctx context.Context, key string, val string,
	expiration time.Duration, timeout time.Duration, retry RetryStrategy) (*Lock, error) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		lctx, cancel := context.WithTimeout(ctx, timeout)
		res, err := c.client.Eval(lctx, luaLock, []string{key}, val, expiration.Seconds()).Result()
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// lock is success
		if res == "OK" {
			return newLock(c.client, key, val, expiration), nil
		}

		interval, ok := retry.Next()
		if !ok {
			if err != nil {
				err = fmt.Errorf("last retry error: %w", err)
			} else {
				err = fmt.Errorf("lock is held: %w", ErrFailedToSeizeLock)
			}

			return nil, err
		}
		if timer == nil {
			timer = time.NewTimer(interval)
		} else {
			timer.Reset(interval)
		}

		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TryLock try to lock with key
func (c *Client) TryLock(ctx context.Context, key string, expiration time.Duration) (*Lock, error) {
	val := uuid.New().String()

	ok, err := c.client.SetNX(ctx, key, val, expiration).Result()
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrFailedToSeizeLock
	}

	return newLock(c.client, key, val, expiration), nil
}

type Lock struct {
	client           redis.Cmdable
	key              string
	val              string
	expiration       time.Duration
	unlock           chan struct{}
	signalUnlockOnce sync.Once
}

// newLock create Lock struct
func newLock(client redis.Cmdable, key string, val string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		val:        val,
		expiration: expiration,
		unlock:     make(chan struct{}, 1),
	}
}

// AutoRefresh
func (l *Lock) AutoRefresh(interval time.Duration, timeout time.Duration) error {
	ticker := time.NewTicker(interval)
	// channel with timeout
	ch := make(chan struct{}, 1)
	defer func() {
		ticker.Stop()
		close(ch)
	}()
	for {
		select {
		case <-ticker.C:
			lctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(lctx)
			cancel()
			if err == context.DeadlineExceeded {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			// unknow error, maybe network
			if err != nil {
				return err
			}
		case <-ch:
			lctx, cancel := context.WithTimeout(context.Background(), timeout)
			err := l.Refresh(lctx)
			cancel()
			if err == context.DeadlineExceeded {
				select {
				case ch <- struct{}{}:
				default:
				}
				continue
			}
			if err != nil {
				return err
			}
		case <-l.unlock:
			return nil
		}
	}
}

// Refresh refresh key
func (l *Lock) Refresh(ctx context.Context) error {
	resp, err := l.client.Eval(ctx, luaRefresh, []string{l.key}, l.val, l.expiration.Seconds()).Int64()
	if err != nil {
		return err
	}
	if resp != 1 {
		return ErrLockNotHold
	}

	return nil
}

// Unlock
func (l *Lock) Unlock(ctx context.Context) error {
	resp, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.val).Int64()
	defer func() {
		l.signalUnlockOnce.Do(func() {
			l.unlock <- struct{}{}
			close(l.unlock)
		})
	}()
	if err == redis.Nil {
		// key does not exits
		return ErrLockNotHold
	}

	if err != nil {
		// unkonw error, maybe network error
		return err
	}

	if resp != 1 {
		// unlock failed
		return ErrLockNotHold
	}

	return nil
}
