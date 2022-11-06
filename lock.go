package rlock

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/google/uuid"
)

var (
	// error failed to seize lock
	ErrFailedToSeizeLock = errors.New("failed to seize lock")
	// error key does not hold
	ErrLockNotHold = errors.New("error lock not hold")

	//go:embed script/unlock.lua
	luaUnlock string
)

type Client struct {
	client redis.Cmdable
}

func NewClient(client redis.Cmdable) *Client {
	return &Client{client: client}
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
	client     redis.Cmdable
	key        string
	val        string
	expiration time.Duration
}

// newLock create Lock struct
func newLock(client redis.Cmdable, key string, val string, expiration time.Duration) *Lock {
	return &Lock{
		client:     client,
		key:        key,
		val:        val,
		expiration: expiration,
	}
}

// Unlock
func (l *Lock) Unlock(ctx context.Context) error {
	resp, err := l.client.Eval(ctx, luaUnlock, []string{l.key}, l.val).Int64()

	if err == redis.Nil {
		// key does not exits
		return ErrLockNotHold
	}

	if err != nil {
		// unkonw error, maybe net error
		return err
	}

	if resp != 1 {
		// unlock failed
		return ErrLockNotHold
	}

	return nil
}
