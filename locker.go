package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrLockerAlreadySet = errors.New("locker has been occupied")
)

func RedisLockerE(cli *redis.Client, key, val string, ttl time.Duration, f func() error) (bool, error) {
	unlocker, err := RedisLock(cli, key, ttl)
	if err != nil {
		if errors.Is(err, ErrLockerAlreadySet) {
			return false, nil
		}
		return false, err
	}
	defer unlocker()
	return true, f()
}

func RedisLock(cli *redis.Client, key string, ttl time.Duration) (unlocker func(), err error) {
	return RedisLockV(cli, key, time.Now().String(), ttl)
}

func RedisLockV(cli *redis.Client, key string, val string, ttl time.Duration) (unlocker func(), err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reply, err := cli.SetNX(ctx, key, val, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("get locker %s, err: %v", key, err)
	}
	if !reply {
		return nil, ErrLockerAlreadySet
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cli.Del(ctx, key)
	}, nil
}
