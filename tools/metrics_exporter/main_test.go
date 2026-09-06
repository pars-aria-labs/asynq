// Copyright 2026 Pars Aria Labs. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package main

import (
	"reflect"
	"testing"

	"github.com/pars-aria-labs/asynq"
)

func TestRedisClientOptFromFlags(t *testing.T) {
	previousAddr := flagRedisAddr
	previousDB := flagRedisDB
	previousPassword := flagRedisPassword
	previousUsername := flagRedisUsername
	previousPrefix := flagRedisPrefix
	t.Cleanup(func() {
		flagRedisAddr = previousAddr
		flagRedisDB = previousDB
		flagRedisPassword = previousPassword
		flagRedisUsername = previousUsername
		flagRedisPrefix = previousPrefix
	})

	flagRedisAddr = "redis.internal:6380"
	flagRedisDB = 7
	flagRedisPassword = "secret"
	flagRedisUsername = "metrics"
	flagRedisPrefix = "billing-prod"

	want := asynq.RedisClientOpt{
		Addr:     "redis.internal:6380",
		DB:       7,
		Password: "secret",
		Username: "metrics",
		Prefix:   "billing-prod",
	}
	if got := redisClientOptFromFlags(); !reflect.DeepEqual(got, want) {
		t.Errorf("redisClientOptFromFlags() = %#v, want %#v", got, want)
	}
}
