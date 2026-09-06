// Copyright 2026 Pars Aria Labs. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package cmd

import (
	"reflect"
	"testing"

	"github.com/pars-aria-labs/asynq"
	"github.com/spf13/viper"
)

func TestRedisPrefixFlagIsRegistered(t *testing.T) {
	flag := rootCmd.PersistentFlags().Lookup("prefix")
	if flag == nil {
		t.Fatal("root command does not define --prefix")
	}
	if got, want := flag.DefValue, ""; got != want {
		t.Errorf("--prefix default = %q, want %q", got, want)
	}
}

func TestGetRedisConnOptIncludesPrefix(t *testing.T) {
	t.Cleanup(viper.Reset)

	t.Run("standalone", func(t *testing.T) {
		viper.Set("cluster", false)
		viper.Set("uri", "redis.internal:6380")
		viper.Set("db", 7)
		viper.Set("username", "worker")
		viper.Set("password", "secret")
		viper.Set("prefix", "billing-prod")

		got, ok := getRedisConnOpt().(asynq.RedisClientOpt)
		if !ok {
			t.Fatalf("getRedisConnOpt() type = %T, want asynq.RedisClientOpt", getRedisConnOpt())
		}
		want := asynq.RedisClientOpt{
			Addr:     "redis.internal:6380",
			DB:       7,
			Username: "worker",
			Password: "secret",
			Prefix:   "billing-prod",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("getRedisConnOpt() = %#v, want %#v", got, want)
		}
	})

	t.Run("cluster", func(t *testing.T) {
		viper.Set("cluster", true)
		viper.Set("cluster_addrs", "redis-0:7000,redis-1:7001")
		viper.Set("username", "worker")
		viper.Set("password", "secret")
		viper.Set("prefix", "billing-prod")

		got, ok := getRedisConnOpt().(asynq.RedisClusterClientOpt)
		if !ok {
			t.Fatalf("getRedisConnOpt() type = %T, want asynq.RedisClusterClientOpt", getRedisConnOpt())
		}
		want := asynq.RedisClusterClientOpt{
			Addrs:    []string{"redis-0:7000", "redis-1:7001"},
			Username: "worker",
			Password: "secret",
			Prefix:   "billing-prod",
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("getRedisConnOpt() = %#v, want %#v", got, want)
		}
	})
}
