//
// Copyright (c) 2018 Redis Labs Inc.
//
// SPDX-License-Identifier: Apache-2.0
//

// +build redisRunning

// This test will only be executed if the tag redisRunning is added when running
// the tests with a command like:
// LD_LIBRARY_PATH=$GOROOT/src/github.com/redislab/eredis/redis/src go test -tags redisRunning

// To test Redis, specify the a `Host` value as follows:
// * TCP connection: use the IP address or host name
// * Unix domain socket: use the path to the socket file (e.g. /tmp/redis.sock)
// * Embedded: leave empty

package clients

import (
	"testing"
)

func TestRedisDB(t *testing.T) {

	t.Log("This test needs to have a running Redis on localhost")

	config := DBConfiguration{
		DbType: REDIS,
		Host:   "0.0.0.0",
		Port:   6379,
	}

	rc, err := newRedisClient(config)
	if err != nil {
		t.Fatalf("Could not connect with Redis: %v", err)
	}

	testDB(t, rc)
}

func BenchmarkRedisDB_TCP(b *testing.B) {

	b.Log("This benchmark needs to have a running Redis on localhost")

	config := DBConfiguration{
		DbType: REDIS,
		Host:   "0.0.0.0",
		Port:   6379,
	}

	benchmarkDB(b, config)
}

func BenchmarkRedisDB_UDS(b *testing.B) {

	b.Log("This benchmark needs to have a running Redis listening /tmp/redis.sock")

	config := DBConfiguration{
		DbType: REDIS,
		Host:   "/tmp/redis.sock",
	}

	benchmarkDB(b, config)
}

func BenchmarkRedisDB_Embedded(b *testing.B) {

	b.Log("This benchmark needs to have liberedis.so at LD_LIBRARY_PATH=$GOROOT/src/github.com/redislabs/eredis/redis/src")

	config := DBConfiguration{
		DbType: REDIS,
		Host:   "",
	}

	benchmarkDB(b, config)
}
