//
// Copyright (c) 2018 Redis Labs Inc.
//
// SPDX-License-Identifier: Apache-2.0
//

// +build redisRunning

// This test will only be executed if the tag redusRunning is added when running
// the tests with a command like:
// go test -tags redisRunning

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

func BenchmarkRedisDB(b *testing.B) {

	b.Log("This benchmark needs to have a running Redis on localhost")

	config := DBConfiguration{
		DbType: REDIS,
		Host:   "0.0.0.0",
		Port:   6379,
	}

	benchmarkDB(b, config)
}
