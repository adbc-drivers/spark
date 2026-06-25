// Copyright (c) 2026 ADBC Drivers Contributors
//
// This file has been modified from its original version, which is
// under the Apache License:
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package channel_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/adbc-drivers/spark/go/connectclient/client/channel"
	"github.com/adbc-drivers/spark/go/connectclient/sparkerrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicChannelBuilder(t *testing.T) {
	cb, _ := channel.NewBuilder(channel.ConnectionParameters{Host: "host"})
	if cb == nil {
		t.Error("ChannelBuilder must not be null")
	}
}

func TestBasicChannelParsing(t *testing.T) {
	cb, err := channel.NewBuilder(channel.ConnectionParameters{Host: "empty"})
	assert.Nilf(t, err, "Valid parameters should not fail: %v", err)
	assert.Equalf(t, 15002, cb.Port(), "Default port must be set, but got %v", cb.Port)

	cb, err = channel.NewBuilder(channel.ConnectionParameters{
		Host:  "host",
		Port:  15002,
		User:  "a",
		Token: "b",
		Headers: map[string]string{
			"x-other-header": "c",
		},
	})
	assert.Nilf(t, err, "Should not have an error for a proper URL")
	assert.Equal(t, "host", cb.Host())
	assert.Equal(t, 15002, cb.Port())
	assert.Len(t, cb.Headers(), 1)
	assert.Equal(t, "c", cb.Headers()["x-other-header"])
	assert.Equal(t, "a", cb.User())
	assert.Equal(t, "b", cb.Token())

	cb, err = channel.NewBuilder(channel.ConnectionParameters{
		Host:      "localhost",
		Port:      443,
		Token:     "token",
		User:      "user_id",
		SessionID: "session",
		Headers: map[string]string{
			"cluster_id": "a",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 443, cb.Port())
	assert.Equal(t, "localhost", cb.Host())
	assert.Equal(t, "token", cb.Token())
	assert.Equal(t, "user_id", cb.User())
	assert.Equal(t, "session", cb.SessionId())
}

func TestChannelBuildConnect(t *testing.T) {
	ctx := context.Background()
	cb, err := channel.NewBuilder(channel.ConnectionParameters{Host: "localhost"})
	assert.NoError(t, err)
	id := cb.SessionId()
	_, err = uuid.Parse(id)
	assert.NoError(t, err)
	assert.NoError(t, err, "Should not have an error for a proper URL.")
	conn, err := cb.Build(ctx)
	assert.Nil(t, err, "no error for proper connection")
	assert.NotNil(t, conn)

	cb, err = channel.NewBuilder(channel.ConnectionParameters{
		Host:  "localhost",
		Port:  443,
		Token: "abcd",
		User:  "a",
	})
	assert.Nil(t, err, "Should not have an error for a proper URL.")
	conn, err = cb.Build(ctx)
	assert.Nil(t, err, "no error for proper connection")
	assert.NotNil(t, conn)
}

func TestChannelBulder_UserAgent(t *testing.T) {
	cb, err := channel.NewBuilder(channel.ConnectionParameters{Host: "localhost"})
	assert.NoError(t, err)
	assert.True(t, strings.Contains(cb.UserAgent(), "_SPARK_CONNECT_GO"))
	assert.True(t, strings.Contains(cb.UserAgent(), "go/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "os/"))

	cb, err = channel.NewBuilder(channel.ConnectionParameters{Host: "localhost", UserAgent: "custom"})
	assert.NoError(t, err)
	assert.True(t, strings.Contains(cb.UserAgent(), "custom"))
	assert.True(t, strings.Contains(cb.UserAgent(), "go/"))
	assert.True(t, strings.Contains(cb.UserAgent(), "os/"))
}

func TestBasicChannelParsingRejectsMissingHost(t *testing.T) {
	_, err := channel.NewBuilder(channel.ConnectionParameters{})
	require.Error(t, err)
	assert.ErrorIs(t, err, sparkerrors.InvalidInputError)
}
