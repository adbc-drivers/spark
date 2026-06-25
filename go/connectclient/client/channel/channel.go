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

package channel

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime"

	"github.com/google/uuid"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/adbc-drivers/spark/go/connectclient/sparkerrors"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

// Builder is the interface that is used to implement different patterns that
// create the GRPC connection.
//
// This allows other consumers to plugin custom authentication and authorization
// handlers without having to extend directly the Spark Connect code.
type Builder interface {
	// Build creates the grpc.ClientConn according to the configuration of the builder.
	// Implementations are free to provide additional parameters in their implementation
	// and simply must satisfy this minimal set of requirements.
	Build(ctx context.Context) (*grpc.ClientConn, error)
	// User identifies the username passed as part of the Spark Connect requests.
	User() string
	// Headers refers to the request metadata that is passed for every request from the
	// client to the server.
	Headers() map[string]string
	// SessionId identifies the client side session identifier. This value must be a UUID formatted
	// as a string.
	SessionId() string
	// UserAgent identifies the user agent string that is passed as part of the request. It contains
	// information about the operating system, Go version etc.
	UserAgent() string
}

// BaseBuilder stores the parameters used to create a Spark Connect channel.
type BaseBuilder struct {
	host                      string
	port                      int
	token                     string
	user                      string
	useSSL                    bool
	validateServerCertificate bool
	headers                   map[string]string
	sessionId                 string
	userAgent                 string
}

type ConnectionParameters struct {
	Host                      string
	Port                      int
	Token                     string
	User                      string
	UseSSL                    bool
	ValidateServerCertificate *bool
	Headers                   map[string]string
	SessionID                 string
	UserAgent                 string
}

func (cb *BaseBuilder) Host() string {
	return cb.host
}

func (cb *BaseBuilder) Port() int {
	return cb.port
}

func (cb *BaseBuilder) Token() string {
	return cb.token
}

func (cb *BaseBuilder) User() string {
	return cb.user
}

func (cb *BaseBuilder) Headers() map[string]string {
	return cb.headers
}

func (cb *BaseBuilder) SessionId() string {
	return cb.sessionId
}

func (cb *BaseBuilder) UserAgent() string {
	return cb.userAgent
}

// Build finalizes the creation of the gprc.ClientConn by creating a GRPC channel
// with the necessary options extracted from the connection string. For
// TLS connections, this function will load the system certificates.
func (cb *BaseBuilder) Build(ctx context.Context) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithAuthority(cb.host))
	if cb.token == "" && !cb.useSSL {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		config := &tls.Config{
			InsecureSkipVerify: !cb.validateServerCertificate,
		}
		cred := credentials.NewTLS(config)
		opts = append(opts, grpc.WithTransportCredentials(cred))
		if cb.token != "" {
			ts := oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: cb.token,
				TokenType:   "bearer",
			})
			opts = append(opts, grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: ts}))
		}
	}

	remote := fmt.Sprintf("%v:%v", cb.host, cb.port)
	conn, err := grpc.NewClient(remote, opts...)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w",
			remote, err), sparkerrors.ConnectionError)
	}
	return conn, nil
}

// NewBuilder creates a new instance of the BaseBuilder from typed connection
// parameters.
func NewBuilder(params ConnectionParameters) (*BaseBuilder, error) {
	if params.Host == "" {
		return nil, sparkerrors.WithType(errors.New("connection parameters must contain a hostname"), sparkerrors.InvalidInputError)
	}

	port := params.Port
	if port == 0 {
		port = 15002
	}

	if port < 0 {
		return nil, sparkerrors.WithType(errors.New("port must be non-negative"), sparkerrors.InvalidInputError)
	}

	validateServerCertificate := true
	if params.ValidateServerCertificate != nil {
		validateServerCertificate = *params.ValidateServerCertificate
	}

	headers := make(map[string]string, len(params.Headers))
	maps.Copy(headers, params.Headers)

	sessionId := params.SessionID
	if sessionId == "" {
		sessionId = uuid.NewString()
	}

	cb := &BaseBuilder{
		host:                      params.Host,
		port:                      port,
		token:                     params.Token,
		user:                      params.User,
		useSSL:                    params.UseSSL,
		validateServerCertificate: validateServerCertificate,
		headers:                   headers,
		sessionId:                 sessionId,
		userAgent:                 params.UserAgent,
	}

	// Set default user ID if not set.
	if cb.user == "" {
		cb.user = os.Getenv("USER")
		if cb.user == "" {
			cb.user = "na"
		}
	}

	// Update the user agent if it is not set or set to a custom value.
	val := os.Getenv("SPARK_CONNECT_USER_AGENT")
	if cb.userAgent == "" && val != "" {
		cb.userAgent = os.Getenv("SPARK_CONNECT_USER_AGENT")
	} else if cb.userAgent == "" {
		cb.userAgent = "_SPARK_CONNECT_GO"
	}

	// In addition, to the specified user agent, we need to append information about the
	// host encoded as user agent components.
	// TODO(lidavidm): encode driver version, proto revision, etc.
	cb.userAgent = fmt.Sprintf("%s os/%s go/%s", cb.userAgent, runtime.GOOS, runtime.Version())

	return cb, nil
}
