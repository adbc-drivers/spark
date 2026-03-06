// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sasl

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"
)

type status byte

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-sasl-spec.txt
const (
	stSTART    status = iota + 1 // Hello, let's go on a date
	stOK                         // Everything's good so far, let's see each other again
	stBAD                        // I understand what you're saying, I just don't like it, we have to break up
	stERROR                      // We can't go on like this, it's like you're speaking another language
	stCOMPLETE                   // Let's do this!
)

type Mechanism interface {
	Name() string
	SetHost(host string)
	Start() ([]byte, error)
	Step(challenge []byte) ([]byte, error)
	Encode([]byte) ([]byte, error)
	Decode([]byte) ([]byte, error)
}

type Transport struct {
	mech Mechanism
	tp   thrift.TTransport

	readBuf, writeBuf bytes.Buffer
	headerBuf         [5]byte
}

func WrapTransport(tp thrift.TTransport, hostname string, mech Mechanism) *Transport {
	if mech == nil {
		mech = &PlainMechanism{}
	}
	mech.SetHost(hostname)

	return &Transport{
		tp:   tp,
		mech: mech,
	}
}

func (t *Transport) Open() error {
	if !t.IsOpen() {
		if err := t.tp.Open(); err != nil {
			return thrift.NewTTransportExceptionFromError(err)
		}
	}

	ctx := context.TODO()
	if err := t.sendSaslMsg(ctx, stSTART, []byte(t.mech.Name())); err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}

	initial, err := t.mech.Start()
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}

	err = t.sendSaslMsg(context.Background(), stOK, initial)

SASL_LOOP:
	for {
		if err != nil {
			return thrift.NewTTransportExceptionFromError(err)
		}

		status, payload, err := t.recvSaslMsg()
		if err != nil {
			return thrift.NewTTransportExceptionFromError(err)
		}

		switch status {
		case stOK:
			next, err := t.mech.Step(payload)
			if err != nil {
				return thrift.NewTTransportExceptionFromError(err)
			}
			err = t.sendSaslMsg(ctx, stOK, next)
		case stCOMPLETE:
			// log the payload if len > 0?
			break SASL_LOOP
		case stBAD:
			fallthrough
		case stERROR:
			return thrift.NewTTransportExceptionFromError(fmt.Errorf("sasl negotiation failed: %d (%s)", status, string(payload)))
		}
	}

	return nil
}

func (t *Transport) sendSaslMsg(ctx context.Context, s status, body []byte) error {
	t.headerBuf[0] = byte(s)
	binary.BigEndian.PutUint32(t.headerBuf[1:], uint32(len(body)))

	_, err := t.tp.Write(append(t.headerBuf[:], body...))
	if err != nil {
		return err
	}

	return t.tp.Flush(ctx)
}

func (t *Transport) recvSaslMsg() (status, []byte, error) {
	if _, err := io.ReadFull(t.tp, t.headerBuf[:]); err != nil {
		return stERROR, nil, err
	}

	status := status(t.headerBuf[0])
	bodyLen := binary.BigEndian.Uint32(t.headerBuf[1:])

	var payload []byte
	if bodyLen > 0 {
		payload = make([]byte, bodyLen)
		if _, err := io.ReadFull(t.tp, payload); err != nil {
			return stERROR, nil, err
		}
	}

	return status, payload, nil
}

func (t *Transport) IsOpen() bool {
	return t.tp.IsOpen()
}

func (t *Transport) Read(p []byte) (n int, err error) {
	n, err = t.readBuf.Read(p)
	if len(p) == n || (err != nil && !errors.Is(err, io.EOF)) {
		return
	}

	// read and decode next frame
	buf := t.headerBuf[:4]
	if _, err := io.ReadFull(t.tp, buf); err != nil {
		return n, thrift.NewTTransportExceptionFromError(err)
	}

	sz := binary.BigEndian.Uint32(buf)
	if sz > thrift.DEFAULT_MAX_FRAME_SIZE {
		return n, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, fmt.Sprintf("invalid frame size: %d", sz))
	}

	t.readBuf.Reset()
	t.readBuf.Grow(int(sz))
	_, err = io.CopyN(&t.readBuf, t.tp, int64(sz))
	if err != nil {
		return n, thrift.NewTTransportExceptionFromError(err)
	}

	decoded, err := t.mech.Decode(t.readBuf.Bytes())
	if err != nil {
		return n, thrift.NewTTransportExceptionFromError(err)
	}

	t.readBuf.Truncate(len(decoded))
	return t.readBuf.Read(p[n:])
}

func (t *Transport) Write(p []byte) (n int, err error) {
	return t.writeBuf.Write(p)
}

func (t *Transport) Close() error { return t.tp.Close() }

func (t *Transport) RemainingBytes() (numBytes uint64) {
	return uint64(t.readBuf.Len())
}

func (t *Transport) Flush(ctx context.Context) error {
	wrappedBuf, err := t.mech.Encode(t.writeBuf.Bytes())
	if err != nil {
		return thrift.NewTTransportExceptionFromError(err)
	}

	t.writeBuf.Reset()

	sz, buf := len(wrappedBuf), t.headerBuf[:4]
	binary.BigEndian.PutUint32(buf, uint32(sz))

	if _, err = t.tp.Write(buf); err != nil {
		return err
	}

	if sz > 0 {
		if n, err := t.tp.Write(wrappedBuf); err != nil {
			return thrift.NewTTransportExceptionFromError(fmt.Errorf("error while flushing write buffer (%d bytes) only wrote %d bytes: %w", sz, n, err))
		}
	}

	return t.tp.Flush(ctx)
}

var (
	_ thrift.TTransport = (*Transport)(nil)
	_ Mechanism         = (*PlainMechanism)(nil)
	_ Mechanism         = (*GSSAPIMechanism)(nil)
	_ Mechanism         = (*DigestMD5Mechanism)(nil)
)
