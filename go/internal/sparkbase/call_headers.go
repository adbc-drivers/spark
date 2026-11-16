// Copyright (c) 2026 ADBC Drivers Contributors
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

package sparkbase

import (
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/metadata"
)

// CallHeaders stores HTTP headers to include with requests.
type CallHeaders struct {
	mu     sync.RWMutex
	values metadata.MD
}

func normalizeHeaderName(name string) (string, error) {
	for _, ch := range name {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' && ch != '.' {
			return "", adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "[spark] invalid textual call header name"}
		}
	}
	name = strings.ToLower(name)
	if name == "" || strings.HasSuffix(name, "-bin") || strings.HasPrefix(name, "grpc-") {
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[spark] invalid call header name %s", name),
		}
	}
	return name, nil
}

func (h *CallHeaders) SetCallHeader(name, value string) error {
	name, err := normalizeHeaderName(name)
	if err != nil {
		return err
	}
	for _, ch := range value {
		if ch < 0x20 || ch > 0x7e {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "[spark] invalid call header value",
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.values == nil {
		h.values = metadata.MD{}
	}
	h.values.Set(name, value)
	return nil
}

func (h *CallHeaders) GetCallHeader(name string) (string, error) {
	name, err := normalizeHeaderName(name)
	if err != nil {
		return "", err
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if values, ok := h.values[name]; ok {
		return values[0], nil
	}
	return "", adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  "[spark] call header is not set",
	}
}

func (h *CallHeaders) Snapshot() metadata.MD {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.values.Copy()
}

// CallHeaderClient is an optional capability implemented by Connect clients.
type CallHeaderClient interface {
	SetCallHeader(string, string) error
	GetCallHeader(string) (string, error)
}
