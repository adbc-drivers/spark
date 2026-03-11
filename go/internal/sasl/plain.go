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

type PlainMechanism struct {
	Username string
	Password string
	AuthID   string

	identity string
}

func (m *PlainMechanism) Name() string {
	return "PLAIN"
}

func (m *PlainMechanism) SetHost(string) {}

func (m *PlainMechanism) Start() ([]byte, error) {
	return m.Step(nil)
}

func (m *PlainMechanism) Step(challenge []byte) ([]byte, error) {
	var authID string

	if m.AuthID != "" {
		authID = m.AuthID
	} else {
		authID = m.identity
	}

	const NUL = "\x00"
	return []byte(authID + NUL + m.Username + NUL + m.Password), nil
}

func (m *PlainMechanism) Encode(data []byte) ([]byte, error) {
	return data, nil
}

func (m *PlainMechanism) Decode(data []byte) ([]byte, error) {
	return data, nil
}
