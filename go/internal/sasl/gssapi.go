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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"regexp"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/crypto"
	"github.com/jcmturner/gokrb5/v8/gssapi"
	"github.com/jcmturner/gokrb5/v8/iana/keyusage"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/jcmturner/gokrb5/v8/spnego"
	"github.com/jcmturner/gokrb5/v8/types"
)

var krbSPNHost = regexp.MustCompile(`\A[^/]+/(_HOST)([@/]|\z)`)

// GSSAPIMechanism implements the GSSAPI mechanism for SASL specifically
// using KRB5 as the underlying mechanism.
//
// TODO: push the krb specific details under a gssapi.Mechanism interface
// implementation and then reimplement in terms of that interface.
// This will allow for other GSSAPI mechanisms to be implemented in the future.
type GSSAPIMechanism struct {
	Username    string
	Password    string
	Service     string
	Realm       string
	SelectedQOP byte

	Kt       *keytab.Keytab
	Krb5Conf *config.Config

	hostname   string
	fqsn       string
	krbClient  *client.Client
	sessionkey types.EncryptionKey
	kvno       int

	stage           int
	serverMaxLength int
	qop             byte
}

func (m *GSSAPIMechanism) Name() string {
	return "GSSAPI"
}

func (m *GSSAPIMechanism) SetHost(host string) {
	m.hostname = host
}

func (m *GSSAPIMechanism) Start() ([]byte, error) {
	if m.SelectedQOP == 0 {
		m.SelectedQOP = byte(qopPriority[QopAuth] | qopPriority[QopIntegrity] | qopPriority[QopPrivacy])
	}

	// Allow using a service principal designated for another host to still be used
	// useful for containerized environments
	qualifiedServiceHost := os.Getenv("SERVICE_HOST_QUALIFIED")
	if qualifiedServiceHost != "" {
		m.fqsn = replaceSPNHostWildcard(m.Service, qualifiedServiceHost)
	} else {
		m.fqsn = replaceSPNHostWildcard(m.Service, m.hostname)
	}

	if m.Krb5Conf == nil {
		m.Krb5Conf = config.New()
	}

	if m.Kt != nil {
		m.krbClient = client.NewWithKeytab(m.Username, m.Realm, m.Kt, m.Krb5Conf)
	} else {
		m.krbClient = client.NewWithPassword(m.Username, m.Realm, m.Password, m.Krb5Conf)
	}

	m.stage = 0
	return m.Step(nil)
}

func (m *GSSAPIMechanism) Step(challenge []byte) ([]byte, error) {
	switch m.stage {
	case 0:
		ticket, key, err := m.krbClient.GetServiceTicket(m.fqsn)
		if err != nil {
			return nil, err
		}

		m.kvno = ticket.EncPart.KVNO

		token, err := spnego.NewNegTokenInitKRB5(m.krbClient, ticket, key)
		if err != nil {
			return nil, err
		}

		m.stage++
		m.sessionkey = key
		return token.Marshal()
	case 1:
		var token gssapi.WrapToken
		if err := token.Unmarshal(challenge, true); err != nil {
			return nil, err
		}

		if _, err := token.Verify(m.sessionkey, keyusage.GSSAPI_ACCEPTOR_SEAL); err != nil {
			return nil, err
		}

		// sign the payload and send it back
		signed, err := gssapi.NewInitiatorWrapToken(token.Payload, m.sessionkey)
		if err != nil {
			return nil, err
		}

		m.stage++
		return signed.Marshal()
	case 2:
		var token gssapi.WrapToken
		if err := token.Unmarshal(challenge, true); err != nil {
			return nil, err
		}

		if _, err := token.Verify(m.sessionkey, keyusage.GSSAPI_ACCEPTOR_SEAL); err != nil {
			return nil, err
		}

		if len(token.Payload) != 4 {
			return nil, fmt.Errorf("expected decoded payload of length 4, got %d", len(token.Payload))
		}

		var err error

		qopBits := token.Payload[0]
		token.Payload[0] = 0
		m.serverMaxLength = int(binary.BigEndian.Uint32(token.Payload))
		m.qop, err = m.selectQop(qopBits)
		if err != nil {
			return nil, err
		}

		header := make([]byte, 4)
		maxLength := m.serverMaxLength
		if thrift.DEFAULT_MAX_FRAME_SIZE < m.serverMaxLength {
			maxLength = thrift.DEFAULT_MAX_FRAME_SIZE
		}

		headerInt := (uint(m.qop) << 24) | uint(maxLength)
		binary.BigEndian.PutUint32(header, uint32(headerInt))

		// FLAG_BYTE + 3 bytes of length + user or authority
		var name string
		if name = m.krbClient.Credentials.CName().PrincipalNameString(); m.Username != "" {
			name = m.Username
		}

		out := append(header, []byte(name)...)
		result, err := gssapi.NewInitiatorWrapToken(out, m.sessionkey)
		if err != nil {
			return nil, err
		}

		return result.Marshal()
	default:
		return nil, nil
	}
}

func (m *GSSAPIMechanism) Encode(data []byte) ([]byte, error) {
	if m.qop == byte(qopPriority[QopAuth]) {
		return data, nil
	}

	if m.qop == byte(qopPriority[QopPrivacy]) {
		encrypted, err := crypto.GetEncryptedData(data, m.sessionkey, keyusage.GSSAPI_INITIATOR_SEAL, m.kvno)
		if err != nil {
			return nil, err
		}

		data, err = encrypted.Marshal()
		if err != nil {
			return nil, err
		}
	}

	result, err := gssapi.NewInitiatorWrapToken(data, m.sessionkey)
	if err != nil {
		return nil, err
	}

	return result.Marshal()
}

func (m *GSSAPIMechanism) Decode(data []byte) ([]byte, error) {
	if m.qop == byte(qopPriority[QopAuth]) {
		return data, nil
	}

	var token gssapi.WrapToken
	if err := token.Unmarshal(data, true); err != nil {
		return nil, err
	}

	if m.qop == byte(qopPriority[QopPrivacy]) {
		decrypted, err := crypto.DecryptMessage(token.Payload, m.sessionkey, keyusage.GSSAPI_ACCEPTOR_SEAL)
		if err != nil {
			return nil, err
		}

		return decrypted, nil
	}

	_, err := token.Verify(m.sessionkey, keyusage.GSSAPI_ACCEPTOR_SEAL)
	if err != nil {
		return nil, err
	}

	return token.Payload, nil
}

func (m *GSSAPIMechanism) selectQop(qop byte) (byte, error) {
	available := m.SelectedQOP & qop
	for _, q := range []byte{byte(qopPriority[QopPrivacy]), byte(qopPriority[QopIntegrity]), byte(qopPriority[QopAuth])} {
		if available&q != 0 {
			return q, nil
		}
	}
	return 0, errors.New("no qop satisfying all conditions was found")
}

// replaceSPNHostWildcard substitutes the special string '_HOST' in the given
// SPN for the given (current) host.
func replaceSPNHostWildcard(spn, host string) string {
	res := krbSPNHost.FindStringSubmatchIndex(spn)
	if res == nil || res[2] == -1 {
		return spn
	}

	return spn[:res[2]] + host + spn[res[3]:]
}
