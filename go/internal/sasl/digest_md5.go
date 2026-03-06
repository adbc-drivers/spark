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
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/rc4"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"regexp"
	"sort"
	"strings"
)

const (
	// QopAuth only provides auth, no confidentiality
	QopAuth = "auth"
	// QopIntegrity provides auth and integrity protection
	QopIntegrity = "auth-int"
	// QopPrivacy provides auth and confidentiality protection,
	// the most secure option
	QopPrivacy = "auth-conf"
)

var qopPriority = map[string]int8{
	QopPrivacy:   4,
	QopIntegrity: 2,
	QopAuth:      1,
}

type Qops []string

func (q Qops) Len() int { return len(q) }
func (q Qops) Less(i, j int) bool {
	ps1, ok := qopPriority[q[i]]
	if !ok {
		ps1 = -1 // treat unknown qops as lowest priority
	}
	ps2, ok := qopPriority[q[j]]
	if !ok {
		ps2 = -1 // treat unknown qops as lowest priority
	}
	return ps1 > ps2
}
func (q Qops) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

var challengeRegexp = regexp.MustCompile(",?([a-zA-Z0-9]+)=(\"([^\"]+)\"|([^,]+)),?")

type challenge struct {
	realm   string
	nonce   string
	qop     Qops
	charset string
	cipher  []string
	algo    string
}

func parseChallenge(input []byte) (*challenge, error) {
	ch := challenge{}

	matched := challengeRegexp.FindAllSubmatch(input, -1)
	if matched == nil {
		return nil, fmt.Errorf("invalid challenge format: %s", input)
	}

	for _, m := range matched {
		key := string(m[1])
		val := string(m[3])
		switch key {
		case "realm":
			ch.realm = val
		case "nonce":
			ch.nonce = val
		case "qop":
			ch.qop = strings.Split(val, ",")
		case "charset":
			ch.charset = val
		case "cipher":
			ch.cipher = strings.Split(val, ",")
		case "algorithm":
			ch.algo = val
		default:
		}
	}

	if len(ch.qop) == 0 {
		return nil, errors.New("invalid token challenge: no selected QOP")
	}

	sort.Sort(ch.qop)
	return &ch, nil
}

const (
	saslIntegrityPrefixLen = 4
	macDataLen             = 4
	macHMACLen             = 10
	macMsgTypeLen          = 2
	macSeqNumLen           = 4
)

var macMsgType = [2]byte{0x00, 0x01}

func genNonce() (string, error) {
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(nonce), nil
}

func chooseCipher(options []string) string {
	s := make(map[string]bool)
	for _, c := range options {
		s[c] = true
	}

	// TODO: Support 3DES

	switch {
	case s["rc4"]:
		return "rc4"
	case s["rc4-56"]:
		return "rc4-56"
	case s["rc4-40"]:
		return "rc4-40"
	default:
		return ""
	}
}

type md5Encoder interface {
	encode(data []byte) ([]byte, error)
	decode(data []byte) ([]byte, error)
}

type DigestMD5Mechanism struct {
	Service  string
	Username string
	Password string

	authID   []byte
	hostname string

	token  *challenge
	cnonce string
	cipher string

	encoder md5Encoder
}

func (m *DigestMD5Mechanism) Name() string {
	return "DIGEST-MD5"
}

func (m *DigestMD5Mechanism) SetHost(host string) {
	m.hostname = host
}

func (m *DigestMD5Mechanism) Start() ([]byte, error) {
	return m.Step(nil)
}

func (m *DigestMD5Mechanism) Encode(data []byte) ([]byte, error) {
	if m.encoder == nil {
		return data, nil
	}
	return m.encoder.encode(data)
}

func (m *DigestMD5Mechanism) Decode(data []byte) ([]byte, error) {
	if m.encoder == nil {
		return data, nil
	}
	return m.encoder.decode(data)
}

func (m *DigestMD5Mechanism) Step(challenge []byte) ([]byte, error) {
	if challenge == nil {
		return nil, nil
	}

	// step 1
	if m.token == nil {
		var err error
		m.token, err = parseChallenge(challenge)
		if err != nil {
			return nil, err
		}

		m.cnonce, err = genNonce()
		if err != nil {
			return nil, fmt.Errorf("failed to generate nonce: %w", err)
		}

		m.cipher = chooseCipher(m.token.cipher)
		rspdigest := m.compute(true)

		ret := fmt.Appendf(nil, `username="%s", realm="%s", nonce="%s", cnonce="%s", nc=%08x, qop=%s, digest-uri="%s/%s", response=%s, charset=utf-8`,
			m.authID, m.token.realm, m.token.nonce, m.cnonce, len(m.cnonce), m.token.qop[0], m.Service, m.hostname, rspdigest)
		return ret, nil
	}

	// step 2
	rspauth := strings.Split(string(challenge), "=")
	if rspauth[0] != "rspauth" {
		return nil, fmt.Errorf("invalid challenge response: %s", challenge)
	}

	if rspauth[1] != m.compute(false) {
		return nil, errors.New("rspauth did not match digest")
	}

	var privacy, integrity bool
	switch m.token.qop[0] {
	case QopPrivacy:
		privacy, integrity = true, true
	case QopIntegrity:
		privacy, integrity = false, true
	default:
	}

	// auth done, nothing left to do
	if !privacy && !integrity {
		return nil, nil
	}

	kic, kis := generateIntegrityKeys(m.a1())
	if !privacy {
		m.encoder = &integrityEncoder{
			encodeMAC: hmac.New(md5.New, kic[:]),
			decodeMAC: hmac.New(md5.New, kis[:]),
		}
		return nil, nil
	}

	if m.cipher == "" {
		return nil, fmt.Errorf("no available cipher among choices: %v", m.token.cipher)
	}

	kcc, kcs := generatePrivacyKeys(m.a1(), m.cipher)
	encryptor, err := rc4.NewCipher(kcc[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create rc4 encryptor: %w", err)
	}

	decryptor, err := rc4.NewCipher(kcs[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create rc4 decryptor: %w", err)
	}

	m.encoder = &privacyEncoder{
		encryptor: encryptor,
		decryptor: decryptor,
		encodeMAC: hmac.New(md5.New, kic[:]),
		decodeMAC: hmac.New(md5.New, kis[:]),
	}
	return nil, nil
}

// compute implements the computation of md5 digest authentication per RFC 2831.
// The response value computation is defined as:
//
//	    HEX(KD(HEX(H(A1)),
//	      { nonce-value, ":", nc-value, ":", cnonce-value, ":", qop-value,
//	        ":", HEX(H(A2)) }))
//	    A1 = { H({ username-value, ":", realm-value, ":", passwd }),
//	           ":", nonce-value, ":", cnonce-value }
//
//	  If "qop" is "auth":
//
//			 A2 = { "AUTHENTICATE:", digest-uri-value }
//
//	  If "qop" is "auth-int" or "auth-conf":
//
//	      A2 = { "AUTHENTICATE:", digest-uri-value,
//	             ":00000000000000000000000000000000" }
//
//	  Where:
//
//	    - { a, b, ... } is the concatenation of the octet strings a, b, ...
//	    - H(s) is the 16 octet MD5 Hash [RFC1321] of the octet string s
//	    - KD(k, s) is H({k, ":", s})
//	    - HEX(n) is the representation of the 16 octet MD5 hash n as a string of
//	      32 hex digits (with alphabetic characters in lower case)
func (m *DigestMD5Mechanism) compute(initial bool) string {
	x := hex.EncodeToString(h(m.a1()))
	y := strings.Join([]string{
		m.token.nonce,
		fmt.Sprintf("%08x", len(m.cnonce)),
		m.cnonce,
		m.token.qop[0],
		hex.EncodeToString(h(m.a2(initial))),
	}, ":")

	return hex.EncodeToString(kd(x, y))
}

func (m *DigestMD5Mechanism) a1() string {
	x := h(strings.Join([]string{string(m.authID), m.token.realm, m.Password}, ":"))
	return strings.Join([]string{string(x[:]), m.token.nonce, m.cnonce}, ":")
}

func (m *DigestMD5Mechanism) a2(initial bool) string {
	digestURI := m.Service + "/" + m.hostname
	var a2 string

	// when validating the server's response-auth, we need to leave out the "AUTHENTICATE:" prefix
	if initial {
		a2 = strings.Join([]string{"AUTHENTICATE", digestURI}, ":")
	} else {
		a2 = ":" + digestURI
	}

	if m.token.qop[0] == QopPrivacy || m.token.qop[0] == QopIntegrity {
		a2 += ":00000000000000000000000000000000"
	}

	return a2
}

func h(s string) []byte {
	hash := md5.Sum([]byte(s))
	return hash[:]
}

func kd(k, s string) []byte {
	return h(k + ":" + s)
}

func generateIntegrityKeys(a1 string) ([md5.Size]byte, [md5.Size]byte) {
	const clientIntMagicStr = "Digest session key to client-to-server signing key magic constant"
	const serverIntMagicStr = "Digest session key to server-to-client signing key magic constant"

	sum := h(a1)
	return md5.Sum(append(sum[:], clientIntMagicStr...)), md5.Sum(append(sum[:], serverIntMagicStr...))
}

func generatePrivacyKeys(a1 string, cipher string) ([md5.Size]byte, [md5.Size]byte) {
	sum := h(a1)
	var n int
	switch cipher {
	case "rc4-40":
		n = 5
	case "rc4-56":
		n = 7
	default:
		n = md5.Size
	}

	kcc := md5.Sum(append(sum[:n], []byte("Digest H(A1) to client-toserver sealing key magic constant")...))
	kcs := md5.Sum(append(sum[:n], []byte("Digest H(A1) to server-to-client sealing key magic constant")...))
	return kcc, kcs
}

func lenEncodeBytes(seqnum int) (out [4]byte) {
	out[0] = byte((seqnum >> 24) & 0xFF)
	out[1] = byte((seqnum >> 16) & 0xFF)
	out[2] = byte((seqnum >> 8) & 0xFF)
	out[3] = byte(seqnum & 0xFF)
	return
}

type integrityEncoder struct {
	writeBuf bytes.Buffer

	sendSeqNum int
	readSeqNum int

	encodeMAC hash.Hash
	decodeMAC hash.Hash
}

func (e *integrityEncoder) encode(data []byte) ([]byte, error) {
	inputLen := len(data)
	seqBuf := lenEncodeBytes(e.sendSeqNum)
	outputLen := macDataLen + inputLen + macHMACLen + macMsgTypeLen + macSeqNumLen

	e.writeBuf.Reset()
	e.writeBuf.Grow(outputLen)

	if err := binary.Write(&e.writeBuf, binary.BigEndian, int32(outputLen-macDataLen)); err != nil {
		return nil, fmt.Errorf("failed to write output length: %w", err)
	}

	if _, err := e.writeBuf.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data: %w", err)
	}

	hmac := msgHMAC(e.encodeMAC, seqBuf, data)
	if _, err := e.writeBuf.Write(hmac); err != nil {
		return nil, fmt.Errorf("failed to write HMAC: %w", err)
	}

	if _, err := e.writeBuf.Write(macMsgType[:]); err != nil {
		return nil, fmt.Errorf("failed to write message type: %w", err)
	}

	if err := binary.Write(&e.writeBuf, binary.BigEndian, int32(e.sendSeqNum)); err != nil {
		return nil, fmt.Errorf("failed to write sequence number: %w", err)
	}

	e.sendSeqNum++
	return e.writeBuf.Bytes(), nil
}

func (e *integrityEncoder) decode(data []byte) ([]byte, error) {
	inputLen := len(data)
	if inputLen < saslIntegrityPrefixLen {
		return nil, fmt.Errorf("input data too short: %d bytes", inputLen)
	}

	seqBuf := lenEncodeBytes(e.readSeqNum)
	dataLen := inputLen - macHMACLen - macMsgTypeLen - macSeqNumLen
	hmac := msgHMAC(e.decodeMAC, seqBuf, data[:dataLen])

	seqNumStart := inputLen - macSeqNumLen
	msgTypeStart := seqNumStart - macMsgTypeLen
	origHashStart := msgTypeStart - macHMACLen

	if !bytes.Equal(hmac, data[origHashStart:origHashStart+macHMACLen]) ||
		!bytes.Equal(macMsgType[:], data[msgTypeStart:msgTypeStart+macMsgTypeLen]) ||
		!bytes.Equal(seqBuf[:], data[seqNumStart:seqNumStart+macSeqNumLen]) {
		return nil, errors.New("HMAC integrity check failed")
	}

	e.readSeqNum++
	return data[:dataLen], nil
}

type privacyEncoder struct {
	sendSeqNum int
	readSeqNum int

	decodeMAC hash.Hash
	encodeMAC hash.Hash

	decryptor *rc4.Cipher
	encryptor *rc4.Cipher

	writeBuf bytes.Buffer
}

func (e *privacyEncoder) encode(data []byte) ([]byte, error) {
	inputLen := len(data)
	seqBuf := lenEncodeBytes(e.sendSeqNum)

	encryptedLen := inputLen + macHMACLen
	outputLen := macDataLen + encryptedLen + macMsgTypeLen + macSeqNumLen
	e.writeBuf.Reset()
	e.writeBuf.Grow(outputLen)

	finalLength := encryptedLen + macMsgTypeLen + macSeqNumLen
	if err := binary.Write(&e.writeBuf, binary.BigEndian, int32(finalLength)); err != nil {
		return nil, fmt.Errorf("failed to write output length: %w", err)
	}

	if _, err := e.writeBuf.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write data to buffer: %w", err)
	}

	hmac := msgHMAC(e.encodeMAC, seqBuf, data)
	if _, err := e.writeBuf.Write(hmac); err != nil {
		return nil, fmt.Errorf("failed to write HMAC: %w", err)
	}

	toEncrypt := e.writeBuf.Bytes()[macDataLen:]
	// encrypt in place
	e.encryptor.XORKeyStream(toEncrypt, toEncrypt)
	if _, err := e.writeBuf.Write(macMsgType[:]); err != nil {
		return nil, fmt.Errorf("failed to write message type: %w", err)
	}

	if err := binary.Write(&e.writeBuf, binary.BigEndian, int32(e.sendSeqNum)); err != nil {
		return nil, fmt.Errorf("failed to write sequence number: %w", err)
	}

	e.sendSeqNum++
	return e.writeBuf.Bytes(), nil
}

func (e *privacyEncoder) decode(data []byte) ([]byte, error) {
	inputLen := len(data)
	if inputLen < saslIntegrityPrefixLen {
		return nil, fmt.Errorf("input data too short: %d bytes", inputLen)
	}

	seqNumStart := inputLen - macSeqNumLen
	msgTypeStart := seqNumStart - macMsgTypeLen

	encryptedLen := inputLen - macMsgTypeLen - macSeqNumLen
	// decrypt in place
	e.decryptor.XORKeyStream(data[:encryptedLen], data[:encryptedLen])

	origHash := data[encryptedLen-macHMACLen : encryptedLen]
	encryptedLen -= macHMACLen

	seqBuf := lenEncodeBytes(e.readSeqNum)
	hmac := msgHMAC(e.decodeMAC, seqBuf, data[:encryptedLen])

	msgType := data[msgTypeStart : msgTypeStart+macMsgTypeLen]
	seqNum := data[seqNumStart : seqNumStart+macSeqNumLen]

	if !bytes.Equal(hmac, origHash) || !bytes.Equal(macMsgType[:], msgType) || !bytes.Equal(seqNum, seqBuf[:]) {
		return nil, errors.New("HMAC integrity check failed")
	}

	e.readSeqNum++
	return data[:encryptedLen], nil
}

// msgHMAC implements the HMAC wrapper per the RFC:
//
//	HMAC(ki, {seqnum, msg})[0..9].
func msgHMAC(mac hash.Hash, seq [4]byte, msg []byte) []byte {
	mac.Reset()
	mac.Write(seq[:])
	mac.Write(msg)

	return mac.Sum(nil)[:10]
}
