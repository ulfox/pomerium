package oauth

import (
	"bytes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/pomerium/pomerium/pkg/cryptutil"
)

// StatePrefix is the prefix used to indicate the state is via pomerium.
const StatePrefix = "POMERIUM-"

// State is the state in the oauth query string.
type State struct {
	Nonce       string
	Timestamp   time.Time
	RedirectURL string
}

// NewState creates a new State.
func NewState(redirectURL string) *State {
	return &State{
		Nonce:       uuid.NewString(),
		Timestamp:   time.Now(),
		RedirectURL: redirectURL,
	}
}

// DecodeState decodes state from a raw state string.
func DecodeState(aead cipher.AEAD, rawState string) (*State, error) {
	withoutPrefix := strings.TrimPrefix(rawState, StatePrefix)
	rawStateBytes, err := base64.RawURLEncoding.DecodeString(withoutPrefix)
	if err != nil {
		return nil, fmt.Errorf("invalid state encoding: %w", err)
	}

	// split the state into its components
	state := bytes.SplitN(rawStateBytes, []byte{'|'}, 3)
	if len(state) != 3 {
		return nil, fmt.Errorf("invalid state format")
	}

	// verify that the returned timestamp is valid
	if err := cryptutil.ValidTimestamp(string(state[1])); err != nil {
		return nil, fmt.Errorf("invalid state timestamp: %w", err)
	}

	nonce := string(state[0])
	timestamp, err := strconv.ParseInt(string(state[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid state timestamp: %w", err)
	}

	ad := []byte(fmt.Sprintf("%s|%d|", nonce, timestamp))
	decrypted, err := cryptutil.Decrypt(aead, state[2], ad)
	if err != nil {
		return nil, fmt.Errorf("invalid state redirect URL: %w", err)
	}

	return &State{
		Nonce:       nonce,
		Timestamp:   time.Unix(timestamp, 0),
		RedirectURL: string(decrypted),
	}, nil
}

// Encode encodes the state.
func (state *State) Encode(aead cipher.AEAD) string {
	timestamp := state.Timestamp.Unix()
	ad := []byte(fmt.Sprintf("%s|%d|", state.Nonce, timestamp))
	encrypted := cryptutil.Encrypt(aead, []byte(state.RedirectURL), ad)
	return StatePrefix + base64.RawURLEncoding.EncodeToString(append(ad, encrypted...))
}
