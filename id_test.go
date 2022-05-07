package GRING_test

import (
	"fmt"
	"github.com/theued/GRING"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"strconv"
	"testing"
	"testing/quick"
)

func TestID_String(t *testing.T) {
	t.Parallel()

	f := func(publicKey GRING.PublicKey, host net.IP, port uint16) bool {
		if host.IsLoopback() || host.IsUnspecified() { // Make-shift 'normalizeIP(net.IP)'.
			host = nil
		}

		h := host.String()
		if h == "<nil>" {
			h = ""
		}

		id := GRING.NewID(publicKey, host, port)

		if !assert.Equal(t,
			fmt.Sprintf(
				`{"public_key": "%s", "address": "%s"}`,
				publicKey, net.JoinHostPort(h, strconv.FormatUint(uint64(port), 10)),
			),
			id.String(),
		) {
			return false
		}

		return true
	}

	assert.NoError(t, quick.Check(f, nil))
}

func TestUnmarshalID(t *testing.T) {
	t.Parallel()

	_, err := GRING.UnmarshalID(nil)
	assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())

	_, err = GRING.UnmarshalID(append(GRING.ZeroPublicKey[:], 1))
	assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())

	_, err = GRING.UnmarshalID(append(GRING.ZeroPublicKey[:], append(net.IPv6loopback, 1)...))
	assert.EqualError(t, err, io.ErrUnexpectedEOF.Error())

	_, err = GRING.UnmarshalID(append(GRING.ZeroPublicKey[:], append(net.IPv6loopback, 1, 2)...))
	assert.NoError(t, err)
}
