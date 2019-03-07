package identity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	vid := idMapper.GetPKIidOfCert(selfIdentity)
	require.NotNil(t, vid)

	signed, err := idMapper.Sign([]byte("bla bla"))
	assert.NoError(t, err)
	assert.NoError(t, idMapper.Verify(vid, signed, []byte("bla bla")))
}
