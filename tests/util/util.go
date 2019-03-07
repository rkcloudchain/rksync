package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// GetIdentityPath returns the path to the identity files
func GetIdentityPath(t *testing.T, filename string) string {
	wd, err := os.Getwd()
	require.NoError(t, err)

	parent := filepath.Dir(wd)
	return filepath.Join(parent, "fixtures", "identity", filename)
}
