package util

import (
	"os"
	"path/filepath"
)

// GetIdentityPath returns the path to the identity files
func GetIdentityPath(filename string) string {
	wd, _ := os.Getwd()

	parent := filepath.Dir(wd)
	return filepath.Join(parent, "fixtures", "identity", filename)
}

// GetTLSPath returns the path to the tls files
func GetTLSPath(filename string) string {
	wd, _ := os.Getwd()
	parent := filepath.Dir(wd)
	return filepath.Join(parent, "fixtures", "tls", filename)
}
