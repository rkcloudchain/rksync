/*
Copyright Rockontrol Corp. All Rights Reserved.
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package lib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	s := NewSet()
	assert.Len(t, s.ToArray(), 0)
	assert.Equal(t, 0, s.Size())
	assert.False(t, s.Exists(42))

	s.Add(42)
	assert.True(t, s.Exists(42))
	assert.Len(t, s.ToArray(), 1)
	assert.Equal(t, 1, s.Size())

	s.Remove(42)
	assert.False(t, s.Exists(42))
	assert.Equal(t, 0, s.Size())

	s.Add(42)
	assert.True(t, s.Exists(42))

	s.Clear()
	assert.False(t, s.Exists(42))
	assert.Equal(t, 0, s.Size())
}
