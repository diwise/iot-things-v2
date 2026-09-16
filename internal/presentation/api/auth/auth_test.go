package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/matryer/is"
)

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

// THINGS-002: errors wrapped with %w must preserve the chain, so
// errors.Is/As keep working across the presentation layer.
func TestNewAuthenticatorReadErrorWrapsCause(t *testing.T) {
	is := is.New(t)

	_, err := NewAuthenticator(context.Background(), errReader{})
	is.True(err != nil)
	is.Equal(err.Error(), "unable to read authz policies: read failed")
	is.True(errors.Unwrap(err) != nil)
	is.Equal(errors.Unwrap(err).Error(), "read failed")
}
