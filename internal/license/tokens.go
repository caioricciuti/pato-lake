package license

import (
	"strings"

	"github.com/google/uuid"
)

// GenerateSessionToken generates a session token
func GenerateSessionToken() string {
	u1 := uuid.New().String()
	u2 := strings.ReplaceAll(uuid.New().String(), "-", "")
	return u1 + u2
}
