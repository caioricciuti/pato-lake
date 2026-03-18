package handlers

import "testing"

func TestUserRateLimitKey(t *testing.T) {
	k1 := userRateLimitKey("Default")
	k2 := userRateLimitKey("default")

	if k1 != k2 {
		t.Fatalf("userRateLimitKey should normalize case: got %q vs %q", k1, k2)
	}

	if k1 != "user:default" {
		t.Fatalf("unexpected normalized key: %s", k1)
	}
}

func TestNormalizeUsername(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Admin", "admin"},
		{"  user  ", "user"},
		{"CamelCase", "camelcase"},
		{"", ""},
	}

	for _, tc := range tests {
		got := normalizeUsername(tc.input)
		if got != tc.want {
			t.Fatalf("normalizeUsername(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
