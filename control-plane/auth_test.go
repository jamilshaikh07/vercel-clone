package main

import "testing"

func TestParseAllowlist(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"jamilshaikh07", []string{"jamilshaikh07"}},
		{"Alice, Bob\nCarol;dave eve", []string{"alice", "bob", "carol", "dave", "eve"}},
		{"  Mixed,Case  ", []string{"mixed", "case"}},
	}
	for _, c := range cases {
		got := parseAllowlist(c.in)
		if len(got) != len(c.want) {
			t.Errorf("parseAllowlist(%q) size = %d, want %d (%v)", c.in, len(got), len(c.want), got)
			continue
		}
		for _, w := range c.want {
			if !got[w] {
				t.Errorf("parseAllowlist(%q) missing %q (got %v)", c.in, w, got)
			}
		}
	}
}

func TestLoginAllowed(t *testing.T) {
	// Empty allowlist => gate disabled, everyone allowed (never lock out owner).
	open := &authConfig{allowedLogins: parseAllowlist("")}
	if !open.loginAllowed("anyone") {
		t.Error("empty allowlist must allow everyone")
	}

	gated := &authConfig{allowedLogins: parseAllowlist("jamilshaikh07, friend")}
	if !gated.loginAllowed("jamilshaikh07") {
		t.Error("listed login must be allowed")
	}
	if !gated.loginAllowed("JamilShaikh07") {
		t.Error("allowlist must be case-insensitive")
	}
	if !gated.loginAllowed("  friend  ") {
		t.Error("login comparison must trim whitespace")
	}
	if gated.loginAllowed("cryptominer") {
		t.Error("unlisted login must be blocked")
	}
}
