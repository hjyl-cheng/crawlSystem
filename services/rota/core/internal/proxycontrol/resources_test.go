package proxycontrol

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestPrepareManagedPasswordCachesPreparedRotation(t *testing.T) {
	m := &Manager{
		options:                Options{WorkerPassword: "worker-password"},
		managedCredentialCache: make(map[string]managedCredentialCacheEntry),
	}

	first, firstMatches, err := m.prepareManagedPassword("bullmq-channel-01", "$2a$04$invalid-but-stable-observed-hash")
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || firstMatches {
		t.Fatalf("unexpected first preflight result: hash=%q matches=%v", first, firstMatches)
	}
	second, secondMatches, err := m.prepareManagedPassword("bullmq-channel-01", "$2a$04$invalid-but-stable-observed-hash")
	if err != nil {
		t.Fatal(err)
	}
	if second != first || secondMatches {
		t.Fatalf("cached preflight changed: first=(%q,%v), second=(%q,%v)", first, firstMatches, second, secondMatches)
	}
}

func TestPrepareManagedPasswordReusesUnchangedBcryptHash(t *testing.T) {
	m := &Manager{
		options:                Options{WorkerPassword: "worker-password"},
		managedCredentialCache: make(map[string]managedCredentialCacheEntry),
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(m.options.WorkerPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	observed := string(hash)

	first, firstMatches, err := m.prepareManagedPassword("bullmq-channel-02", observed)
	if err != nil || !firstMatches || first != observed {
		t.Fatalf("matching preflight failed: hash=%q matches=%v err=%v", first, firstMatches, err)
	}
	second, secondMatches, err := m.prepareManagedPassword("bullmq-channel-02", observed)
	if err != nil || !secondMatches || second != observed {
		t.Fatalf("cached matching preflight failed: hash=%q matches=%v err=%v", second, secondMatches, err)
	}
}
