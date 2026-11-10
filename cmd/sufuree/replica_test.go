package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, dir, name, body string, mode os.FileMode) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestReplicaConfigFailsClosedOutsideDevelopment(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "key.json")
	if err := runReplicaKey([]string{"init", "--out", key}); err != nil {
		t.Fatal(err)
	}
	jwt := writeFile(t, dir, "jwt", "0123456789abcdef0123", 0o600)
	open := writeFile(t, dir, "jwt-open", "0123456789abcdef0123", 0o644)
	cfg := func(dev bool, filer, s3, w, r string) string {
		b := `{"key_file":"` + key + `","key_version":1,"bucket":"zati",
		  "filer":{"url":"` + filer + `","write_jwt_key_file":"` + w + `","read_jwt_key_file":"` + r + `"},
		  "s3":{"url":"` + s3 + `"},"development":` + map[bool]string{true: "true", false: "false"}[dev] + `}`
		return writeFile(t, t.TempDir(), "replica.json", b, 0o600)
	}
	ctx := context.Background()
	cases := []struct {
		name, path, want string
	}{
		{"plain http", cfg(false, "http://f", "https://s", jwt, jwt), "https"},
		{"unsigned filer", cfg(false, "https://f", "https://s", "", jwt), "jwt"},
		{"plaintext key", cfg(false, "https://f", "https://s", jwt, jwt), "KMS-wrapped"},
		{"world-readable jwt key", cfg(true, "http://f", "http://s", open, ""), "readable only by its owner"},
		{"unknown field", writeFile(t, dir, "bad.json", `{"key_file":"`+key+`","key_version":1,"bucket":"zati","filer":{"url":"x"},"s3":{"url":"y"},"prefix":"p"}`, 0o600), "unknown field"},
		{"no version", writeFile(t, dir, "nov.json", `{"key_file":"`+key+`","bucket":"zati","filer":{"url":"x"},"s3":{"url":"y"},"development":true}`, 0o600), "key_version"},
	}
	for _, tc := range cases {
		if _, err := loadReplicaService(ctx, tc.path); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error mentioning %q", tc.name, err, tc.want)
		}
	}
	if _, err := loadReplicaService(ctx, cfg(true, "http://f", "http://s", "", "")); err != nil {
		t.Fatalf("development config: %v", err)
	}
}

func TestControllerRefusesReplicaCredentialsWithoutTLS(t *testing.T) {
	dir := t.TempDir()
	err := runController([]string{"--data-dir", dir, "--listen", "127.0.0.1:0", "--replica-credentials", filepath.Join(dir, "none.json")})
	if err == nil || !strings.Contains(err.Error(), "--tls-cert") {
		t.Fatalf("err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "control.db")); !os.IsNotExist(err) {
		t.Fatal("the refused controller created a store")
	}
}
