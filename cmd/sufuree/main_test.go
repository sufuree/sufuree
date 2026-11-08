package main

import "testing"

func TestOneAgentPerDataDir(t *testing.T) {
	dir := t.TempDir()
	unlock, err := lockHost(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockHost(dir); err == nil {
		t.Fatal("a second agent took the same data_dir")
	}
	unlock()
	again, err := lockHost(dir)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	again()
}
