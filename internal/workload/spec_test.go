package workload

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const good = `{"version":1,"data_dir":"/d","socket_dir":"/s",
 "wake_hints":{"endpoint":"http://127.0.0.1:8333","bucket":"b"},
 "workloads":[{"id":"inst-1","image":"/i.img","memory_mib":512,"guest_port":8080,"state_mib":512,
  "boot_env_file":"zati.env","boot_env":{"K":"env:SECRET"},"generation_env_key":"ZATI_REPLICA_GENERATION",
  "hint_prefix":"inst/inst-1","idle_timeout":"60s","startup_timeout":"20s","scheduled_lead":"5s","min_awake":"30s"}]}`

func write(t *testing.T, s string) string {
	p := filepath.Join(t.TempDir(), "h.json")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndResolve(t *testing.T) {
	c, err := Load(write(t, good))
	if err != nil {
		t.Fatal(err)
	}
	w := c.Workloads[0]
	env, err := w.ResolveBootEnv(7, func(k string) (string, bool) { return "v-" + k, k == "SECRET" })
	if err != nil || env["K"] != "v-SECRET" || env["ZATI_REPLICA_GENERATION"] != "7" {
		t.Fatalf("env = %v, %v", env, err)
	}
	if _, err := w.ResolveBootEnv(1, func(string) (string, bool) { return "", false }); err == nil {
		t.Fatal("missing env secret accepted")
	}
}

func TestLoadRefuses(t *testing.T) {
	for name, s := range map[string]string{
		"unknown field": strings.Replace(good, `"version":1`, `"version":1,"extra":true`, 1),
		"bad version":   strings.Replace(good, `"version":1`, `"version":2`, 1),
		"bad id":        strings.Replace(good, `"inst-1"`, `"Inst/1"`, 1),
		"path env file": strings.Replace(good, `"zati.env"`, `"../zati.env"`, 1),
		"no hints":      strings.Replace(good, `"wake_hints":{"endpoint":"http://127.0.0.1:8333","bucket":"b"},`, ``, 1),
	} {
		if _, err := Load(write(t, s)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
