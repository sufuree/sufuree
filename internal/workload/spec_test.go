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
		"unknown field":          strings.Replace(good, `"version":1`, `"version":1,"extra":true`, 1),
		"bad version":            strings.Replace(good, `"version":1`, `"version":2`, 1),
		"bad id":                 strings.Replace(good, `"inst-1"`, `"Inst/1"`, 1),
		"path env file":          strings.Replace(good, `"zati.env"`, `"../zati.env"`, 1),
		"bad boot file":          strings.Replace(good, `"boot_env":{`, `"boot_files":{"zati.env":"x"},"boot_env":{`, 1),
		"no hints":               strings.Replace(good, `"wake_hints":{"endpoint":"http://127.0.0.1:8333","bucket":"b"},`, ``, 1),
		"progress without hints": strings.Replace(good, `"hint_prefix":"inst/inst-1",`, `"progress_env_key":"MIN",`, 1),
		"progress is generation": strings.Replace(good, `"hint_prefix":"inst/inst-1",`, `"hint_prefix":"inst/inst-1","progress_env_key":"ZATI_REPLICA_GENERATION",`, 1),
		"progress in boot_env":   strings.Replace(good, `"hint_prefix":"inst/inst-1",`, `"hint_prefix":"inst/inst-1","progress_env_key":"K",`, 1),
	} {
		if _, err := Load(write(t, s)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

const creds = `"boot_credentials":{"role_arn":"arn:aws:iam::123456789012:role/guest","region":"us-west-1",
  "session_tags":{"replica":"inst/inst-1"},"duration":"15m",
  "env":{"access_key_id":"AK","secret_access_key":"SK","session_token":"ST"}},`

func TestBootCredentials(t *testing.T) {
	withCreds := strings.Replace(good, `"boot_env":{`, creds+`"boot_env":{`, 1)
	c, err := Load(write(t, withCreds))
	if err != nil {
		t.Fatal(err)
	}
	if r := c.Workloads[0].BootCredentials; r == nil || r.SessionTags["replica"] != "inst/inst-1" || r.Env.SessionToken != "ST" {
		t.Fatalf("boot_credentials = %+v", r)
	}
	for name, s := range map[string]string{
		"no tags":            strings.Replace(withCreds, `{"replica":"inst/inst-1"}`, `{}`, 1),
		"bad tag":            strings.Replace(withCreds, `"inst/inst-1"`, `"inst#1"`, 1),
		"not a role":         strings.Replace(withCreds, `:role/guest`, `:user/guest`, 1),
		"no region":          strings.Replace(withCreds, `"region":"us-west-1",`, ``, 1),
		"short":              strings.Replace(withCreds, `"15m"`, `"5m"`, 1),
		"long":               strings.Replace(withCreds, `"15m"`, `"2h"`, 1),
		"dup env key":        strings.Replace(withCreds, `"session_token":"ST"`, `"session_token":"AK"`, 1),
		"env collision":      strings.Replace(withCreds, `"session_token":"ST"`, `"session_token":"K"`, 1),
		"progress collision": strings.Replace(withCreds, `"hint_prefix":"inst/inst-1",`, `"hint_prefix":"inst/inst-1","progress_env_key":"ST",`, 1),
		"gen collision":      strings.Replace(withCreds, `"session_token":"ST"`, `"session_token":"ZATI_REPLICA_GENERATION"`, 1),
		"unknown field":      strings.Replace(withCreds, `"region":"us-west-1",`, `"region":"us-west-1","policy":"*",`, 1),
	} {
		if _, err := Load(write(t, s)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	ca := strings.Replace(good, `"bucket":"b"}`, `"bucket":"b","ca_file":"/etc/zati/ca.pem"}`, 1)
	if c, err := Load(write(t, ca)); err != nil || c.WakeHints.CAFile != "/etc/zati/ca.pem" {
		t.Fatalf("ca_file: %v %+v", err, c.WakeHints)
	}
}

func TestRefusesInjection(t *testing.T) {
	withCreds := strings.Replace(good, `"boot_env":{`, creds+`"boot_env":{`, 1)
	for name, s := range map[string]string{
		"comma in image":     strings.Replace(good, `"/i.img"`, `"/i.img,snapshot=off"`, 1),
		"comma in data_dir":  strings.Replace(good, `"data_dir":"/d"`, `"data_dir":"/d,x"`, 1),
		"relative data_dir":  strings.Replace(good, `"data_dir":"/d"`, `"data_dir":"d"`, 1),
		"generation key '='": strings.Replace(good, `"ZATI_REPLICA_GENERATION"`, `"A=B"`, 1),
		"long id with creds": strings.Replace(strings.Replace(withCreds, `"inst-1"`, `"`+strings.Repeat("a", 43)+`"`, 1), `"inst/inst-1","idle`, `"inst/x","idle`, 1),
	} {
		if _, err := Load(write(t, s)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	c, err := Load(write(t, good))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a\nZATI_REPLICA_GENERATION=1", "a\rb"} {
		if _, err := c.Workloads[0].ResolveBootEnv(1, func(string) (string, bool) { return v, true }); err == nil {
			t.Errorf("multi-line resolved value %q accepted", v)
		}
	}
}
