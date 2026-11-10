package bootcred

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootscope"
	"github.com/sufuree/sufuree/internal/resource"
)

func TestGuardedIssuePinsControllerTags(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/hosts/h1/credentials/inst-1", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"installation": "inst-1",
			"session_tags": map[string]string{"replica": "inst/inst-1", "a": "b"},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	fake := &fakeSTS{}
	inner := &STS{NewClient: func(string) (AssumeRoler, error) { return fake, nil }}
	g := &Guarded{
		Inner:  inner,
		Client: &AssignmentClient{BaseURL: srv.URL, HostUID: "h1", HTTP: srv.Client()},
	}
	env, err := g.Issue(context.Background(), spec(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if env["AK"] == "" || len(fake.in) != 1 {
		t.Fatalf("env=%v calls=%d", env, len(fake.in))
	}
	var replica string
	for _, tag := range fake.in[0].Tags {
		if awsString(tag.Key) == "replica" {
			replica = awsString(tag.Value)
		}
	}
	if replica != "inst/inst-1" {
		t.Fatalf("replica tag = %q", replica)
	}
}

func TestAssignmentClientFetch(t *testing.T) {
	as := []bootscope.Assignment{{
		Placement: resource.Placement{
			UID: "p1", HostUID: "h1", Replica: "inst-1", Phase: resource.PhaseReady,
		},
		SessionTags: map[string]string{"replica": "inst/inst-1"},
	}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/hosts/h1/assignments", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(as)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := &AssignmentClient{BaseURL: srv.URL, HostUID: "h1", HTTP: srv.Client()}
	got, err := c.FetchAssignments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Placement.Replica != "inst-1" {
		t.Fatalf("got %+v", got)
	}
}

func TestGuardedRefusesForeignInstallation(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/hosts/h1/credentials/inst-1", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, bootscope.ErrNotHeld.Error(), http.StatusForbidden)
	})
	mux.HandleFunc("GET /v1/hosts/h1/assignments", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]bootscope.Assignment{})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	fake := &fakeSTS{}
	g := &Guarded{
		Inner:  &STS{NewClient: func(string) (AssumeRoler, error) { return fake, nil }},
		Client: &AssignmentClient{BaseURL: srv.URL, HostUID: "h1", HTTP: srv.Client()},
	}
	_, err := g.Issue(context.Background(), spec(), 1)
	if err == nil {
		t.Fatal("expected refusal")
	}
	if !errors.Is(err, ErrAuthorizeDenied) {
		t.Fatalf("err = %v, want ErrAuthorizeDenied", err)
	}
	if len(fake.in) != 0 {
		t.Fatalf("STS called despite refusal: %d", len(fake.in))
	}
}

func TestGuardedDenyDoesNotUseStaleCache(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/hosts/h1/credentials/inst-1", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, bootscope.ErrNotHeld.Error(), http.StatusForbidden)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	fake := &fakeSTS{}
	g := &Guarded{
		Inner:  &STS{NewClient: func(string) (AssumeRoler, error) { return fake, nil }},
		Client: &AssignmentClient{BaseURL: srv.URL, HostUID: "h1", HTTP: srv.Client()},
	}
	// Poison the cache with an assignment the controller has since revoked.
	g.assignments = []bootscope.Assignment{{
		Placement: resource.Placement{
			UID: "p1", HostUID: "h1", Replica: "inst-1", Phase: resource.PhaseReady,
		},
		SessionTags: map[string]string{"replica": "inst/inst-1"},
	}}
	g.fetchedAt = time.Now()
	_, err := g.Issue(context.Background(), spec(), 1)
	if !errors.Is(err, ErrAuthorizeDenied) {
		t.Fatalf("err = %v", err)
	}
	if len(fake.in) != 0 {
		t.Fatalf("STS called after deny: %d", len(fake.in))
	}
}

func awsString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

var _ Issuer = (*Guarded)(nil)

func TestReplicaCredentialsClient(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		ok     bool
		denied bool
	}{
		{"issued", 200, `{"installation":"inst-1","session_tags":{"replica":"inst/inst-1"},"replica":{"access_key_id":"SFRA","secret_access_key":"s"}}`, true, false},
		{"controller without replica credentials", 200, `{"installation":"inst-1","session_tags":{"replica":"inst/inst-1"}}`, false, false},
		{"empty secret", 200, `{"replica":{"access_key_id":"SFRA","secret_access_key":""}}`, false, false},
		{"not held", 403, `bootscope: host does not hold this installation`, false, true},
		{"unavailable", 503, `replica credentials: filer down`, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/hosts/h1/credentials/inst-1" {
					http.NotFound(w, r)
					return
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			c := &AssignmentClient{BaseURL: srv.URL, HostUID: "h1", HTTP: srv.Client()}
			got, err := c.ReplicaCredentials(context.Background(), "inst-1")
			if tc.ok != (err == nil) || (tc.ok && (got.AccessKeyID != "SFRA" || got.SecretAccessKey != "s")) {
				t.Fatalf("got %+v, %v", got, err)
			}
			if errors.Is(err, ErrAuthorizeDenied) != tc.denied {
				t.Fatalf("denied = %v, want %v (%v)", errors.Is(err, ErrAuthorizeDenied), tc.denied, err)
			}
		})
	}
}
