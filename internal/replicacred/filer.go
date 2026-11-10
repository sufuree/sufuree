package replicacred

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// FilerRegistrar registers identities as SeaweedFS identity files,
// /etc/iam/identities/<name>.json, through the filer's HTTP API. The S3
// servers reload identities when that directory changes.
//
// The filer must keep these small files inline in its metadata
// (-filer.saveToFilerLimit at least 4096): the S3 servers' credential
// manager reads only inline content, sees a chunked file as empty, and
// then refuses every identity reload until that file is gone. Probe checks
// each registration took effect, and a registration that didn't is
// removed for that reason.
//
// The identity files hold secrets: the filer's HTTP API must require signed
// reads and writes (jwt.filer_signing.key and jwt.filer_signing.read.key
// in security.toml) and must not be reachable from guests.
type FilerRegistrar struct {
	// BaseURL is the filer, such as https://filer:8888.
	BaseURL string
	HTTP    *http.Client
	// WriteKey and ReadKey are the filer's JWT signing keys. A nil key
	// sends no token (development only).
	WriteKey, ReadKey []byte
	// Probe checks a registered identity's credentials work (S3Probe).
	Probe func(ctx context.Context, bucket, installation string, c Credentials) error
	// ProbeTimeout bounds how long Register waits for the S3 servers to
	// reload (default 10s).
	ProbeTimeout time.Duration
}

const identitiesDir = "/etc/iam/identities/"

func (f *FilerRegistrar) client() *http.Client {
	if f.HTTP != nil {
		return f.HTTP
	}
	return http.DefaultClient
}

func (f *FilerRegistrar) url(installation string) string {
	return strings.TrimRight(f.BaseURL, "/") + identitiesDir + url.PathEscape(IdentityName(installation)) + ".json"
}

func (f *FilerRegistrar) do(ctx context.Context, method, u string, key []byte, body []byte) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, err
	}
	if key != nil {
		tok, err := filerToken(key, time.Now())
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return f.client().Do(req)
}

// Current returns installation's registered identity file, or nil.
func (f *FilerRegistrar) Current(ctx context.Context, installation string) ([]byte, error) {
	res, err := f.do(ctx, http.MethodGet, f.url(installation), f.ReadKey, nil)
	if err != nil {
		return nil, fmt.Errorf("replicacred: read identity: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	switch {
	case err != nil:
		return nil, fmt.Errorf("replicacred: read identity: %w", err)
	case res.StatusCode == http.StatusNotFound:
		return nil, nil
	case res.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("replicacred: read identity: filer %s", res.Status)
	}
	return b, nil
}

// Register makes doc installation's identity file, unless it already is,
// and waits until its credentials c work. When they don't, the file is
// removed (see FilerRegistrar) and an error returned.
func (f *FilerRegistrar) Register(ctx context.Context, bucket, installation string, doc []byte, c Credentials) error {
	cur, err := f.Current(ctx, installation)
	if err != nil {
		return err
	}
	if cur != nil && sameJSON(cur, doc) {
		return f.probe(ctx, bucket, installation, c)
	}
	res, err := f.do(ctx, http.MethodPut, f.url(installation), f.WriteKey, doc)
	if err != nil {
		return fmt.Errorf("replicacred: write identity: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		return fmt.Errorf("replicacred: write identity: filer %s", res.Status)
	}
	if perr := f.probe(ctx, bucket, installation, c); perr != nil {
		if derr := f.remove(installation); derr != nil {
			return fmt.Errorf("%w; removing the identity also failed: %v", perr, derr)
		}
		return fmt.Errorf("%w (the identity was removed)", perr)
	}
	return nil
}

// remove deletes installation's identity file. It uses its own context: it
// cleans up after a probe that may have used up the caller's.
func (f *FilerRegistrar) remove(installation string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := f.do(ctx, http.MethodDelete, f.url(installation), f.WriteKey, nil)
	if err != nil {
		return err
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusNoContent && res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNotFound {
		return fmt.Errorf("filer %s", res.Status)
	}
	return nil
}

// probe retries f.Probe until it passes or ProbeTimeout ends: the S3
// servers reload identities asynchronously.
func (f *FilerRegistrar) probe(ctx context.Context, bucket, installation string, c Credentials) error {
	if f.Probe == nil {
		return errors.New("replicacred: the registrar has no probe")
	}
	d := f.ProbeTimeout
	if d <= 0 {
		d = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	wait := 100 * time.Millisecond
	for {
		err := f.Probe(ctx, bucket, installation, c)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("replicacred: the object store did not accept installation %s's credentials: %w", installation, err)
		case <-time.After(wait):
		}
		if wait < time.Second {
			wait *= 2
		}
	}
}

func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return bytes.Equal(xb, yb)
}

// filerToken is a SeaweedFS filer JWT: HS256 with a short expiry.
func filerToken(key []byte, now time.Time) (string, error) {
	enc := base64.RawURLEncoding
	h := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	p, err := json.Marshal(map[string]int64{"exp": now.Add(time.Minute).Unix()})
	if err != nil {
		return "", err
	}
	in := h + "." + enc.EncodeToString(p)
	m := hmac.New(sha256.New, key)
	m.Write([]byte(in))
	return in + "." + enc.EncodeToString(m.Sum(nil)), nil
}
