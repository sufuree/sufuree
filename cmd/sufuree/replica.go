package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/replicacred"
)

// runReplicaKey creates the provisioning key replica credentials are
// derived from (package replicacred). With --kms-key-id it exists on disk
// only wrapped by that AWS KMS key (required for PHI).
//
//	sufuree replica-key init --out FILE [--kms-key-id alias/sufuree-replica-credentials]
func runReplicaKey(args []string) error {
	if len(args) == 0 || args[0] != "init" {
		return errors.New("usage: sufuree replica-key init --out FILE [--kms-key-id KEY]")
	}
	fs := flag.NewFlagSet("replica-key init", flag.ContinueOnError)
	out := fs.String("out", "", "key file to create (0600)")
	kmsKey := fs.String("kms-key-id", "", "AWS KMS key (ID, ARN or alias) that wraps the provisioning key; required for PHI. The key's ARN is recorded")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	ctx := context.Background()
	var w replicacred.Wrapper
	if *kmsKey != "" {
		kw, err := kmsWrapper(ctx, *kmsKey)
		if err != nil {
			return err
		}
		w = kw
	}
	if err := replicacred.InitKey(ctx, *out, w); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "created", *out)
	return nil
}

// replicaConfig is the controller's --replica-credentials file.
type replicaConfig struct {
	// KeyFile is the provisioning key (sufuree replica-key init).
	KeyFile string `json:"key_file"`
	// KeyVersion is raised to rotate every installation's credentials.
	KeyVersion uint32 `json:"key_version"`
	Bucket     string `json:"bucket"`
	Filer      struct {
		URL    string `json:"url"`
		CAFile string `json:"ca_file"`
		// WriteJWTKeyFile and ReadJWTKeyFile hold the filer's
		// jwt.filer_signing.key and jwt.filer_signing.read.key.
		WriteJWTKeyFile string `json:"write_jwt_key_file"`
		ReadJWTKeyFile  string `json:"read_jwt_key_file"`
	} `json:"filer"`
	S3 struct {
		URL    string `json:"url"`
		CAFile string `json:"ca_file"`
		Region string `json:"region"`
	} `json:"s3"`
	// Development allows a plaintext key, plain HTTP and an unsigned filer.
	// Never for PHI.
	Development bool `json:"development"`
}

// loadReplicaService builds the controller's replica credential service.
// Outside development it requires a KMS-wrapped key, HTTPS to the filer and
// S3, and signed filer reads and writes.
func loadReplicaService(ctx context.Context, path string) (*replicacred.Service, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg replicaConfig
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("--replica-credentials: %w", err)
	}
	switch {
	case cfg.KeyFile == "" || cfg.KeyVersion == 0 || cfg.Bucket == "" || cfg.Filer.URL == "" || cfg.S3.URL == "":
		return nil, errors.New("--replica-credentials needs key_file, key_version, bucket, filer.url and s3.url")
	case !cfg.Development && (!strings.HasPrefix(cfg.Filer.URL, "https://") || !strings.HasPrefix(cfg.S3.URL, "https://")):
		return nil, errors.New("--replica-credentials: filer.url and s3.url must be https (or set development)")
	case !cfg.Development && (cfg.Filer.WriteJWTKeyFile == "" || cfg.Filer.ReadJWTKeyFile == ""):
		return nil, errors.New("--replica-credentials: the filer's identity files hold secrets; set write_jwt_key_file and read_jwt_key_file (or set development)")
	}
	blob, err := controlauth.ReadPrivate(cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	kid, err := replicacred.KeyID(blob)
	if err != nil {
		return nil, err
	}
	var u replicacred.Unwrapper
	switch {
	case kid != "":
		kw, err := kmsWrapper(ctx, kid)
		if err != nil {
			return nil, err
		}
		u = kw
	case !cfg.Development:
		return nil, errors.New("--replica-credentials: the provisioning key must be KMS-wrapped (or set development)")
	}
	key, err := replicacred.LoadKey(ctx, blob, u)
	if err != nil {
		return nil, err
	}
	d, err := replicacred.NewDeriver(key, cfg.KeyVersion)
	clear(key)
	if err != nil {
		return nil, err
	}
	filerHTTP, err := httpClientWithCA(cfg.Filer.CAFile)
	if err != nil {
		return nil, err
	}
	s3HTTP, err := httpClientWithCA(cfg.S3.CAFile)
	if err != nil {
		return nil, err
	}
	reg := &replicacred.FilerRegistrar{BaseURL: cfg.Filer.URL, HTTP: filerHTTP,
		Probe: replicacred.S3Probe(cfg.S3.URL, cfg.S3.Region, s3HTTP)}
	if reg.WriteKey, err = readJWTKey(cfg.Filer.WriteJWTKeyFile); err != nil {
		return nil, err
	}
	if reg.ReadKey, err = readJWTKey(cfg.Filer.ReadJWTKeyFile); err != nil {
		return nil, err
	}
	return replicacred.NewService(d, reg, cfg.Bucket, 0)
}

func readJWTKey(path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	b, err := controlauth.ReadPrivate(path)
	if err != nil {
		return nil, err
	}
	k := []byte(strings.TrimSpace(string(b)))
	if len(k) < 16 {
		return nil, fmt.Errorf("%s: a filer JWT key must be at least 16 bytes", path)
	}
	return k, nil
}

// httpClientWithCA trusts only caFile's certificates for this client, or
// the system roots when caFile is empty.
func httpClientWithCA(caFile string) (*http.Client, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s holds no certificate", caFile)
		}
		tr.TLSClientConfig.RootCAs = pool
	}
	return &http.Client{Transport: tr, Timeout: 15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}
