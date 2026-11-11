package replicacred

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Probe returns a Probe that lists installation's own prefix with its
// credentials: it passes once the store knows the credentials and they
// reach that prefix.
func S3Probe(endpoint, region string, hc *http.Client) func(ctx context.Context, bucket, installation string, c Credentials) error {
	if region == "" {
		region = "us-east-1"
	}
	return func(ctx context.Context, bucket, installation string, c Credentials) error {
		opts := awss3.Options{
			Region:       region,
			BaseEndpoint: aws.String(endpoint),
			UsePathStyle: true,
			Credentials:  credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, ""),
		}
		if hc != nil {
			opts.HTTPClient = hc
		}
		_, err := awss3.New(opts).ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(Prefix(installation) + "/"),
			MaxKeys: aws.Int32(1),
		})
		return err
	}
}

// Registrar registers and removes an installation's identity
// (FilerRegistrar).
type Registrar interface {
	Register(ctx context.Context, bucket, installation string, doc []byte, c Credentials) error
	Remove(ctx context.Context, installation string) error
}

// ErrRevoked means the installation's identity was revoked.
var ErrRevoked = errors.New("replicacred: the installation's credentials were revoked")

// Service issues installations' credentials on the controller, registering
// each before it is first returned.
type Service struct {
	deriver   *Deriver
	registrar Registrar
	bucket    string
	// recheck is how long a registration is trusted before Issue checks it
	// again (default 10m), so an identity removed from the store comes back.
	recheck time.Duration

	mu         sync.Mutex
	registered map[string]time.Time
	inflight   map[string]*call
	// revoked holds installations Revoke was called for. Issue refuses them
	// for the life of the process; after a restart the controller's
	// placements (Issue's caller checks them) and its revocation records
	// keep them out.
	revoked map[string]bool
}

type call struct {
	done chan struct{}
	err  error
}

// NewService builds a Service for bucket. recheck <= 0 means 10m.
func NewService(d *Deriver, r Registrar, bucket string, recheck time.Duration) (*Service, error) {
	if d == nil || r == nil {
		return nil, errors.New("replicacred: a service needs a deriver and a registrar")
	}
	if _, err := Identity(bucket, "probe", Credentials{}); err != nil {
		return nil, err
	}
	if recheck <= 0 {
		recheck = 10 * time.Minute
	}
	return &Service{deriver: d, registrar: r, bucket: bucket, recheck: recheck,
		registered: map[string]time.Time{}, inflight: map[string]*call{}, revoked: map[string]bool{}}, nil
}

// RegisterTimeout bounds one registration, including the probe.
const RegisterTimeout = 30 * time.Second

// Issue returns installation's credentials, registered with the store. The
// caller must already have checked the requester may have them.
// Concurrent calls for one installation share one registration. It runs
// under its own timeout, not the first caller's context, so a caller that
// gives up neither fails the others nor leaves a half-done registration.
func (s *Service) Issue(ctx context.Context, installation string) (Credentials, error) {
	c, err := s.deriver.For(installation)
	if err != nil {
		return Credentials{}, err
	}
	s.mu.Lock()
	if s.revoked[installation] {
		s.mu.Unlock()
		return Credentials{}, ErrRevoked
	}
	if at, ok := s.registered[installation]; ok && time.Since(at) < s.recheck {
		s.mu.Unlock()
		return c, nil
	}
	cl, ok := s.inflight[installation]
	if !ok {
		cl = &call{done: make(chan struct{})}
		s.inflight[installation] = cl
		go s.run(context.WithoutCancel(ctx), installation, c, cl)
	}
	s.mu.Unlock()
	select {
	case <-cl.done:
	case <-ctx.Done():
		return Credentials{}, ctx.Err()
	}
	if cl.err != nil {
		return Credentials{}, cl.err
	}
	return c, nil
}

// run registers installation once and releases every waiter. It ends
// within RegisterTimeout.
func (s *Service) run(ctx context.Context, installation string, c Credentials, cl *call) {
	ctx, cancel := context.WithTimeout(ctx, RegisterTimeout)
	defer cancel()
	err := s.register(ctx, installation, c)
	s.mu.Lock()
	cl.err = err
	delete(s.inflight, installation)
	if err == nil {
		s.registered[installation] = time.Now()
	}
	s.mu.Unlock()
	close(cl.done)
}

func (s *Service) register(ctx context.Context, installation string, c Credentials) error {
	doc, err := Identity(s.bucket, installation, c)
	if err != nil {
		return err
	}
	return s.registrar.Register(ctx, s.bucket, installation, doc, c)
}

// Revoke removes installation's identity from the store, and Issue refuses
// it from then on. A registration already running finishes first, so it
// can't write the identity back afterwards. Revoking again repeats the
// removal; a missing identity is not an error.
func (s *Service) Revoke(ctx context.Context, installation string) error {
	if err := ValidInstallation(installation); err != nil {
		return err
	}
	s.mu.Lock()
	s.revoked[installation] = true
	delete(s.registered, installation)
	cl := s.inflight[installation]
	s.mu.Unlock()
	if cl != nil {
		select {
		case <-cl.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := s.registrar.Remove(ctx, installation); err != nil {
		return fmt.Errorf("replicacred: revoke %s: %w", installation, err)
	}
	return nil
}
