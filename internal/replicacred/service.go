package replicacred

import (
	"context"
	"errors"
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

// Registrar registers an installation's identity (FilerRegistrar).
type Registrar interface {
	Register(ctx context.Context, bucket, installation string, doc []byte, c Credentials) error
}

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
		registered: map[string]time.Time{}, inflight: map[string]*call{}}, nil
}

// Issue returns installation's credentials, registered with the store. The
// caller must already have checked the requester may have them.
// Concurrent calls for one installation share one registration.
func (s *Service) Issue(ctx context.Context, installation string) (Credentials, error) {
	c, err := s.deriver.For(installation)
	if err != nil {
		return Credentials{}, err
	}
	s.mu.Lock()
	if at, ok := s.registered[installation]; ok && time.Since(at) < s.recheck {
		s.mu.Unlock()
		return c, nil
	}
	if cl, ok := s.inflight[installation]; ok {
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
	cl := &call{done: make(chan struct{})}
	s.inflight[installation] = cl
	s.mu.Unlock()

	cl.err = s.register(ctx, installation, c)
	s.mu.Lock()
	delete(s.inflight, installation)
	if cl.err == nil {
		s.registered[installation] = time.Now()
	}
	s.mu.Unlock()
	close(cl.done)
	if cl.err != nil {
		return Credentials{}, cl.err
	}
	return c, nil
}

func (s *Service) register(ctx context.Context, installation string, c Credentials) error {
	doc, err := Identity(s.bucket, installation, c)
	if err != nil {
		return err
	}
	return s.registrar.Register(ctx, s.bucket, installation, doc, c)
}
