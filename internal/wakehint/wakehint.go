// Package wakehint reads the hint a stateful workload publishes so the host
// can decide, without waking the guest, whether it may stay asleep: whether
// it has in-flight work ("busy") and when its next scheduled work is due.
//
// The hint is advisory for scheduling, never for safety: a guest that is
// stopped while busy loses no acknowledged writes (its writes are durable
// before acknowledgement) and resumes the work on its next boot.
package wakehint

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Hint is the published document (version 1).
type Hint struct {
	Version    int        `json:"version"`
	Generation uint64     `json:"generation"`
	Busy       bool       `json:"busy"`
	NextDue    *time.Time `json:"next_due,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
	// Progress, when present, is the workload's durable position (see
	// Progress). Optional.
	Progress *Progress `json:"progress,omitempty"`
}

// Progress is a position a workload reports as durably committed, ordered
// by Epoch then Seq. Epoch must be a generation the host issued (such as
// the generation whose data is committed); Seq is the workload's. The host
// ignores progress naming a generation it never issued. It keeps the
// highest it has seen and passes it to the next boot (progress_env_key), so
// the workload can refuse its own store if that store was rolled back
// behind it. The host cannot authenticate it: a forged higher value can
// stop the workload from booting, never make it accept less.
type Progress struct {
	Epoch uint64 `json:"epoch"`
	Seq   uint64 `json:"seq"`
}

// Less orders positions by epoch, then sequence.
func (p Progress) Less(q Progress) bool {
	return p.Epoch < q.Epoch || (p.Epoch == q.Epoch && p.Seq < q.Seq)
}

// Reader fetches hints.
type Reader interface {
	Read(ctx context.Context, prefix string) (Hint, bool, error)
}

// S3 reads <prefix>/wake.json from an S3-compatible store.
type S3 struct {
	c      *awss3.Client
	bucket string
}

// NewS3 builds a reader. Credentials are passed in, never logged. caPEM,
// when set, is trusted in addition to the system roots.
func NewS3(endpoint, region, bucket, accessKey, secretKey string, caPEM []byte) (*S3, error) {
	if region == "" {
		region = "us-east-1"
	}
	o := awss3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	if len(caPEM) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("wake hints: ca_file holds no PEM certificate")
		}
		o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		})
	}
	return &S3{c: awss3.New(o), bucket: bucket}, nil
}

// Read returns the hint, or ok=false when none has been published.
func (s *S3) Read(ctx context.Context, prefix string) (Hint, bool, error) {
	key := prefix + "/wake.json"
	out, err := s.c.GetObject(ctx, &awss3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && (ae.ErrorCode() == "NoSuchKey" || ae.ErrorCode() == "NotFound") {
			return Hint{}, false, nil
		}
		return Hint{}, false, err
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(out.Body, 4096))
	if err != nil {
		return Hint{}, false, err
	}
	var h Hint
	if err := json.Unmarshal(data, &h); err != nil || h.Version != 1 {
		return Hint{}, false, fmt.Errorf("wakehint: %s is not a version 1 hint", key)
	}
	return h, true, nil
}
