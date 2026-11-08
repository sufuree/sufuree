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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// NewS3 builds a reader. Credentials are passed in, never logged.
func NewS3(endpoint, region, bucket, accessKey, secretKey string) *S3 {
	if region == "" {
		region = "us-east-1"
	}
	c := awss3.New(awss3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	})
	return &S3{c: c, bucket: bucket}
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
