// Package bootcred issues a workload's per-boot cloud credentials
// (workload.AWSRole): short-lived, scoped to the workload by session tags,
// and named for the workload and generation so each use is attributable.
// Credentials are returned for the boot env only; they are never logged.
package bootcred

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/sufuree/sufuree/internal/workload"
)

// Issuer returns boot env entries holding credentials for one boot.
type Issuer interface {
	Issue(ctx context.Context, w workload.Spec, generation uint64) (map[string]string, error)
}

// AssumeRoler is the STS call the issuer makes.
type AssumeRoler interface {
	AssumeRole(ctx context.Context, in *sts.AssumeRoleInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleOutput, error)
}

// STS issues credentials with sts:AssumeRole. NewClient builds a client for
// a region from the agent's own credentials.
type STS struct {
	NewClient func(region string) (AssumeRoler, error)

	mu      sync.Mutex
	clients map[string]AssumeRoler
}

func (s *STS) client(region string) (AssumeRoler, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[region]; ok {
		return c, nil
	}
	c, err := s.NewClient(region)
	if err != nil {
		return nil, err
	}
	if s.clients == nil {
		s.clients = map[string]AssumeRoler{}
	}
	s.clients[region] = c
	return c, nil
}

// SessionName is <id>-g<generation>, within STS's 64 characters.
func SessionName(id string, generation uint64) string {
	suffix := fmt.Sprintf("-g%d", generation)
	if len(id)+len(suffix) > 64 {
		id = id[:64-len(suffix)]
	}
	return id + suffix
}

// Issue assumes the workload's role for this generation.
func (s *STS) Issue(ctx context.Context, w workload.Spec, generation uint64) (map[string]string, error) {
	r := w.BootCredentials
	if r == nil {
		return nil, nil
	}
	c, err := s.client(r.Region)
	if err != nil {
		return nil, fmt.Errorf("boot credentials: %w", err)
	}
	d := r.Duration.Duration
	if d == 0 {
		d = 15 * time.Minute
	}
	keys := make([]string, 0, len(r.SessionTags))
	for k := range r.SessionTags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tags := make([]ststypes.Tag, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, ststypes.Tag{Key: aws.String(k), Value: aws.String(r.SessionTags[k])})
	}
	out, err := c.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(r.RoleARN),
		RoleSessionName: aws.String(SessionName(w.ID, generation)),
		DurationSeconds: aws.Int32(int32(d / time.Second)),
		Tags:            tags,
	})
	if err != nil {
		return nil, fmt.Errorf("boot credentials: assume %s: %w", r.RoleARN, err)
	}
	cr := out.Credentials
	if cr == nil || aws.ToString(cr.AccessKeyId) == "" || aws.ToString(cr.SecretAccessKey) == "" || aws.ToString(cr.SessionToken) == "" {
		return nil, errors.New("boot credentials: STS returned no credentials")
	}
	return map[string]string{
		r.Env.AccessKeyID:     aws.ToString(cr.AccessKeyId),
		r.Env.SecretAccessKey: aws.ToString(cr.SecretAccessKey),
		r.Env.SessionToken:    aws.ToString(cr.SessionToken),
	}, nil
}
