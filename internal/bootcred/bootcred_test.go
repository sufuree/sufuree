package bootcred

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"

	"github.com/sufuree/sufuree/internal/workload"
)

type fakeSTS struct {
	in  []*sts.AssumeRoleInput
	err error
}

func (f *fakeSTS) AssumeRole(_ context.Context, in *sts.AssumeRoleInput, _ ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	f.in = append(f.in, in)
	if f.err != nil {
		return nil, f.err
	}
	return &sts.AssumeRoleOutput{Credentials: &ststypes.Credentials{
		AccessKeyId: aws.String("ASIA" + aws.ToString(in.RoleSessionName)), SecretAccessKey: aws.String("sk"), SessionToken: aws.String("tok"),
	}}, nil
}

func spec() workload.Spec {
	return workload.Spec{ID: "inst-1", BootCredentials: &workload.AWSRole{
		RoleARN: "arn:aws:iam::123456789012:role/guest", Region: "us-west-1",
		SessionTags: map[string]string{"replica": "inst/inst-1", "a": "b"},
		Env:         workload.AWSRoleEnv{AccessKeyID: "AK", SecretAccessKey: "SK", SessionToken: "ST"},
	}}
}

func TestIssueScopesAndNamesTheSession(t *testing.T) {
	f := &fakeSTS{}
	var regions []string
	s := &STS{NewClient: func(r string) (AssumeRoler, error) { regions = append(regions, r); return f, nil }}
	for _, gen := range []uint64{4, 5} {
		env, err := s.Issue(context.Background(), spec(), gen)
		if err != nil {
			t.Fatal(err)
		}
		if env["AK"] != "ASIAinst-1-g"+string(rune('0'+gen)) || env["SK"] != "sk" || env["ST"] != "tok" || len(env) != 3 {
			t.Fatalf("env = %v", env)
		}
	}
	in := f.in[1]
	if aws.ToString(in.RoleSessionName) != "inst-1-g5" || aws.ToInt32(in.DurationSeconds) != int32((15*time.Minute)/time.Second) {
		t.Fatalf("session %q duration %d", aws.ToString(in.RoleSessionName), aws.ToInt32(in.DurationSeconds))
	}
	if len(in.Tags) != 2 || aws.ToString(in.Tags[0].Key) != "a" || aws.ToString(in.Tags[1].Value) != "inst/inst-1" {
		t.Fatalf("tags = %+v", in.Tags)
	}
	if in.Policy != nil || len(in.TransitiveTagKeys) != 0 {
		t.Fatal("unexpected session policy or transitive tags")
	}
	if len(regions) != 1 || regions[0] != "us-west-1" {
		t.Fatalf("clients built for %v (want one, cached)", regions)
	}
}

func TestIssueFailsClosed(t *testing.T) {
	s := &STS{NewClient: func(string) (AssumeRoler, error) { return &fakeSTS{err: errors.New("AccessDenied")}, nil }}
	if env, err := s.Issue(context.Background(), spec(), 1); err == nil || env != nil {
		t.Fatalf("env=%v err=%v", env, err)
	}
	empty := &STS{NewClient: func(string) (AssumeRoler, error) { return emptySTS{}, nil }}
	if _, err := empty.Issue(context.Background(), spec(), 1); err == nil {
		t.Fatal("empty STS credentials accepted")
	}
}

type emptySTS struct{}

func (emptySTS) AssumeRole(context.Context, *sts.AssumeRoleInput, ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	return &sts.AssumeRoleOutput{Credentials: &ststypes.Credentials{}}, nil
}

func TestSessionNameFits(t *testing.T) {
	id := strings.Repeat("a", 63)
	n := SessionName(id, 1<<62)
	if len(n) != 64 || !strings.HasSuffix(n, "-g4611686018427387904") {
		t.Fatalf("%q (%d)", n, len(n))
	}
}
