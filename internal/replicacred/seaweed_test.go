package replicacred

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// TestSeaweedFS runs against a real SeaweedFS (4.48 or later) whose filer
// keeps small files inline (-filer.saveToFilerLimit) and signs reads and
// writes (security.toml). The bucket must exist. Set:
//
//	SUFUREE_SEAWEED_FILER, SUFUREE_SEAWEED_S3, SUFUREE_SEAWEED_BUCKET,
//	SUFUREE_SEAWEED_FILER_WRITE_KEY, SUFUREE_SEAWEED_FILER_READ_KEY
func TestSeaweedFS(t *testing.T) {
	filer, s3url, bucket := os.Getenv("SUFUREE_SEAWEED_FILER"), os.Getenv("SUFUREE_SEAWEED_S3"), os.Getenv("SUFUREE_SEAWEED_BUCKET")
	if filer == "" || s3url == "" || bucket == "" {
		t.Skip("set SUFUREE_SEAWEED_FILER, SUFUREE_SEAWEED_S3 and SUFUREE_SEAWEED_BUCKET to run against SeaweedFS")
	}
	ctx := context.Background()
	reg := &FilerRegistrar{BaseURL: filer, Probe: S3Probe(s3url, "", nil),
		WriteKey: []byte(os.Getenv("SUFUREE_SEAWEED_FILER_WRITE_KEY")), ReadKey: []byte(os.Getenv("SUFUREE_SEAWEED_FILER_READ_KEY"))}
	suffix := time.Now().Format("150405.000000")
	a, b := "it-a-"+suffix, "it-b-"+suffix
	d1, _ := NewDeriver(testKey(7), 1)
	svc, err := NewService(d1, reg, bucket, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := svc.Issue(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := svc.Issue(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reg.remove(a)
		_ = reg.remove(b)
	})
	client := func(c Credentials) *awss3.Client {
		return awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(s3url), UsePathStyle: true,
			Credentials: credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.SecretAccessKey, "")})
	}
	put := func(c Credentials, key string, cond bool) error {
		in := &awss3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader([]byte("x"))}
		if cond {
			in.IfNoneMatch = aws.String("*")
		}
		_, err := client(c).PutObject(ctx, in)
		return err
	}
	get := func(c Credentials, key string) error {
		out, err := client(c).GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err == nil {
			_ = out.Body.Close()
		}
		return err
	}
	code := func(err error) string {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			return ae.ErrorCode()
		}
		if err == nil {
			return "OK"
		}
		return err.Error()
	}
	own := Prefix(a) + "/lease.json"
	if err := put(ca, own, true); err != nil {
		t.Fatalf("own conditional put: %v", err)
	}
	if err := get(ca, own); err != nil {
		t.Fatalf("own get: %v", err)
	}
	if err := put(cb, Prefix(b)+"/x", false); err != nil {
		t.Fatalf("b's own put: %v", err)
	}
	for name, err := range map[string]error{
		"a reads b":           get(ca, Prefix(b)+"/x"),
		"b reads a":           get(cb, own),
		"b writes a":          put(cb, Prefix(a)+"/evil", false),
		"a writes a neighbor": put(ca, Prefix(a)+"x/evil", false),
	} {
		if c := code(err); c != "AccessDenied" {
			t.Errorf("%s: %s, want AccessDenied", name, c)
		}
	}
	for name, prefix := range map[string]*string{"the bucket": nil, "b's prefix": aws.String(Prefix(b) + "/")} {
		_, err = client(ca).ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: prefix})
		if c := code(err); c != "AccessDenied" {
			t.Errorf("a lists %s: %s, want AccessDenied", name, c)
		}
	}
	// A dot-dot key is refused outright, whatever it would resolve to.
	if c := code(get(ca, Prefix(a)+"/../"+b+"/x")); c == "OK" {
		t.Errorf("a read b's object through a .. key")
	}

	// Rotation: the next version's registration revokes the old key.
	d2, _ := NewDeriver(testKey(7), 2)
	svc2, _ := NewService(d2, reg, bucket, time.Hour)
	ca2, err := svc2.Issue(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if err := get(ca2, own); err != nil {
		t.Fatalf("rotated key: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c := code(get(ca, own))
		if c == "InvalidAccessKeyId" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the old key still gets %s after rotation", c)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Signed reads: an unsigned read of an identity file is refused.
	unsigned := &FilerRegistrar{BaseURL: filer}
	if _, err := unsigned.Current(ctx, a); err == nil || !strings.Contains(err.Error(), "401") {
		t.Errorf("an unsigned read of an identity file: %v, want 401", err)
	}
}
