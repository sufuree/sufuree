package wakehint

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Runs against a real S3-compatible store when SUFUREE_TEST_S3_ENDPOINT,
// SUFUREE_TEST_S3_BUCKET, SUFUREE_HINT_ACCESS_KEY and SUFUREE_HINT_SECRET_KEY
// are set.
func TestReadAgainstStore(t *testing.T) {
	ep, bucket := os.Getenv("SUFUREE_TEST_S3_ENDPOINT"), os.Getenv("SUFUREE_TEST_S3_BUCKET")
	if ep == "" || bucket == "" {
		t.Skip("no test object store configured")
	}
	var ca []byte
	if f := os.Getenv("SUFUREE_TEST_S3_CA_FILE"); f != "" {
		var err error
		if ca, err = os.ReadFile(f); err != nil {
			t.Fatal(err)
		}
	}
	r, err := NewS3(ep, "", bucket, os.Getenv("SUFUREE_HINT_ACCESS_KEY"), os.Getenv("SUFUREE_HINT_SECRET_KEY"), ca)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = r.c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &bucket})
	prefix := "hint-test/" + time.Now().Format("150405.000000000")
	if _, ok, err := r.Read(ctx, prefix); err != nil || ok {
		t.Fatalf("missing hint: ok=%v err=%v", ok, err)
	}
	key := prefix + "/wake.json"
	body := `{"version":1,"generation":3,"busy":true,"next_due":"2030-01-01T00:00:00Z","updated_at":"2026-11-07T00:00:00Z"}`
	if _, err := r.c.PutObject(ctx, &awss3.PutObjectInput{Bucket: &bucket, Key: &key, Body: strings.NewReader(body)}); err != nil {
		t.Fatal(err)
	}
	h, ok, err := r.Read(ctx, prefix)
	if err != nil || !ok || h.Generation != 3 || !h.Busy || h.NextDue == nil || h.NextDue.Year() != 2030 {
		t.Fatalf("hint = %+v ok=%v err=%v", h, ok, err)
	}
}

func TestNewS3RejectsAnEmptyCA(t *testing.T) {
	if _, err := NewS3("https://s3.example", "", "b", "a", "s", []byte("not pem")); err == nil {
		t.Fatal("a CA file with no certificate was accepted")
	}
}
