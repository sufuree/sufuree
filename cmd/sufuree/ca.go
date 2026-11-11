package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"github.com/sufuree/sufuree/internal/controlauth"
)

// runCA manages the controller CA. With --kms-key-id the CA key exists
// on disk only wrapped by that AWS KMS key (required for PHI); without it
// the key is a local 0600 file (development and local qualification).
// AWS credentials and region come from the standard AWS environment and
// shared config, never from flags.
//
//	sufuree ca init --dir D [--days 3650] [--kms-key-id alias/sufuree-ca]
//	sufuree ca issue --dir D --role R --name N --out PREFIX [--server-name a,b] [--days 30]
func runCA(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sufuree ca init|issue ...")
	}
	switch args[0] {
	case "init":
		fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
		dir := fs.String("dir", "", "CA directory (created 0700)")
		days := fs.Int("days", 3650, "CA validity in days")
		kmsKey := fs.String("kms-key-id", "", "AWS KMS key (ID, ARN or alias) that wraps the CA key; required for PHI. The key's ARN is recorded, so later issues don't depend on the alias")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *days <= 0 {
			return errors.New("--dir is required")
		}
		ctx := context.Background()
		var w controlauth.KeyWrapper
		if *kmsKey != "" {
			var err error
			if w, err = kmsWrapper(ctx, *kmsKey); err != nil {
				return err
			}
		}
		if err := controlauth.InitCAWrapped(ctx, *dir, time.Duration(*days)*24*time.Hour, w); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, "created", *dir+"/ca.crt")
		return nil
	case "issue":
		fs := flag.NewFlagSet("ca issue", flag.ContinueOnError)
		dir := fs.String("dir", "", "CA directory")
		role := fs.String("role", "", "host, operator, product-client or controller")
		name := fs.String("name", "", "principal name (a host's UID for role host)")
		out := fs.String("out", "", "output prefix: writes PREFIX.crt and PREFIX.key (0600)")
		servers := fs.String("server-name", "", "comma-separated DNS names or IPs (role controller only)")
		days := fs.Int("days", 30, "certificate validity in days")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *role == "" || *name == "" || *out == "" {
			return errors.New("--dir, --role, --name and --out are required")
		}
		ctx := context.Background()
		var w controlauth.KeyWrapper
		if controlauth.Wrapped(*dir) {
			id, err := controlauth.WrappedKeyID(*dir)
			if err != nil {
				return err
			}
			if w, err = kmsWrapper(ctx, id); err != nil {
				return err
			}
		}
		ca, err := controlauth.LoadCAWrapped(ctx, *dir, w)
		if err != nil {
			return err
		}
		var names []string
		if *servers != "" {
			names = strings.Split(*servers, ",")
		}
		is, err := ca.Issue(controlauth.Principal{Role: *role, Name: *name}, time.Duration(*days)*24*time.Hour, names)
		if err != nil {
			return err
		}
		if err := controlauth.WriteIssued(*out, is); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, "issued", *out+".crt")
		return nil
	default:
		return fmt.Errorf("unknown ca command %q", args[0])
	}
}

// runStoreCA manages the object store's CA (controlauth store.go): the
// private CA SeaweedFS's components, its operators and Sufuree's controller
// authenticate to each other with. It is kept like the controller CA, and
// each certificate is issued on its own, so one can be replaced alone.
//
//	sufuree store-ca init --dir D [--days 3650] [--kms-key-id alias/sufuree-store-ca]
//	sufuree store-ca issue --dir D --name N --out PREFIX [--server-name a,b] [--days 30]
//
// --name is the certificate's common name, which SeaweedFS admits gRPC
// peers by (master, volume, filer, s3, admin, ...). With --server-name the
// certificate is a component's, for serving and calling; without, a
// client's only.
func runStoreCA(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sufuree store-ca init|issue ...")
	}
	ctx := context.Background()
	switch args[0] {
	case "init":
		fs := flag.NewFlagSet("store-ca init", flag.ContinueOnError)
		dir := fs.String("dir", "", "CA directory (created 0700)")
		days := fs.Int("days", 3650, "CA validity in days")
		kmsKey := fs.String("kms-key-id", "", "AWS KMS key (ID, ARN or alias) that wraps the CA key; required for PHI. The key's ARN is recorded, so later issues don't depend on the alias")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" {
			return errors.New("--dir is required")
		}
		if *days <= 0 {
			return errors.New("--days must be positive")
		}
		if fs.NArg() > 0 {
			return fmt.Errorf("unexpected arguments %q", fs.Args())
		}
		var w controlauth.KeyWrapper
		if *kmsKey != "" {
			var err error
			if w, err = kmsWrapper(ctx, *kmsKey); err != nil {
				return err
			}
		} else {
			fmt.Fprintln(os.Stderr, "warning: without --kms-key-id the CA key is a plain file (ca.key); development only, never for PHI")
		}
		if err := controlauth.InitStoreCA(ctx, *dir, time.Duration(*days)*24*time.Hour, w); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, "created", *dir+"/ca.crt")
		return nil
	case "issue":
		fs := flag.NewFlagSet("store-ca issue", flag.ContinueOnError)
		dir := fs.String("dir", "", "CA directory")
		name := fs.String("name", "", "certificate common name: the component or client it authenticates")
		out := fs.String("out", "", "output prefix: writes PREFIX.crt and PREFIX.key (0600)")
		servers := fs.String("server-name", "", "comma-separated DNS names or IPs the component serves on; omit for a client-only certificate")
		days := fs.Int("days", 30, "certificate validity in days")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *name == "" || *out == "" {
			return errors.New("--dir, --name and --out are required")
		}
		if fs.NArg() > 0 {
			return fmt.Errorf("unexpected arguments %q", fs.Args())
		}
		var w controlauth.KeyWrapper
		if controlauth.Wrapped(*dir) {
			id, err := controlauth.WrappedKeyID(*dir)
			if err != nil {
				return err
			}
			if w, err = kmsWrapper(ctx, id); err != nil {
				return err
			}
		}
		ca, err := controlauth.LoadStoreCA(ctx, *dir, w)
		if err != nil {
			return err
		}
		var names []string
		if *servers != "" {
			for _, n := range strings.Split(*servers, ",") {
				names = append(names, strings.TrimSpace(n))
			}
		}
		is, err := ca.IssueStore(*name, time.Duration(*days)*24*time.Hour, names)
		if err != nil {
			return err
		}
		if err := controlauth.WriteIssued(*out, is); err != nil {
			return err
		}
		fmt.Fprintln(os.Stdout, "issued", *out+".crt")
		return nil
	default:
		return fmt.Errorf("unknown store-ca command %q", args[0])
	}
}

// kmsWrapper builds a KMS key wrapper from the standard AWS environment.
func kmsWrapper(ctx context.Context, key string) (controlauth.KeyWrapper, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if a, err := arn.Parse(key); err == nil && a.Region != "" {
		// A recorded key ARN names its region; use it whatever the
		// environment's default region is.
		opts = append(opts, awsconfig.WithRegion(a.Region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &controlauth.KMSWrapper{Client: kms.NewFromConfig(cfg), Key: key}, nil
}
