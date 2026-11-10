package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sufuree/sufuree/internal/controlauth"
)

// runCA manages the controller CA (local file custody; development and
// local qualification only until KMS custody lands).
//
//	sufuree ca init --dir D [--days 3650]
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
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dir == "" || *days <= 0 {
			return errors.New("--dir is required")
		}
		if err := controlauth.InitCA(*dir, time.Duration(*days)*24*time.Hour); err != nil {
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
		ca, err := controlauth.LoadCA(*dir)
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
