package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/resource"
)

func runApp(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sufuree app apply --file spec.json --controller URL | sufuree app get NAME --controller URL")
	}
	switch args[0] {
	case "apply":
		return applyApp(args[1:])
	case "get":
		return getApp(args[1:])
	default:
		return fmt.Errorf("unknown app command %q", args[0])
	}
}

func applyApp(args []string) error {
	fs := flag.NewFlagSet("app apply", flag.ContinueOnError)
	file := fs.String("file", "", "app resource JSON")
	controller := fs.String("controller", "", "control API base URL")
	tlsf := tlsFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" || *controller == "" {
		return errors.New("--file and --controller are required")
	}
	if err := useControlClient(*tlsf, *controller); err != nil {
		return err
	}
	body, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var res resource.Resource
	if err := resource.Decode(body, &res); err != nil {
		return err
	}
	if res.Kind == "" {
		res.Kind = resource.KindApp
	}
	if res.Kind != resource.KindApp {
		return errors.New("app apply accepts kind App")
	}
	if res.Name == "" {
		return errors.New("app name is required")
	}
	base := strings.TrimRight(*controller, "/")
	url := base + "/v1/resources/App/" + res.Name
	current, status, err := httpJSON(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK:
		var got resource.Resource
		if err := resource.Decode(current, &got); err != nil {
			return err
		}
		res.ResourceVersion = got.ResourceVersion
		res.UID = got.UID
	case http.StatusNotFound:
		res.ResourceVersion = 0
	default:
		return fmt.Errorf("controller get: %s", strings.TrimSpace(string(current)))
	}
	out, status, err := putResource(url, res)
	if err != nil {
		return err
	}
	if status == http.StatusConflict && res.ResourceVersion != 0 {
		current, status, err = httpJSON(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("controller get: %s", strings.TrimSpace(string(current)))
		}
		var got resource.Resource
		if err := resource.Decode(current, &got); err != nil {
			return err
		}
		res.ResourceVersion = got.ResourceVersion
		res.UID = got.UID
		out, status, err = putResource(url, res)
		if err != nil {
			return err
		}
	}
	if status != http.StatusOK {
		return fmt.Errorf("controller apply: %s", strings.TrimSpace(string(out)))
	}
	return printResource(out)
}

func getApp(args []string) error {
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		args = args[1:]
	}
	fs := flag.NewFlagSet("app get", flag.ContinueOnError)
	controller := fs.String("controller", "", "control API base URL")
	tlsf := tlsFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := useControlClient(*tlsf, *controller); err != nil {
		return err
	}
	if name == "" && fs.NArg() == 1 {
		name = fs.Arg(0)
	}
	if name == "" || *controller == "" || fs.NArg() > 1 {
		return errors.New("usage: sufuree app get NAME --controller URL")
	}
	url := strings.TrimRight(*controller, "/") + "/v1/resources/App/" + name
	body, status, err := httpJSON(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("app %s not found", name)
	}
	if status != http.StatusOK {
		return fmt.Errorf("controller get: %s", strings.TrimSpace(string(body)))
	}
	return printResource(body)
}

func putResource(url string, res resource.Resource) ([]byte, int, error) {
	buf, err := resource.Encode(res)
	if err != nil {
		return nil, 0, err
	}
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(buf))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if res.ResourceVersion != 0 {
		req.Header.Set("If-Match", fmt.Sprintf("%d", res.ResourceVersion))
	}
	resp, err := controlClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return body, resp.StatusCode, err
}

func httpJSON(method, url string, body []byte) ([]byte, int, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return nil, 0, err
	}
	resp, err := controlClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return out, resp.StatusCode, err
}

func printResource(body []byte) error {
	var buf bytes.Buffer
	if err := json.Indent(&buf, body, "", "  "); err != nil {
		fmt.Fprintln(os.Stdout, string(body))
		return nil
	}
	fmt.Fprintln(os.Stdout, buf.String())
	return nil
}

// controlClient is the HTTP client app commands use for the control API.
var controlClient = http.DefaultClient

func tlsFlags(fs *flag.FlagSet) *controlauth.TLSFlags {
	var f controlauth.TLSFlags
	fs.StringVar(&f.Cert, "tls-cert", "", "client certificate for an https controller (an operator or product-client principal)")
	fs.StringVar(&f.Key, "tls-key", "", "client key")
	fs.StringVar(&f.CA, "tls-ca", "", "controller CA certificate")
	return &f
}

func useControlClient(f controlauth.TLSFlags, controller string) error {
	c, err := f.Client(controller)
	if err != nil {
		return err
	}
	controlClient = c
	return nil
}
