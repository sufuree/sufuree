// Command sufuree is the Sufuree CLI.
//
//	sufuree host --config host.json
//	sufuree controller --data-dir DIR --listen ADDR [--executor memory] [--deployment ID]
//	                    [--raft-bind ADDR --raft-id ID] [--raft-bootstrap]
//	                    [--edge-listen ADDR]
//	sufuree app apply --file spec.json --controller URL
//	sufuree app get NAME --controller URL
//	sufuree version
//
// host runs one host agent. controller reconciles desired resources and may
// serve an HTTP edge. version prints the RFC-0001 implementation revision.
// None of these commands is a qualified cloud deployment.
package main

import (
	"errors"
	"fmt"
	"os"
)

const version = "0.1.0-rfc0001"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sufuree:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sufuree <host|controller|app|ca|replica-key|version>")
	}
	switch args[0] {
	case "host":
		return runHost(args[1:])
	case "controller":
		return runController(args[1:])
	case "app":
		return runApp(args[1:])
	case "ca":
		return runCA(args[1:])
	case "replica-key":
		return runReplicaKey(args[1:])
	case "version":
		fmt.Fprintln(os.Stdout, version)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
