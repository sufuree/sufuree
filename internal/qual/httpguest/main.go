// Command httpguest is the minimal stateless Go HTTP qualification workload for
// RFC-0001 F05. It listens on :8080 and answers any path with HTTP 200. It is
// built into a Nanos unikernel via Ops, outside the ingress; see
// docs/qualification/rfc-0001-qemu-pin.md. The image binary is not committed;
// the pin records its SHA-256.
package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	if err := http.ListenAndServe(":8080", nil); err != nil {
		panic(err)
	}
}
