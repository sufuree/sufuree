package guest

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/workload"
)

func TestHTTPBootReadyAndStop(t *testing.T) {
	h := &HTTP{}
	g, err := h.Boot(context.Background(), workload.Spec{
		ID: "w", GuestPort: 8080, GenerationEnvKey: "GEN",
	}, 3, map[string]string{"GEN": "3"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Get("http://" + g.Addr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(body) != "ok\n" || res.Header.Get("X-Generation") != "3" {
		t.Fatalf("status=%d body=%q gen=%q", res.StatusCode, body, res.Header.Get("X-Generation"))
	}
	if err := g.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.Exited():
	case <-time.After(2 * time.Second):
		t.Fatal("guest did not exit")
	}
}
