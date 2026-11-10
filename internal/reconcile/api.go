package reconcile

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/sufuree/sufuree/internal/bootscope"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// Handler is the local control API.
//
//	PUT /v1/resources/{kind}/{name}
//	GET /v1/resources/{kind}/{name}
//	GET /v1/resources
//	GET /v1/hosts
//	GET /v1/hosts/{id}/assignments
//	POST /v1/hosts/{id}/credentials/{installation}
//	GET /v1/operations
//	GET /v1/warm
//
// PUT rejects a stale ResourceVersion. If-Match, when present, must equal the
// body's resource_version.
func Handler(c *Controller) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/resources/{kind}/{name}", func(w http.ResponseWriter, r *http.Request) {
		putResource(c, w, r)
	})
	mux.HandleFunc("GET /v1/resources/{kind}/{name}", func(w http.ResponseWriter, r *http.Request) {
		kind, name, ok := pathResource(w, r)
		if !ok {
			return
		}
		got, err := c.GetResource(r.Context(), kind, name)
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, got)
	})
	mux.HandleFunc("GET /v1/resources", func(w http.ResponseWriter, r *http.Request) {
		got, err := c.ListResources(r.Context())
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, got)
	})
	mux.HandleFunc("GET /v1/hosts", func(w http.ResponseWriter, r *http.Request) {
		got, err := c.ListHosts(r.Context())
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, got)
	})
	mux.HandleFunc("GET /v1/hosts/{id}/assignments", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		got, err := c.ListPlacementsByHost(r.Context(), id)
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		// Publish controller-pinned session tags with each active placement so
		// a host cannot invent an inst/* scope for an installation it does not hold.
		writeJSON(w, http.StatusOK, bootscope.AssignmentsForHost(got, id))
	})
	mux.HandleFunc("POST /v1/hosts/{id}/credentials/{installation}", func(w http.ResponseWriter, r *http.Request) {
		postCredentials(c, w, r)
	})
	mux.HandleFunc("GET /v1/operations", func(w http.ResponseWriter, r *http.Request) {
		got, err := c.ListOperations(r.Context())
		if err != nil {
			writeStoreErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, got)
	})
	mux.HandleFunc("GET /v1/warm", func(w http.ResponseWriter, r *http.Request) {
		got := c.BufferRecommendation()
		if got == nil {
			writeJSON(w, http.StatusOK, map[string]any{"mode": "off"})
			return
		}
		writeJSON(w, http.StatusOK, got)
	})
	return mux
}

// credentialRequest is the optional body for the bootscope authorize broker.
// The controller does not call STS; it only returns pinned session tags the
// host must use when assuming the guest role locally.
type credentialRequest struct {
	RequestedTags map[string]string `json:"requested_tags,omitempty"`
}

type credentialResponse struct {
	Installation string            `json:"installation"`
	SessionTags  map[string]string `json:"session_tags"`
}

func postCredentials(c *Controller, w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	installation := r.PathValue("installation")
	if hostID == "" || installation == "" {
		http.Error(w, "host id and installation are required", http.StatusBadRequest)
		return
	}
	var req credentialRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "credential request is not JSON", http.StatusBadRequest)
			return
		}
	}
	placements, err := c.ListPlacementsByHost(r.Context(), hostID)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	tags, err := bootscope.Authorize(placements, hostID, installation, req.RequestedTags)
	if err != nil {
		switch {
		case errors.Is(err, bootscope.ErrNotHeld):
			http.Error(w, err.Error(), http.StatusForbidden)
		case errors.Is(err, bootscope.ErrTagMismatch):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	writeJSON(w, http.StatusOK, credentialResponse{Installation: installation, SessionTags: tags})
}

func putResource(c *Controller, w http.ResponseWriter, r *http.Request) {
	kind, name, ok := pathResource(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var in resource.Resource
	if err := resource.Decode(body, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in.Kind != "" && in.Kind != kind {
		http.Error(w, "kind does not match the URL", http.StatusBadRequest)
		return
	}
	if in.Name != "" && in.Name != name {
		http.Error(w, "name does not match the URL", http.StatusBadRequest)
		return
	}
	in.Kind = kind
	in.Name = name
	if match := r.Header.Get("If-Match"); match != "" {
		v, err := strconv.ParseUint(strings.Trim(match, `"`), 10, 64)
		if err != nil {
			http.Error(w, "If-Match is not a resource version", http.StatusBadRequest)
			return
		}
		if in.ResourceVersion != 0 && in.ResourceVersion != v {
			http.Error(w, "If-Match does not match resource_version", http.StatusBadRequest)
			return
		}
		in.ResourceVersion = v
	}
	got, err := c.ApplyResource(r.Context(), in)
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, got)
}

func pathResource(w http.ResponseWriter, r *http.Request) (resource.Kind, string, bool) {
	kind, err := parseKind(r.PathValue("kind"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return "", "", false
	}
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return "", "", false
	}
	return kind, name, true
}

func parseKind(s string) (resource.Kind, error) {
	switch strings.ToLower(s) {
	case "app":
		return resource.KindApp, nil
	case "pool":
		return resource.KindPool, nil
	default:
		return "", errors.New("kind must be App or Pool")
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(buf, '\n'))
}

func writeStoreErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, store.ErrStaleResourceVersion):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
