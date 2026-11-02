# Archived original proposal — RFC-0001

Public archival copy of the historical proposal; deployment-specific names have been generalized. The original is retained in the private working record. **Superseded; not an implementation contract.** See the [canonical RFC](../rfc/0001-scale-to-zero-unikernel-runtime.md) for current requirements, proposed design, open gaps and acceptance criteria. The performance claims, timelines and product scope below are unqualified original proposals.

---

The RFC below is structured as a self-contained engineering specification that you can drop directly into your repository or feed to your coding agent. It bridges **Phase 1** (your internal Go unikernel workloads and dogfooding) with **Phase 2** (the polyglot "Supabase-in-a-Unikernel" appliance for non-Go vibe coders and autonomous agents).

---

# RFC-0001: Sufuree — Scale-to-Zero Unikernel Ingress, Consensus & Polyglot BaaS Engine

* **Status:** Draft / Architecture Review
* **Target Ecosystem:** Example deployment (`*.example.com` multi-tenant runtime isolation)
* **Primary Language:** Go (Golang 1.23+)
* **Deployment Targets:** AWS (`c8i`/`m8i` Nested KVM) & GCP (`C3`/`N2` Nested KVM)
* **Core Protocols:** HTTP/1.1, HTTP/2, WebSocket/SSE, OpenAPI 3.1, Model Context Protocol (MCP over Stdio + Streamable HTTP with SIMD JSON acceleration)

---

## 1. Executive Summary & Motivation

Autonomous AI agents and multi-tenant customer backends (`<client>.example.com`) require strict compute isolation, instant provisioning, and zero idle compute cost. Standard container orchestration (Kubernetes/Docker) shares a host Linux kernel—creating multi-tenant escape risks for untrusted agent execution—and carries significant control-plane memory overhead. Meanwhile, starting/stopping native AWS EC2 or GCP Compute Engine VMs per request incurs a 5–12 second control-plane latency penalty and a 60-second minimum billing floor per boot.

**Sufuree** solves this by operating as a horizontally scalable, self-contained **Go unikernel ingress and orchestration cluster**. It routes traffic to tenant unikernels running on a nested KVM host pool, waking them in **5–20ms**, holding idle Realtime (WebSocket/SSE) subscriptions at the router tier, and enforcing a deterministic **Billing-Floor Reaper** that maximizes warm compute without double-billing.

To serve two distinct audiences without architectural drift, Sufuree separates **Execution Mode A (Bring-Your-Own Go Unikernel)** for internal dogfooding from **Execution Mode B (The Polyglot BaaS Appliance)** for external vibe coders and AI agents using TypeScript, Python, or declarative schemas.

---

## 2. Ecosystem Alignment & Design Principles

When the coding agent audits this RFC against the existing codebase, it must enforce the following architectural invariants:

1. **Deterministic State-Machine Execution:** Every tenant lifecycle transition (`COLD` $\to$ `WAKING` $\to$ `WARM` $\to$ `DRAINING` $\to$ `SNAPSHOT_SYNC` $\to$ `COLD`) and host pool transition must be governed by explicit, auditable finite-state machines with zero implicit background mutations.
2. **Zero External Control-Plane Dependencies:** Sufuree nodes compile to standalone Go unikernels. Cluster coordination uses embedded **HashiCorp Raft** (control plane) and **HashiCorp Memberlist** (data plane gossip), backed by cloud object storage (S3/GCS) for durable state snapshots. No external Redis, Etcd, or Postgres cluster is required to run Sufuree.
3. **Standardized Identity & Auth:** All control-plane mutations, tenant dashboard sessions, and multi-tenant JWT verifications standardize on **WorkOS** OIDC/SAML/API Key primitives.
4. **First-Class MCP & OpenAPI Surface:** Every control-plane action and every tenant data-plane endpoint exposes both an **OpenAPI 3.1** specification and an **MCP (Stdio + Streamable HTTP)** interface utilizing SIMD-accelerated JSON parsing for low-latency agent tool calls.
5. **Telemetry & Audit Loops:** Structured telemetry, boot latency histograms, billing-window ledger events, and panic/crash traces emit over a structured sink compatible with **Observability** for automated AST-level issue synthesis.

---

## 3. System Topology & Two-Tier Billing Physics

Sufuree decouples **Cloud Host Billing (60s floor)** from **Tenant Compute Execution (ms-level lifecycle)**:

```
                       [ Wildcard Ingress: *.example.com ]
                                      │
             ┌────────────────────────┴────────────────────────┐
             ▼                                                 ▼
   ┌───────────────────┐    Memberlist Gossip (UDP)  ┌───────────────────┐
   │ Sufuree Router #1 │◄───────────────────────────►│ Sufuree Router #2 │
   │  (Go Unikernel)   │◄───────────────────────────►│  (Go Unikernel)   │
   └─────────┬─────────┘      Embedded Raft (TCP)    └─────────┬─────────┘
             │         (Singleflight Gate + WS Offload)        │
             └────────────────────────┬────────────────────────┘
                                      │ Internal WireGuard / VXLAN Mesh
             ┌────────────────────────┴────────────────────────┐
             ▼                                                 ▼
   ┌─────────────────────────────────┐               ┌─────────────────────────────────┐
   │ KVM Host VM #1 (AWS c8i/GCP C3) │               │ KVM Host VM #2 (AWS c8i/GCP C3) │
   │ [60s Cloud Billing Floor Lock]  │               │ [60s Cloud Billing Floor Lock]  │
   │ ┌─────────────────────────────┐ │               │ ┌─────────────────────────────┐ │
   │ │ sufuree-hostd (Go Daemon)   │ │               │ │ sufuree-hostd (Go Daemon)   │ │
   │ └──────┬───────────────┬──────┘ │               │ └──────┬───────────────┬──────┘ │
   │        ▼               ▼        │               │        ▼               ▼        │
   │  ┌───────────┐   ┌───────────┐  │               │  ┌───────────┐   ┌───────────┐  │
   │  │ Tenant A  │   │ Tenant B  │  │               │  │ Tenant C  │   │ Tenant D  │  │
   │  │(Custom Go)│   │(BaaS VM)  │  │               │  │(Custom Go)│   │(BaaS VM)  │  │
   │  └───────────┘   └───────────┘  │               │  └───────────┘   └───────────┘  │
   └─────────────────────────────────┘               └─────────────────────────────────┘

```

### 3.1 Tier 1: Cloud KVM Host Pool (`sufuree-hostd`)

* **Supported Instances:** AWS `c8i.xlarge` / `m8i.xlarge` (non-metal Nitro with nested KVM enabled) and GCP `c3-standard-4` / `n2-standard-4` (`enableNestedVirtualization=true`).
* **Host Billing Floor Enforcement:** When the Raft Leader provisions or starts a Cloud Host VM via AWS EC2 (`StartInstances`) or GCP Compute (`instances.start`) at $T_{\text{host\_start}}$, that host is locked into an `ACTIVE` lease until at least:

$$T_{\text{host\_floor}} = T_{\text{host\_start}} + B_{\text{cloud\_min}} - T_{\text{host\_shutdown}}$$



where $B_{\text{cloud\_min}} = 60\text{s}$ and $T_{\text{host\_shutdown}} = 4\text{s}$.
* **Host Daemon (`sufuree-hostd`):** A single static Go binary running on the host VM that exposes a mutually-authenticated gRPC/MCP interface to the Sufuree Router tier. It manages `/dev/kvm` via Firecracker or `qemu-system-x86_64 -M microvm`, pre-allocates `/dev/net/tun` TAP interfaces, and mounts tenant persistent block devices (`virtio-blk`).

### 3.2 Tier 2: Tenant MicroVM / Unikernel Lifecycle

* **Boot / Resume Mechanics:**
* **Cold Boot (~15–25ms):** Boots a Nanos or Unikraft ELF/disk image directly into KVM.
* **Warm Snapshot Resume (~5ms):** Pauses vCPUs, restores memory pages via `userfaultfd` or direct NVMe mmap from the last paused state, and immediately processes the waiting TCP/HTTP packet.


* **Tenant Minimum Warm Window:** Even inside a warm KVM host, thrashing tenant snapshots on every 50ms request wastes NVMe I/O. Each tenant activation enforces a configurable minimum warm window $W_{\text{tenant\_min}}$ (e.g., `15s` for internal micro-services, or `57s` if mapped 1:1 to a dedicated cloud VM in Direct Mode).

---

## 4. Two-Phase Execution Model: Go Dogfooding vs. Non-Go Vibe Coders

Sufuree supports both native Go unikernels and non-Go vibe coders without changing the router or host daemon architecture. Both run as isolated unikernels on `sufuree-hostd`.

| Feature | Mode A: Native Go Unikernel (Phase 1 — Internal) | Mode B: Polyglot BaaS Unikernel (Phase 2 — Vibe Coders) |
| --- | --- | --- |
| **Target User** | Example deployment internal services (`Example product`, `Observability`, custom Go agents) | Vibe coders (TypeScript/JS, Python, SQL/JSON) & external AI agents |
| **Artifact Type** | Custom-compiled Go binary wrapped in Nanos/Unikraft image | Pre-compiled **Sufuree Golden Unikernel** (written in Go, immutable image) |
| **Compile Time** | ~2 seconds (via pre-warmed `GOCACHE` + raw image injector) | **0 seconds** (instant provisioning; no compiler in the loop) |
| **Data & Auth** | Custom per Go app (or embedded SQLite + WorkOS SDK) | Built-in SQLite (WAL), Auto-OpenAPI/MCP CRUD, WorkOS Auth, S3 Storage |
| **Custom Logic** | Native Go HTTP/MCP handlers compiled into kernel | Embedded **QuickJS-NG / Wazero (WASM)** sandbox inside the Go unikernel |

### 4.1 How Non-Go Vibe Coders Use Mode B (Zero Go Knowledge Required)

A developer using Cursor/Claude Code or building a frontend in Next.js/React/Flutter never writes Go or touches a unikernel toolchain:

1. **Declarative DB, Auth & Realtime (80% of requests):** They push a SQL schema or JSON definition via the Sufuree MCP tool (`sufuree_apply_schema`) or TypeScript SDK. The Golden Unikernel (written in Go) dynamically inspects the SQLite catalog and auto-generates REST endpoints (`/rest/v1/*`), GraphQL, OpenAPI 3.1 specs, and tenant-specific MCP tools.
2. **Polyglot Edge Functions (TypeScript / Python):** When a vibe coder needs custom server-side business logic (e.g., Stripe webhooks, custom agent orchestration in TypeScript or Python), they push a `.ts`, `.js`, or `.py` file via `sufuree_deploy_function`.
* The Sufuree Golden Unikernel embeds **QuickJS** (via `wazero` or pure-Go WASM runtime) and **Pyodide/RustPython-WASM**.
* The user's TypeScript or Python code executes *inside* the tenant's hardware-isolated Go unikernel with direct in-memory host-call bindings to the local SQLite database and MCP router.



---

## 5. Core Go Package Specifications (`sufuree` Repository)

The coding agent should scaffold the repository with the following package boundaries:

```text
sufuree/
├── cmd/
│   ├── sufuree-router/      # Compiles to the Sufuree Ingress Unikernel
│   ├── sufuree-hostd/       # Runs on AWS c8i / GCP C3 KVM Hosts
│   └── sufuree-golden/      # Compiles to the Phase 2 BaaS Tenant Unikernel
├── internal/
│   ├── state/               # Finite State Machine + Raft CP + Memberlist AP
│   ├── proxy/               # Singleflight HTTP/MCP reverse proxy + WS offload
│   ├── reaper/              # Billing-floor & idle-timeout evaluation engine
│   ├── hypervisor/          # Cloud Drivers (AWS/GCP) + KVM/Firecracker VMM driver
│   ├── baas/                # Embedded SQLite, Auto-OpenAPI/MCP, Wazero JS/Py runtime
│   └── telemetry/           # Observability-compatible structured event & crash emitter
└── pkg/
    ├── mcp/                 # SIMD-accelerated Stdio + HTTP Streamable MCP server
    └── sdk/                 # Go & TypeScript client SDKs

```

### 5.1 Deterministic State Machine & Core Structs (`internal/state`)

```go
package state

import (
	"net/netip"
	"sync/atomic"
	"time"
)

type TenantPhase uint8

const (
	PhaseCold         TenantPhase = iota // Unikernel stopped; snapshot on NVMe/S3
	PhaseWaking                          // Singleflight lock held; KVM booting/resuming
	PhaseWarm                            // Serving traffic; inside billing floor or active window
	PhaseDraining                        // Floor expired & idle; flushing in-flight connections
	PhaseSnapshotSync                    // Pausing VMM & committing memory/disk delta
	PhaseFaulted                         // Boot or runtime fault; queued for Observability telemetry
)

type ExecutionMode uint8

const (
	ModeCustomGoUnikernel ExecutionMode = iota // Phase 1: Custom Go unikernel image
	ModeGoldenBaaS                             // Phase 2: Pre-baked Supabase-style unikernel
)

type TenantLease struct {
	TenantID         string        `json:"tenant_id"`          // e.g., "acme" -> acme.example.com
	Mode             ExecutionMode `json:"mode"`
	Phase            TenantPhase   `json:"phase"`
	HostID           string        `json:"host_id"`            // Placement on KVM host or Cloud VM ID
	InternalAddr     netip.AddrPort `json:"internal_addr"`     // Private WireGuard/TAP endpoint
	ImageRef         string        `json:"image_ref"`          // S3/GCS content-addressed unikernel image
	SnapshotRef      string        `json:"snapshot_ref"`       // Latest memory/disk snapshot hash

	// Deterministic Timing & Billing-Floor Controls
	StartedAtUnixNano    int64 `json:"started_at_ns"`
	FloorUntilUnixNano   int64 `json:"floor_until_ns"`     // T_start + B_min - T_shutdown
	LastActiveUnixNano   int64 `json:"last_active_ns"`     // Updated via Memberlist gossip
	IdleTimeoutNano      int64 `json:"idle_timeout_ns"`    // Additional idle grace after floor
	ActiveConns          int32 `json:"active_conns"`       // Atomic counter for HTTP/MCP streams
	OffloadedWebSockets  int32 `json:"offloaded_ws"`       // Held at Sufuree router when VM is Cold
}

// CanReap evaluates the deterministic shutdown invariant.
func (t *TenantLease) CanReap(now time.Time) bool {
	if t.Phase != PhaseWarm {
		return false
	}
	if atomic.LoadInt32(&t.ActiveConns) > 0 {
		return false
	}
	nowNano := now.UnixNano()
	// 1. Never reap before the minimum billing floor minus shutdown allowance
	if nowNano < atomic.LoadInt64(&t.FloorUntilUnixNano) {
		return false
	}
	// 2. Never reap if recent traffic arrived within IdleTimeoutNano
	lastActive := atomic.LoadInt64(&t.LastActiveUnixNano)
	return (nowNano - lastActive) >= t.IdleTimeoutNano
}

```

### 5.2 Singleflight Ingress Proxy & Realtime Offload (`internal/proxy`)

1. **Host Header Extraction:** Incoming request to `<tenant>.example.com` resolves `tenant_id`.
2. **Fast Path (`PhaseWarm`):** Local `memberlist` table returns `InternalAddr`. Atomic increment `ActiveConns`, update local `LastActiveUnixNano` (batched gossip broadcast every 250ms if changed), proxy request, and decrement `ActiveConns` on stream close.
3. **Slow Path (`PhaseCold` / `PhaseWaking`):**
* Enter `wakeGroup.Do(tenantID, func() (netip.AddrPort, error))` using `golang.org/x/sync/singleflight`.
* Request lease transition `COLD -> WAKING` from the Raft Leader.
* Raft Leader computes:

$$\text{FloorUntil} = T_{\text{now}} + \text{BillingPeriod} - \text{ShutdownAllowance}$$



and dispatches `ResumeMicroVM` RPC to the assigned `sufuree-hostd`.
* Poll the tenant's TAP IP health/readiness socket every `2ms` (for KVM mode) or `50ms` (for Direct Cloud VM mode) with a strict context deadline (`10s`).
* Transition state `WAKING -> WARM`, gossip the route across all Sufuree routers, release all goroutines waiting on `singleflight`, and flush buffered HTTP/MCP requests.


4. **Realtime WebSocket/SSE Offload Engine:**
* When a client connects to `wss://<tenant>.example.com/realtime/v1`, Sufuree terminates the WebSocket connection inside the router's memory space (`OffloadedWebSockets++`) instead of proxying a raw TCP socket that would pin `ActiveConns > 0` forever.
* Sufuree registers the client's channel/table subscription filter in the cluster gossip state.
* When the tenant unikernel goes idle and enters `PhaseCold`, the client's WebSocket remains open and connected to Sufuree.
* When a subsequent mutation (`POST`, `PATCH`, `DELETE`, or MCP tool execution) wakes the tenant unikernel, the Golden Unikernel emits a compact WAL change event header (`X-Sufuree-WAL-Broadcast`) or internal UDP frame back to Sufuree, which fans the payload out to all subscribed WebSockets.



### 5.3 Cloud Provider & KVM Driver Interface (`internal/hypervisor`)

To allow both **Direct Cloud VM Mode** (1 VM = 1 Tenant Unikernel on EC2/GCE) and **Nested KVM Host Pool Mode** (1 Cloud Host VM = $N$ Tenant MicroVMs), both implement the same Go interface:

```go
package hypervisor

import (
	"context"
	"net/netip"
	"time"
)

type DriverMode string

const (
	DriverAWSNitroDirect DriverMode = "aws-ec2-direct"
	DriverGCPDirect      DriverMode = "gcp-gce-direct"
	DriverNestedKVM      DriverMode = "nested-kvm-hostd"
)

type BootSpec struct {
	TenantID          string
	ImageURI          string
	SnapshotURI       string
	VCPUCount         int
	MemoryMB          int
	PersistentDiskURI string
	EnvVars           map[string]string
}

type LeasePlacement struct {
	HostID            string
	InternalEndpoint  netip.AddrPort
	MinimumBillingDur time.Duration // 60s for Direct EC2/GCE; configurable (e.g., 10s) for KVM microVM
	ShutdownAllowance time.Duration // 3s for ACPI/Snapshot flush
}

type Provider interface {
	Wake(ctx context.Context, spec BootSpec) (LeasePlacement, error)
	SuspendOrStop(ctx context.Context, tenantID string, placement LeasePlacement, saveSnapshot bool) (snapshotURI string, err error)
	Inspect(ctx context.Context, tenantID string, placement LeasePlacement) (alive bool, err error)
}

```

---

## 6. Dual-Surface Control Plane: OpenAPI 3.1 & MCP

Both human SDKs (or the Flutter executive management UI) and autonomous AI coding agents interact with Sufuree via the exact same schema definitions on `api.example.com` / `mcp.example.com`, secured by WorkOS Bearer Tokens.

### Core MCP Tools Exposed by Sufuree

| MCP Tool Name | Target Audience | Parameters | Deterministic Behavior |
| --- | --- | --- | --- |
| `sufuree_provision_tenant` | Internal & Vibe Coders | `subdomain`, `mode` (`go_custom` | `golden_baas`), `region`, `memory_mb` | Commits tenant entry in Raft, allocates S3/GCS snapshot prefix & `virtio-blk` volume, returns `<subdomain>.example.com` + WorkOS scoped keys. |
| `sufuree_deploy_unikernel` | Phase 1 (Go Apps) | `subdomain`, `go_binary_or_image_url`, `env_vars` | Packages static Go binary into Nanos/Unikraft image, updates `ImageRef` in Raft, triggers rolling drain & 15ms microVM replacement. |
| `sufuree_apply_schema` | Phase 2 (Vibe Coders) | `subdomain`, `sql_ddl`, `rls_policies` | Wakes tenant Golden Unikernel, runs transactional SQLite DDL migration, regenerates tenant's `/openapi.json` and `/mcp` tool definitions. |
| `sufuree_deploy_function` | Phase 2 (Vibe Coders) | `subdomain`, `route`, `runtime` (`ts` | `py` | `wasm`), `source_code` | Compiles TS/Python/WASM bundle into the tenant's `virtio-blk` function store; immediately routable inside the tenant unikernel. |
| `sufuree_get_lease_telemetry` | Observability / Ops Agents | `subdomain`, `include_billing_ledger` | Returns current FSM state (`COLD`/`WARM`), $T_{\text{floor}}$ countdown, boot latency p99, active connections, and structured crash logs. |

---

## 7. Phased Implementation Plan for Coding Agent

To dogfood this cleanly for your own Go unikernels first before opening the polyglot BaaS layer to vibe coders, execute in four sequential milestones:

### Milestone 1: Core Router, Singleflight & Direct Cloud VM Driver (Days 1–5)

* Scaffold `cmd/sufuree-router` in Go.
* Implement `internal/state` with single-node and 3-node embedded `hashicorp/raft` + `hashicorp/memberlist`.
* Implement `internal/reaper` enforcing `CanReap(now)` with configurable `BillingPeriod` (`60s`) and `ShutdownAllowance` (`3s`).
* Build `DriverAWSNitroDirect` and `DriverGCPDirect` to test spinning up existing Go unikernels on AWS and GCP via native cloud APIs first, validating the Singleflight request holding and proxy streaming.

### Milestone 2: Nested KVM Host Daemon (`sufuree-hostd`) & 15ms Wake (Days 6–12)

* Scaffold `cmd/sufuree-hostd` running on AWS `c8i` and GCP `C3` instances with `/dev/kvm`.
* Wire Firecracker / QEMU `microvm` lifecycle management, TAP pool pre-allocation, and WireGuard mesh routing back to `sufuree-router`.
* Implement `SuspendOrStop` with memory snapshot dump and fast resume, dropping `<client>.example.com` cold starts from ~6 seconds (Cloud API) to **<20 milliseconds**.

### Milestone 3: Control Plane MCP, WorkOS Auth & Observability Telemetry Hook (Days 13–17)

* Expose the SIMD-accelerated Stdio + HTTP Streamable MCP endpoint on `sufuree-router`.
* Integrate WorkOS middleware for tenant provisioning and API token validation.
* Wire structured state-transition and fault telemetry emissions so Observability can monitor unikernel boot failures or reaper anomalies.

### Milestone 4: `sufuree-golden` Polyglot BaaS Appliance for Vibe Coders (Days 18–25)

* Build `cmd/sufuree-golden` (the pre-baked Go unikernel image).
* Embed pure-Go SQLite (`modernc.org/sqlite`), automatic OpenAPI 3.1 + `/mcp` CRUD reflection, S3 storage proxy, and the Router-side WebSocket Realtime offload hub.
* Embed `wazero` with QuickJS to allow non-Go users and agents to deploy TypeScript/JavaScript edge functions inside their isolated unikernel without compiling Go.

---
