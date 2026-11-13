# RFC-0001 implementation status

**Code-complete for the RFC-0001 application/runtime control surface in this tree.**
Qualification gates (F01–F19 and §11 scenarios) remain open. This is not an
acceptance record, not a cloud deployment, and not RFC-0002.

`sufuree version` is `0.1.0-rfc0001`.

## Implemented

| Area | Code | Still open for qualification |
| --- | --- | --- |
| Host agent | `sufuree host`: Unix sockets, journaled generations, QEMU/KVM guests, idle/scheduled wake; live QEMU pin schema + opt-in qual (`qemupin`, `TestFixedHostQEMUWakeProxyIdle`) | Multi-host enrollment, provider fence, measured wake on digest-pinned image |
| Resources + store | Declarative App/Pool, operations, placements, hosts, claims in bbolt (`NoSync=false`), CAS on `resource_version`, Export/Import archive | Encrypted backup, crash tests on target FS |
| Raft (one voter) | `internal/raftnode` on **`github.com/sufuree/raft`** (driver + WAL + mTLS transport) + `internal/controlraft.Journal`: with `--raft-*`, every control `Update` proposes `restore_store` and the FSM `Import` is the only write to `control.db`; restart restores the FSM from the WAL snapshot + committed log replay. Library choice: [ADR-0002](../adr/0002-control-plane-sufuree-raft.md) (supersedes ADR-0001) | Three voters, membership changes, combined edge unikernel footprint; typed per-op log entries (today one archive command per mutation); re-pin to a tagged library release |
| Executor | Memory + `executor.Gated` growth grant/ledger; `ec2exec` RunInstances/Terminate/Describe/Fence with ClientToken (live AWS opt-in via `--executor ec2`) | Pulumi/Lambda worker custody, live EC2 receipts, volume effects |
| Growth ledger | Durable: `budget.Ledger` persists each reservation transition through the journal (`budget` store bucket) and re-seeds spent capacity at startup, so a restart cannot reset grant usage | Worker-gate custody (DynamoDB) per RFC; grant-expiry persistence across restart |
| Demand | Join/Cancel/AcknowledgeHandoff; coalesce per resource UID | Cross-process lost-ack proofs on real host gateway |
| Placement | Pack-first; one capacity op; minHosts=0; unknown ops reserve; warm observe/active with `reserve_mib` | Adaptive warm math, anti-affinity, cloud quiet-period proof |
| Reconcile | Intent before dispatch; timeout → unknown; status write; ready-placement handoff bridge | Injected provider crash |
| Edge | Mounted via `--edge-listen`; `/svc/<name>/`, Host routing, 503/429, `/healthz` no wake; `--edge-upstream name=addr` for fixed-host publication; `edge.HostWake` for co-located host-agent wake; TLS via `--edge-tls-cert`/`--edge-tls-key` | Host-posted upstream publication, admission under load, public DNS/failover |
| Observability | `internal/metrics` bounded-cardinality registry; `--metrics-listen` read-only Prometheus endpoint; reconcile/edge counters, raft watermarks/role/term + store/apply histograms, budget usage gauges (§9) | Measured nano footprint with telemetry overhead; external collection/alert path; sizing evidence |
| Host assignments | `GET /v1/hosts/{id}/assignments`; host `--controller` + `--host-id` pulls via `bootcred.AssignmentClient` | Watch/push transport; multi-host enrollment |
| Boot credential scope | `POST /v1/hosts/{id}/credentials/{installation}` authorizes pinned tags (no STS on controller); `bootcred.Guarded` forces those tags before host AssumeRole | Controller-issued STS; AWS executor fences |
| Replica credentials | `--replica-credentials`: per-installation SeaweedFS identities limited to `inst/<id>/*`, derived from a KMS-wrapped provisioning key, registered through the signed filer API and probed before use, delivered on the credentials route | Identity removal on delete; per-installation hint reader credentials |
| Warm | `GET /v1/warm`; observe reports shortfall without buying; active may request buffer after demand is met | Measured adaptive sizing |
| CLI | `host`, `controller` (raft + edge flags), `app apply/get`, `version` | `sufuree db` (RFC-0002) |

## Qualification still open

Every gate in RFC-0001 §10 and every scenario in §11 still needs a receipt.
Local (no-AWS) slices: Raft/bootscope
([rfc-0001-local.md](../qualification/rfc-0001-local.md)), grant-gated 0↔1
(`TestLocalZeroOneZeroWithGrant`), and fixed-host guest wake/proxy/idle
([rfc-0001-fixed-host.md](../qualification/rfc-0001-fixed-host.md)). Cloud path
checklist: [rfc-0001-cloud-path.md](../qualification/rfc-0001-cloud-path.md).
Live QEMU receipt (KVM host + digest-pinned image) and live EC2 0↔1 /
two-host drain remain open (spend/image auth). Pin/harness:
[rfc-0001-qemu-pin.md](../qualification/rfc-0001-qemu-pin.md).
