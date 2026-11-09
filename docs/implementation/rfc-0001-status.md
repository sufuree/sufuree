# RFC-0001 implementation status

**Code-complete for the RFC-0001 application/runtime control surface in this tree.**
Qualification gates (F01–F19 and §11 scenarios) remain open. This is not an
acceptance record, not a cloud deployment, and not RFC-0002.

`sufuree version` is `0.1.0-rfc0001`.

## Implemented

| Area | Code | Still open for qualification |
| --- | --- | --- |
| Host agent | `sufuree host`: Unix sockets, journaled generations, QEMU/KVM guests, idle/scheduled wake | Multi-host enrollment, provider fence, measured wake |
| Resources + store | Declarative App/Pool, operations, placements, hosts, claims in bbolt (`NoSync=false`), CAS on `resource_version`, Export/Import archive | Encrypted backup, crash tests on target FS |
| Raft (one voter) | `internal/raftnode` with hashicorp/raft + raft-boltdb/v2; explicit bootstrap; leader-only reconcile; control mutations 503 when not leader; snapshot can carry store Export. Library choice: [ADR-0001](../adr/0001-control-plane-raft-library.md) (stay on HashiCorp; do not adopt `sufuree/raft` until it exists and multi-voter need is real) | Three voters, membership changes, unikernel footprint; resource mutations are not yet every write in the Raft log (local bbolt remains authoritative for records) |
| Executor | Describe/CreateHost/TerminateHost; volume/snapshot methods fail closed; Memory effects off by default | AWS Pulumi/Lambda worker, budgets, fences |
| Demand | Join/Cancel/AcknowledgeHandoff; coalesce per resource UID | Cross-process lost-ack proofs on real host gateway |
| Placement | Pack-first; one capacity op; minHosts=0; unknown ops reserve; warm observe/active with `reserve_mib` | Adaptive warm math, anti-affinity, cloud quiet-period proof |
| Reconcile | Intent before dispatch; timeout → unknown; status write; ready-placement handoff bridge | Injected provider crash, budget ledger |
| Edge | Mounted via `--edge-listen`; `/svc/<name>/`, Host routing, 503/429, `/healthz` no wake | TLS, real host-gateway upstream publication, admission under load |
| Host assignments | `GET /v1/hosts/{id}/assignments` returns placements plus controller-pinned `session_tags` (`replica=inst/<id>`) via `internal/bootscope` | Agent pull/watch transport; controller-issued STS (host still assumes today) |
| Boot credential scope | `bootscope.Authorize` / `PinSessionTags` refuse `inst/*` tags for installations the host does not actively hold | Wire into a credential broker / AWS executor; host-agent still trusts local `host.json` until then |
| Warm | `GET /v1/warm`; observe reports shortfall without buying; active may request buffer after demand is met | Measured adaptive sizing |
| CLI | `host`, `controller` (raft + edge flags), `app apply/get`, `version` | `sufuree db` (RFC-0002) |

## Qualification still open

Every gate in RFC-0001 §10 and every scenario in §11 still needs a receipt.
In-process Memory executor zero-host tests are not a zero AWS bill.
