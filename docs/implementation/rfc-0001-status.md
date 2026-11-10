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
| Raft (one voter) | `internal/raftnode` + `internal/controlraft.Journal`: with `--raft-*`, every control `Update` proposes `restore_store` and the FSM `Import` is the only write to `control.db`; snapshots Export/Import the store. Library choice: [ADR-0001](../adr/0001-control-plane-raft-library.md) | Three voters, membership changes, unikernel footprint; typed per-op log entries (today one archive command per mutation) |
| Executor | Describe/CreateHost/TerminateHost; volume/snapshot methods fail closed; Memory effects off by default | AWS Pulumi/Lambda worker, budgets, fences |
| Demand | Join/Cancel/AcknowledgeHandoff; coalesce per resource UID | Cross-process lost-ack proofs on real host gateway |
| Placement | Pack-first; one capacity op; minHosts=0; unknown ops reserve; warm observe/active with `reserve_mib` | Adaptive warm math, anti-affinity, cloud quiet-period proof |
| Reconcile | Intent before dispatch; timeout → unknown; status write; ready-placement handoff bridge | Injected provider crash, budget ledger |
| Edge | Mounted via `--edge-listen`; `/svc/<name>/`, Host routing, 503/429, `/healthz` no wake | TLS, real host-gateway upstream publication, admission under load |
| Host assignments | `GET /v1/hosts/{id}/assignments`; host `--controller` + `--host-id` pulls via `bootcred.AssignmentClient` | Watch/push transport; multi-host enrollment |
| Boot credential scope | `POST /v1/hosts/{id}/credentials/{installation}` authorizes pinned tags (no STS on controller); `bootcred.Guarded` forces those tags before host AssumeRole | Controller-issued STS; AWS executor fences |
| Replica credentials | `--replica-credentials`: per-installation SeaweedFS identities limited to `inst/<id>/*`, derived from a KMS-wrapped provisioning key, registered through the signed filer API and probed before use, delivered on the credentials route | Identity removal on delete; per-installation hint reader credentials |
| Warm | `GET /v1/warm`; observe reports shortfall without buying; active may request buffer after demand is met | Measured adaptive sizing |
| CLI | `host`, `controller` (raft + edge flags), `app apply/get`, `version` | `sufuree db` (RFC-0002) |

## Qualification still open

Every gate in RFC-0001 §10 and every scenario in §11 still needs a receipt.
Local (no-AWS) slice for Raft mutations + bootscope authorize:
[docs/qualification/rfc-0001-local.md](../qualification/rfc-0001-local.md)
(`go test ./internal/qual/`). In-process Memory executor is not a cloud bill;
AWS worker gates remain open.
