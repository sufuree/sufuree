# ADR-0001: Control-plane Raft library choice

**Status:** Accepted  
**Date:** 2026-11-09  
**Context:** Sufuree RFC-0001 controller (`feat/rfc0001-code-complete`); sibling design repo [sufuree/raft](https://github.com/sufuree/raft) RFC-0001 (deterministic Go Raft library, design stage only).

## Decision

Sufuree keeps **HashiCorp Raft** (`github.com/hashicorp/raft` + `raft-boltdb/v2`) for the control-plane leadership fence in `internal/raftnode`.

Do **not** adopt `sufuree/raft` as a dependency until that library exists, passes its own release gates, and Sufuree has a concrete need that HashiCorp cannot meet.

Do **not** treat a library swap as the next Raft work item. The higher-priority gap is putting authoritative control mutations on a replicated log (today `internal/store` bbolt remains authoritative for resource/ops records).

## Because

- The current fence is a **one-voter leadership gate** (`IsLeader` / Apply; non-leader control mutations 503). HashiCorp already covers that thin use.
- `sufuree/raft` is **design-only** (no installable package, no measured qualification). It cannot replace anything in tree today.
- Even after a new library ships, Sufuree’s multi-voter / unikernel / effect-fencing gaps (RFC-0001 F01/F17 and related) are not solved by swapping Raft implementations.
- The `sufuree/raft` RFC itself recommends evaluating a pinned existing core (and etcd Raft composition) before owning a new protocol core.

## Stay on HashiCorp until

All of the following are true (or an owner explicitly overrides this ADR):

1. Sufuree needs **multi-voter** membership and recovery that the HashiCorp integration cannot qualify on the target host/guest footprint, **or** HashiCorp’s contracts block required durability/progress semantics; and
2. `sufuree/raft` (or another chosen core) is an **implemented, released** module with passing library correctness/recovery gates; and
3. A written consumer evaluation shows migration cost is justified vs continuing HashiCorp or composing etcd Raft.

## Adopt `sufuree/raft` only if

- It is past design stage and meets (2) above; **and**
- Sufuree’s control FSM and durability needs map cleanly onto its committed/dispatched/applied and resource-budget contracts; **and**
- Comparative evidence (per that library’s RFC §11) beats HashiCorp/etcd composition on the published Sufuree thresholds—or HashiCorp is disqualified on the unikernel/host profile.

Until then, treat `sufuree/raft` as an **optional future dependency / contract reference**, not a Sufuree delivery dependency.

## Rejected for now

| Alternative | Why not now |
| --- | --- |
| Immediate migration to `sufuree/raft` | No library implementation exists. |
| Blocking Sufuree controller work on a new Raft core | Wrong critical path; fence already works for one voter; record replication is the control-plane gap. |
| Claiming current HashiCorp use is multi-voter qualified | One voter only; resource writes are not every Raft log entry (see `docs/implementation/rfc-0001-status.md`). |

## Consequences

- Continue improving `internal/raftnode` as a thin HashiCorp wrapper (bootstrap/recovery rules, leader-only mutations, optional store Export in snapshots).
- Prefer logging/applying authoritative control mutations (or an equivalent FSM) before any consensus-library rewrite.
- Revisit this ADR when multi-voter qualification starts or when `sufuree/raft` publishes a release candidate.

## References

- Sufuree: `internal/raftnode`, `docs/implementation/rfc-0001-status.md`, RFC-0001 §3 (Raft as control mechanism).
- Sibling: `../raft` → [RFC-0001: A deterministic Go Raft library](https://github.com/sufuree/raft/blob/main/docs/rfc/0001-deterministic-go-raft.md) (§11 comparative acceptance, §12 alternatives).
