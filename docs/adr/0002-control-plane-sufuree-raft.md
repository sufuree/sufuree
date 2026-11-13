# ADR-0002: Control plane adopts sufuree/raft

**Status:** Accepted (supersedes [ADR-0001](0001-control-plane-raft-library.md))
**Date:** 2026-11-12
**Context:** Owner direction "Complete RFC-0001. Use `../raft` if possible." The sibling
[sufuree/raft](https://github.com/sufuree/raft) library is now **implemented and qualified**
(its own RFC-0001 §11 acceptance gates passed, including a pinned QEMU/KVM Nanos unikernel
receipt and a Linux reference), which satisfies the adoption conditions ADR-0001 set.

## Decision

Sufuree's control-plane voter (`internal/raftnode`) now runs on **`github.com/sufuree/raft`**
(`driver` + `wal` + `transport`) instead of HashiCorp Raft + raft-boltdb. It is pinned to the published
commit `82d3c72` (pseudo-version `v0.0.0-20261112162430-82d3c7288969`), so a fresh clone builds
without a sibling checkout. The library has no tagged release yet; re-pin to a tagged version
when one exists and re-run qualification before deployment acceptance.

`internal/controlraft` is unchanged in contract: authoritative mutations still enter the log as
`restore_store` archive commands (and `boot_floor` commands), and the FSM Import remains the only
write to the control record file.

## Because

- ADR-0001's gate was "implemented, released module with passing correctness/recovery gates." That
  is now true; the library reports passing determinism, persistence, progress, membership, reads,
  client-uncertainty, resource-bound, bootstrap/recovery, and platform gates.
- The library is qualified on the **intended Nanos unikernel tier** (one vCPU, both WAL substrates,
  flush/rename/locking, power-loss recovery). HashiCorp + bbolt was never qualified there; RFC-0001
  F05/F17 made that compatibility a gate, not an assumption.
- Its deterministic single-owner core, explicit durability watermarks, structured proposal outcomes
  (`Applied`/`Rejected`/`Unknown`), and bounded resource contracts map directly onto RFC-0001 §3
  (intent before effects, unknown-outcome reconciliation) and §8 resource bounds.

## What changed

- `internal/raftnode` rewritten over `driver`/`wal`/`transport`:
  - Explicit create/restart discipline (mirrors `bootstrap`): bootstrap refuses existing state;
    ordinary restart never creates state.
  - Durable WAL file `control.wal` (file `Sync` per batch); streamed snapshots in the WAL.
  - **mTLS-only peer transport.** The library transport requires mutual TLS, so the development/test
    path (no operator TLS) generates an ephemeral self-signed certificate; production passes
    `controlauth.RaftTransportTLS` (CA + controller-role binding).
  - String node IDs map deterministically to the library's nonzero `uint64` member IDs; cluster ID
    derives from the deployment UID (`DeriveClusterID`).
  - `CommandFSM` adapts to the batched `driver.FSM` (`Apply([]Entry)`, `Snapshot() []byte`,
    `Restore(meta, data)`), skips the leader's empty no-op entry, and keeps the same JSON command envelope.
  - **Restart FSM restore:** the library resumes dispatch from the persisted applied watermark and
    assumes the FSM is durable to that index. Sufuree's bbolt record file is separate, so `Open`
    rebuilds the FSM from the WAL snapshot plus committed log replay up to `AppliedIndex` before the
    driver starts.
- Removed dependencies: `github.com/hashicorp/raft`, `github.com/hashicorp/raft-boltdb/v2`,
  `github.com/hashicorp/go-hclog`. `go.etcd.io/bbolt` is retained for the control record file only.

## On-disk format break

The HashiCorp state format (`raft-log.db`, `snapshots/`) is incompatible with the WAL. `Open` refuses
a directory containing legacy state with `ErrLegacyState`; it never migrates or silently re-bootstraps
over old authority. Migration is an explicit owner operation (export the control archive with the old
binary, restore into a freshly bootstrapped WAL cluster). No production deployment exists yet, so no
live data is affected.

## Consequences and remaining work

- Still **one voter**. Typed per-op log entries (today one archive command per mutation) and
  three-voter membership/recovery remain open, as in ADR-0001.
- The library has its own Nanos receipt, but the **combined Sufuree edge/controller footprint** on the
  nano instance is still unmeasured (RFC-0001 F11/F17); a library receipt is not a Sufuree deployment receipt.
- The library is pinned to a published commit (no tagged release yet); re-pin to a tagged version
  and re-run qualification before claiming cloud/runtime acceptance.

## References

- Sufuree: `internal/raftnode`, `internal/controlraft`, `internal/controlauth.RaftTransportTLS`,
  `docs/implementation/rfc-0001-status.md`, RFC-0001 §3/§8/F17.
- Sibling: `../raft` README and RFC-0001 §11 acceptance gates; `driver`, `wal`, `transport`, `bootstrap`.
