# Sufuree

A scale-to-zero runtime for applications and databases.

Sufuree is designed to start workloads when traffic arrives, route requests to
them, and release backend compute when it is no longer needed. The goal is to
make keeping many idle applications and database instances affordable, using
small isolated guests and demand-driven host autoscaling.

**Status: accepted architecture; first single-host slice in progress.** The
`sufuree host` command runs one host's agent and per-workload gateway sockets for
stateful unikernel workloads (see [Single-host slice](#single-host-slice)). There
is no controller, placement, host autoscaling or release yet. Performance,
durability, provider behavior and cost savings still require qualification.

## How it is designed to work

1. **Receive demand.** A small, always-on gateway accepts traffic and requests
   activation of a sleeping workload.
2. **Find capacity.** The controller places the workload on a suitable host,
   starting another host when admitted demand cannot fit.
3. **Start and route.** A host agent starts the isolated guest, verifies readiness
   and forwards traffic while tracking active work.
4. **Drain and scale down.** Idle workloads shut down safely. Empty backend hosts
   terminate after a quiet period, allowing the backend pool to reach zero.

The intended gateway and controller are written in Go and run on a qualified
unikernel. Workloads use unikernel guests where qualified, with Linux guests as
a compatibility fallback. The initial execution target is AWS with QEMU/KVM;
VM, bare-metal and provider differences remain explicit capability boundaries.

## Applications and databases

The design uses one repository, shared runtime packages and a single host-capacity
planner. Planned `sufuree app` and `sufuree db` command groups manage their
respective workload types.

- **Applications:** wake-on-demand routing, replica load balancing, optional
  session affinity, resource admission and safe draining.
- **Databases:** a SurrealDB-focused extension with persistent volume identity,
  single-writer fencing, engine-aware shutdown, backups and verified restoration.
- **Shared control:** declarative configuration, reconciliation, durable operation
  records, scoped infrastructure effects and lifecycle observability.

## What scale-to-zero means

The default target is zero **backend hosts** when no workload needs them. The
reachable gateway/controller, durable control state, retained data and backups
still incur costs.

Activation on a ready host targets seconds. Starting a new host can take minutes;
these are design targets, not measured guarantees. Optional warm capacity trades
cost for faster activation. Active transactions, streams and uncertain operations
can prevent safe shutdown. Sleeping a database does not delete its data.

## Design documents

| Document | Scope |
| --- | --- |
| [RFC-0001: Scale-to-zero unikernel runtime](docs/rfc/0001-scale-to-zero-unikernel-runtime.md) | Shared architecture, application lifecycle, scheduling, networking, host scaling and recovery. |
| [RFC-0002: Scale-to-zero databases](docs/rfc/0002-scale-to-zero-databases.md) | Database lifecycle, storage custody, durability, backup and restore. |

These RFCs are the canonical design. Each records the qualification gates that
must pass before its behavior can be relied on, including crash recovery,
ownership enforcement, runtime compatibility and real cost measurements.

## License

[Apache License 2.0](LICENSE).

## Single-host slice

`sufuree host --config host.json` admits stateful unikernel workloads on one
KVM host. For each workload it:

- serves a `0600` Unix socket at `<socket_dir>/<id>.sock`, holding an activity
  lease per connection and answering `503` with `Retry-After` if activation fails;
- wakes the guest once however many connections arrive, under a fresh lease
  generation that is fsynced to the host journal before boot, and retries once
  under a new generation if the guest is not ready within `startup_timeout`;
- stops the guest after `idle_timeout` without connections (and at least
  `min_awake` after a wake), unless the workload's published hint for the current
  generation says it is busy;
- while asleep, reads the workload's hint (`<hint_prefix>/wake.json` in an
  S3-compatible store) and wakes it when it is busy or its `next_due` is within
  `scheduled_lead`, so scheduled work runs without keeping the guest up.

Each boot gets a fresh `boot` volume carrying `boot_env_file`, because a
unikernel image's argv and environment are fixed at build time. `env:NAME`
values are read from the agent's environment so secrets stay out of the config.
With `boot_credentials`, each boot also gets its own short-lived AWS
credentials: the agent assumes `role_arn` with its own credentials, tags the
session with `session_tags` (the role's policy scopes access by them), and
names it `<id>-g<generation>`, so CloudTrail attributes every use to a workload
and a generation. A boot fails rather than start without them. They are for
start-up and are not refreshed, so a guest must not depend on them after
`duration`. `wake_hints`
takes a `ca_file` for a store with a private CA.
One agent holds each `data_dir` (an exclusive `flock`, so `data_dir` must be on a local filesystem, not NFS). The Ops store
(`$HOME/.ops` of the agent's user) must belong to the agent alone: run it as
a dedicated user. Before creating a `state` or `boot` volume the agent moves
any existing volume with that name into `data_dir/volumes.quarantine`. Each guest's boot volume
is deleted when it exits, and at startup the agent stops any guest an earlier
agent left running, found by its state volume on the QEMU command line. A busy hint wakes a
sleeping workload once per hint generation, and an unreadable hint defers an
idle stop at most three times. Wake hints are not authenticated: anyone who can write the hint store can
cause wakes or defer idle stops (cost and availability, not data access).
A hint may also report `progress`, an (`epoch`, `seq`) position the workload
says is durably committed. With `progress_env_key`, the agent keeps the highest
progress it has read (from every hint read, before each boot, and right after
each guest exits) in its journal and passes it to every boot as
`<epoch>:<seq>`. The workload can then refuse a store that was rolled back
behind it, which the store alone cannot show. The agent cannot authenticate
progress: a forged higher value can keep the workload from booting, never make
it accept less. Progress reported after the agent's last read is not covered.
A record holding progress is journal version 2, which an older agent refuses
rather than rewrite without it. To clear a forged floor (the agent warns on an
epoch jump), stop the agent and remove `progress_epoch` and `progress_seq`
from `data_dir/journal/<id>.json`, keeping `generation`.
Stopping is crash-consistent: workloads must acknowledge only writes that are
already durable off-host. QEMU user networking is a slice simplification, not
the production isolation boundary.
