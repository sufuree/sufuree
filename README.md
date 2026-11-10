# Sufuree

A scale-to-zero runtime for applications and databases.

Sufuree is designed to start workloads when traffic arrives, route requests to
them, and release backend compute when it is no longer needed. The goal is to
make keeping many idle applications and database instances affordable, using
small isolated guests and demand-driven host autoscaling.

**Status: RFC-0001 application/runtime code surface is implemented in-tree; not qualified.**
`sufuree host` runs the single-host agent (see [Single-host slice](#single-host-slice)).
`sufuree controller` reconciles App/Pool specs through a durable bbolt store, optional one-voter Raft leadership, a pack-first planner with warm observe/active, demand handoff, and an executor that refuses cloud effects unless `--executor memory` is set.
Optional `--edge-listen` mounts the HTTP edge. There is no AWS executor, no three-voter quorum, no measured cost saving, and no `sufuree db` (RFC-0002).
See [RFC-0001 implementation status](docs/implementation/rfc-0001-status.md).

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
planner. `sufuree app apply` and `sufuree app get` talk to a running controller.
`sufuree db` is not implemented.

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

## Commands

```text
sufuree version
sufuree host --config host.json
sufuree controller --data-dir DIR --listen ADDR \
  [--executor memory] [--deployment ID] \
  [--raft-bind ADDR --raft-id ID [--raft-bootstrap]] \
  [--edge-listen ADDR]
sufuree app apply --file spec.json --controller http://127.0.0.1:8080
sufuree app get NAME --controller http://127.0.0.1:8080
```

`version` prints `0.1.0-rfc0001`. `host` is unchanged. `controller` serves the local JSON API (`PUT/GET /v1/resources/...`, `GET /v1/hosts`, `GET /v1/hosts/{id}/assignments`, `GET /v1/operations`, `GET /v1/warm`) and reconciles on a one-second tick. Effects stay disabled unless `--executor memory` is passed. Raft flags enable a one-voter leadership fence; `--edge-listen` serves the public HTTP edge. `--deployment` records a cluster identity only when the store does not already have one; the process does not invent that id. Without cluster meta, spec writes still succeed and host create/terminate operations are refused.

`app apply` sends kind `App`. A repeat of the same spec does not bump the generation. A stale `resource_version` is rejected.

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
idle stop at most three times. Without `hint_key_env_key`, wake hints are not
authenticated: anyone who can write the hint store can forge `busy`, `next_due`
or `progress` (cost and availability, not data access). With `hint_key_env_key`,
the agent creates a per-workload HMAC key under `data_dir/hintkeys/` (0600,
never in the hint store), passes it in each boot env, and accepts only version-2
envelopes whose MAC verifies for that workload's `hint_prefix`. Unsigned,
altered, or older signed hints are refused; the agent keeps the last authentic
hint (so deleting or replacing `wake.json` cannot suppress a scheduled wake,
and can end a busy veto only after the same bounded number of unreadable hints
as any read error) and records it in
`data_dir/hintkeys/<id>.last` (0600), verified again on restart, so an agent
restart does not let an older signed hint be replayed. If that record cannot be
read or written the agent logs it and carries on, and the next restart may accept
one older hint. There is no key rotation yet.
A hint may also report `progress`, an (`epoch`, `seq`) position the workload
says is durably committed. With `progress_env_key`, the agent keeps the highest
progress it has read (from every hint read, before each boot, and right after
each guest exits) in its journal and passes it to every boot as
`<epoch>:<seq>`. The workload can then refuse a store that was rolled back
behind it, which the store alone cannot show. `progress_env_key` requires
`hint_key_env_key`, so progress always comes from signed, MAC-protected hints:
an unsigned forged higher value could keep the workload from booting. Progress
reported after the agent's last read is not covered.
Progress whose epoch, or whose hint generation, is above the highest
generation the agent has issued is ignored with a warning, so a forged value
must stay within issued generations. A record holding progress is journal
version 2, which an older agent refuses rather than rewrite without it. To
clear the floor (a forged value, or an intentional rollback of the
workload's store), stop the agent and remove `progress_epoch` and
`progress_seq` from `data_dir/journal/<id>.json`, keeping `generation`.
That host-local journal is for development and single-host use only: it is
lost with the host, and a replacement host starts generations at 1 with no
floor.

### Control API authentication

`sufuree controller --tls-cert --tls-key --tls-client-ca` serves the control
API over TLS 1.3 and requires every caller to present a certificate from the
controller CA. Each certificate names one principal as a URI SAN,
`sufuree://<role>/<name>`. A default-deny route policy decides what each role
may call:

| Role | May call |
| --- | --- |
| `host` | Only its own `/v1/hosts/{id}/assignments`, `credentials`, `boot` and `progress` |
| `operator` | Resources, hosts, operations, warm, floor reads and lowering approvals |
| `product-client` | Resources, floor reads and lowering requests, on behalf of owners it authenticated (customer identity stays with the product) |
| `controller` | Nothing on the control API (its server certificate and its Raft peer identity) |

Hosts and the `app` commands take `--tls-cert`, `--tls-key` and `--tls-ca`
with an `https://` controller URL.

```
sufuree ca init --dir ca --kms-key-id alias/sufuree-ca
sufuree ca issue --dir ca --role controller --name control --server-name ctl.internal --out controller
sufuree ca issue --dir ca --role host --name host-a --out host-a
```

With `--kms-key-id`, the CA key exists on disk only as `ca.key.kms`,
encrypted by that AWS KMS key. The encryption context binds it to the CA
certificate, and `ca issue` decrypts it in memory only. The file records the
key's ARN rather than the alias, so later issues don't depend on the alias or
on the default region. AWS credentials and
region come from the standard AWS environment. `infra/` declares the key
(`alias/sufuree-ca`) with Pulumi. Its policy lets the account administer the key
but not use it, denies grants to everyone, and lets the named operator encrypt
and decrypt only with the Sufuree CA purpose in the encryption context.
`PutKeyPolicy` stays with the account as an audited break-glass. Without `--kms-key-id`, the key
is a local 0600 file (development only, not for PHI). A CA whose key and
certificate don't match is refused.

With the TLS flags set, Raft peers also use mutual TLS 1.3: each controller
presents its controller certificate (server and client authentication), and
a peer must present one from the same CA naming a `controller` principal.
An accepted peer has 10 seconds to complete its handshake. The TLS flags are
all or nothing, and are checked before Raft opens.
Without TLS flags, Raft runs over plain TCP (development only).

Limits:
- **No revocation.** There is no revocation list. Certificates default to 30
  days, and a compromised one is contained by rotating the CA.
- **Product-client scope.** Any `product-client` may manage any resource and
  request any lowering. Each product client must be scoped to its own
  resources before a second product client exists.
- **Encoded paths.** Percent-encoded request paths are refused.

### Boot generations and floors on the controller

With `sufuree controller --boot-floor` and `sufuree host --controller URL
--host-id ID --boot-floor`, the controller is the authority for each
installation's boot generation and rollback floor (Zati RFC 007 §7.4, option
A), so both survive the loss of any host. Production PHI workloads need this.

- **Identity.** An app with `max_replicas` at most 1 is one installation, and
  its placements' replica is its resource UID on every host. The host config's
  workload `id` must be that UID. Apps that can run more replicas keep fresh
  replica IDs and are not covered. A placement made before this change keeps
  its random replica ID while it exists. Once released, the next placement
  uses the UID, which is a different `inst/` prefix, so migrate the
  installation's data and host records to the UID before enabling boot
  floors. An app whose `max_replicas` drops from above 1 to 1 keeps the
  replica of whichever placement is listed first; any other replica's data
  is orphaned.
- **Boot.** Every boot asks the controller for a grant
  (`POST /v1/hosts/{id}/boot/{installation}`), which commits the next
  generation as its own Raft entry. The request carries the host's own record,
  merged first. There is no cached fallback: a host that can't reach the
  controller doesn't boot. The host journal caches the grant and refuses a
  generation that isn't above its own record.
- **Reports.** The host reads an awake workload's hint every minute (owner
  decision) and sends one batched report a minute (`--floor-report`), with
  only the installations that changed, plus a report at every guest exit and
  clean stop. After the loss of a host, the floor covers only up to its last
  report.
- **Incarnations.** The generation only rises. The floor only rises within an
  incarnation. Reports from an older incarnation contribute only their
  generation, so a host that missed a rollback can't raise the floor back. A
  host record with a newer incarnation means the controller is behind (for
  example, restored from a backup taken before a rollback). The controller
  never takes that host's floor, because doing so would lower the floor
  without the audited action. Instead it refuses the boot (409), and the host
  logs an alert. An operator then re-applies the lowering with
  `target_incarnation` set to the host's incarnation. If the controller has
  no record at all, such a host seeds only the generation, in incarnation 1.
- **Intentional rollback.** `POST /v1/installations/{id}/floor/lower` with
  `expect_incarnation`, `epoch`, `seq`, `requested_by` (the installation
  owner), `approved_by` (a different Zati operator), `reason` and `reference`.
  It records the audit, starts a new incarnation, and sets the floor to the
  given mark exactly. That mark must be the restored lease's (data
  generation, TXID). The controller can't read the lease itself, so the
  restore tooling supplies it. Until the new incarnation records its first
  progress, its start moves up to each generation granted, which is above
  the booting host's own record and verified hints. Hosts ignore hint
  progress from generations before it and only ever raise their copy of the
  start. So a guest still running on the discarded data, on any host, can't
  move the new floor once a later boot has outrun it. `GET /v1/installations/{id}/floor` returns the
  record.
- **Raft.** Floor records live in their own bucket and are written by a
  `boot_floor` command, not by `restore_store`, so placement changes neither
  ship nor overwrite them. Raft snapshots carry both.
- **Authentication.** Without TLS flags the control API takes host IDs and
  lowering names on trust (development only). With mutual TLS (below), hosts
  can call only their own `/v1/hosts/{id}/...` routes, and a floor lowering
  takes two calls by two principals:
  1. `POST /v1/installations/{id}/floor/lower-request` from a product client,
     naming the `owner` it authenticated.
  2. `POST /v1/installations/{id}/floor/lower-approve` from an operator,
     naming the exact request by the `pending_digest` that
     `GET /v1/installations/{id}/floor` returns. A request replaced
     meanwhile, even with the same mark, isn't approved, and a request
     expires after 24 hours.

  The audit records both principals.
Stopping is crash-consistent: workloads must acknowledge only writes that are
already durable off-host. QEMU user networking is a slice simplification, not
the production isolation boundary.

### Replica credentials per installation

With `sufuree controller --replica-credentials replica.json` and a workload
with `replica_credentials`, each guest gets object-store credentials that
reach only its own installation's prefix, `<bucket>/inst/<id>/*`, rather than
one credential shared by every guest. Production PHI workloads need this: with
a shared credential, one compromised guest could read every installation's
replica.

```json
"replica_credentials": {"env": {"access_key_id": "ZATI_REPLICA_ACCESS_KEY", "secret_access_key": "ZATI_REPLICA_SECRET_KEY"}}
```

- **Derivation.** The controller derives each installation's access key and
  secret from one provisioning key with HMAC-SHA256, the key version and the
  installation ID, so its Raft state holds no secrets. Create the key with
  `sufuree replica-key init --out FILE --kms-key-id alias/sufuree-replica-credentials`;
  it exists on disk only wrapped by that KMS key (encryption context
  `purpose=sufuree-replica-credentials`), and the controller unwraps it at
  start. A plaintext key is accepted only with `"development": true`.
- **Registration.** Before returning an installation's credentials the first
  time (and every 10 minutes after), the controller writes its SeaweedFS
  identity, `/etc/iam/identities/sufuree-<id>.json`, through the filer's HTTP
  API with a signed request. It then lists the installation's prefix with the
  new credentials until the S3 servers accept them. If they don't within 10
  seconds, it removes the identity file and the boot fails: one unreadable
  identity file stops the S3 servers reloading any identity.
- **Delivery.** A host gets the credentials at every boot from
  `POST /v1/hosts/{id}/credentials/{installation}`, which serves only an
  installation the host holds, over mutual TLS (the controller refuses
  `--replica-credentials` without TLS flags). A boot fails rather than start
  without them. The host agent needs `--controller` and `--host-id`.
- **Rotation.** Raise `key_version`. Each installation's identity is
  replaced, and its old key revoked, at its next boot, when its previous
  guest has stopped.

`replica.json`:

```json
{"key_file": "/etc/sufuree/replica-key.json", "key_version": 1, "bucket": "zati",
 "filer": {"url": "https://filer:8888", "ca_file": "/etc/sufuree/filer-ca.pem",
           "write_jwt_key_file": "/etc/sufuree/filer-write.jwt", "read_jwt_key_file": "/etc/sufuree/filer-read.jwt"},
 "s3": {"url": "https://s3:8333", "ca_file": "/etc/sufuree/s3-ca.pem", "region": "us-east-1"}}
```

The SeaweedFS deployment must:

- keep small files inline in filer metadata, `-filer.saveToFilerLimit=65536`
  (or at least 4096). The S3 servers' credential manager reads only inline
  content and sees a chunked identity file as empty;
- sign filer reads and writes (`jwt.filer_signing.key` and
  `jwt.filer_signing.read.key` in `security.toml`). Identity files hold
  secrets in plaintext;
- not expose the filer's HTTP or gRPC ports to guests. A QEMU user-network
  guest reaches every port on the host's loopback through `10.0.2.2`, so run
  the filer on another host or behind a firewall rule, not on the guest
  host's loopback;
- keep no static `-s3.config` identities other than administrators, and start
  without AWS credentials in the environment: SeaweedFS 4.48 turns
  `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` into an administrator
  identity.

Verified against SeaweedFS 4.48 (`TestSeaweedFS`): each installation's
credentials read, write, conditionally create and list only its own prefix,
and are refused on another installation's prefix, a neighboring prefix such
as `inst/<id>x/`, and a bucket-wide list. Raising the version revokes the old
key, and an unsigned read of an identity file is refused.

Limits:
- **A host keeps credentials it was given.** An installation that moves
  keeps its credentials until the version is raised, so the previous host
  still holds a working key. Raise the version after removing a host that
  may be compromised.
- **Deleted installations keep their identities.** Removing an identity when
  its installation is deleted is not implemented yet.
- **Wake hints use the host's credentials.** The host reads every hint with
  `wake_hints`'s credentials, which reach every installation's prefix.
