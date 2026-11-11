# RFC-0001 cloud qualification path (F01+)

Minimum reliable cloud/runtime qualification, in order:

1. **Growth grant + capacity ledger** (`internal/budget`, `executor.Gated`) — done in-tree.
2. **EC2 executor** (`internal/executor/ec2exec`) — Create/Terminate/Describe with `ClientToken`, deployment tags, and `Fence` (terminate + wait terminal). Volume effects still fail closed.
3. **Fixed-host guest E2E** — CI path done (`TestFixedHostGuestWakeProxyIdle` + `guest.HTTP`); live QEMU harness + pin schema ready (`TestFixedHostQEMUWakeProxyIdle`, [rfc-0001-qemu-pin.md](rfc-0001-qemu-pin.md)) — receipt still needs a KVM host + digest-pinned image.
4. **Live 0↔1 + two-host drain receipts** — still open; requires authorized AWS spend.

## Controller flags

```text
sufuree controller \
  --data-dir ./data --listen 127.0.0.1:8080 \
  --tls-cert controller.pem --tls-key controller-key.pem --tls-client-ca ca.pem \
  --deployment dep-qual \
  --executor ec2 \
  --grant-max-hosts 2 --grant-max-memory-mib 4096 --grant-expires 8h \
  --ec2-region us-west-2 \
  --ec2-ami ami-... \
  --ec2-instance-type t3.micro \
  --ec2-subnet subnet-... \
  --ec2-security-group sg-...
```

`--executor ec2` requires mutual TLS on the control API.

`--executor ec2` loads the default AWS credential chain and **will** call
RunInstances / TerminateInstances. Do not pass it without an explicit spend
grant for that deployment.

The growth ledger is **in-memory for this slice**. Restarting the controller
while EC2 instances from a prior process are still live does **not** re-seed
grant usage; do not restart a live `--executor ec2` controller until durable
ledger reconstruction lands, or you risk exceeding `--grant-max-hosts`.

`--executor memory` with the same `--grant-*` flags exercises the ledger
without cloud spend. Without grant flags (both exactly `0`), memory stays
uncapped (dev path). Negative grant flags are rejected.

## Local receipts (no AWS)

```bash
go test ./internal/qual/ ./internal/qual/qemupin/ -run 'TestLocalZeroOneZeroWithGrant|TestLocalRaftMutationsAndBootscope|TestFixedHostGuestWakeProxyIdle|TestLoadValidPin' -count=1
go test ./internal/budget/ ./internal/executor/ ./internal/executor/ec2exec/ -count=1
```

Live QEMU (opt-in, needs KVM + image): see [rfc-0001-qemu-pin.md](rfc-0001-qemu-pin.md).

## Still required before claiming cloud qualification

| Receipt | Status |
| --- | --- |
| Grant-gated 0→1→0 (Memory) | `TestLocalZeroOneZeroWithGrant` |
| EC2 ClientToken idempotency + Fence (fake API) | `ec2exec` tests |
| Fixed-host guest wake/proxy/idle (HTTP stand-in) | `TestFixedHostGuestWakeProxyIdle` |
| Fixed-host QEMU pin schema + opt-in harness | `qemupin` + `TestFixedHostQEMUWakeProxyIdle` (skip without opt-in) |
| Fixed-host guest on pinned QEMU image | not run (needs KVM host + `SUFUREE_QUAL_GUEST_IMAGE`) |
| Live EC2 create/terminate under grant | not run (needs spend auth + AMI/network) |
| Two-host drain / stale owner | not started |
