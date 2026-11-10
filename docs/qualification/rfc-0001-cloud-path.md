# RFC-0001 cloud qualification path (F01+)

Minimum reliable cloud/runtime qualification, in order:

1. **Growth grant + capacity ledger** (`internal/budget`, `executor.Gated`) — done in-tree.
2. **EC2 executor** (`internal/executor/ec2exec`) — Create/Terminate/Describe with `ClientToken`, deployment tags, and `Fence` (terminate + wait terminal). Volume effects still fail closed.
3. **Fixed-host guest E2E** — still open (pin AMI/QEMU guest, edge→wake→idle).
4. **Live 0↔1 + two-host drain receipts** — still open; requires authorized AWS spend.

## Controller flags

```text
sufuree controller \
  --data-dir ./data --listen 127.0.0.1:8080 \
  --deployment dep-qual \
  --executor ec2 \
  --grant-max-hosts 2 --grant-max-memory-mib 4096 --grant-expires 8h \
  --ec2-region us-west-2 \
  --ec2-ami ami-... \
  --ec2-instance-type t3.micro \
  --ec2-subnet subnet-... \
  --ec2-security-group sg-...
```

`--executor ec2` loads the default AWS credential chain and **will** call
RunInstances / TerminateInstances. Do not pass it without an explicit spend
grant for that deployment.

`--executor memory` with the same `--grant-*` flags exercises the ledger
without cloud spend. Without grant flags, memory stays uncapped (dev path).

## Local receipts (no AWS)

```bash
go test ./internal/qual/ -run 'TestLocalZeroOneZeroWithGrant|TestLocalRaftMutationsAndBootscope' -count=1
go test ./internal/budget/ ./internal/executor/ ./internal/executor/ec2exec/ -count=1
```

## Still required before claiming cloud qualification

| Receipt | Status |
| --- | --- |
| Grant-gated 0→1→0 (Memory) | `TestLocalZeroOneZeroWithGrant` |
| EC2 ClientToken idempotency + Fence (fake API) | `ec2exec` tests |
| Live EC2 create/terminate under grant | not run (needs spend auth + AMI/network) |
| Fixed-host guest wake/proxy/idle | not started |
| Two-host drain / stale owner | not started |
