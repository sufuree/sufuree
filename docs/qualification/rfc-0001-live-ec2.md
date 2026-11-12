# RFC-0001 live EC2 0↔1 (authorized spend)

Opt-in create → fence under a **1-host** growth grant. This is cloud sequence
§3 for capacity, not guest wake and not two-host drain.

## Pin

[`pins/ec2-live-us-west-1.json`](pins/ec2-live-us-west-1.json)
(`schema: sufuree.ec2-live-pin/v1`).

| Field | First receipt choice |
| --- | --- |
| region | `us-west-1` |
| ami | Amazon Linux 2023 x86_64 (pinned in JSON) |
| instance_type | `t3.micro` |
| subnet / sg | default VPC `us-west-1a` + default SG |
| grant_max_hosts | **1** (harness refuses higher) |

## Run

Uses the default AWS credential chain. **Will call RunInstances and
TerminateInstances.**

```bash
SUFUREE_QUAL_EC2=1 \
SUFUREE_QUAL_RECEIPT_DIR="$PWD/docs/qualification/receipts" \
  go test ./internal/qual/ -run TestLiveEC2ZeroOneZero -count=1 -timeout 25m
```

Optional: `SUFUREE_QUAL_EC2_PIN=/path/to/pin.json`.

Receipt: `docs/qualification/receipts/live-ec2-zero-one-zero.json`
(`aws_spend: true`). Do not restart a live `--executor ec2` controller with
in-memory ledger while instances remain (see cloud-path doc).

## Still open

- Two-host drain / stale owner
- Durable ledger restart seed
- Guest-on-EC2 (QEMU pin on the launched host)
