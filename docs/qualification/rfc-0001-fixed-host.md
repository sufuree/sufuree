# RFC-0001 fixed-host guest E2E (no AWS)

RFC sequence §2 on one already-present host: edge admit → wake → ready →
proxy → idle stop → rewake. This does **not** prove host scale-to-zero.

## CI receipt (HTTP guest stand-in)

`guest.HTTP` implements the same `Launcher`/`Guest` contract as `guest.QEMU`
but serves an in-process HTTP listener. Use it when ops/KVM/unikernel images
are unavailable:

```bash
SUFUREE_QUAL_RECEIPT_DIR="$PWD/docs/qualification/receipts" \
  go test ./internal/qual/ -run TestFixedHostGuestWakeProxyIdle -count=1
```

Writes `docs/qualification/receipts/fixed-host-guest-wake-proxy-idle.json`
with `aws_spend: false` and `launcher: guest.HTTP`.

## Pinned QEMU path (live host)

Production hosts still use `guest.QEMU` (`qemu-system-x86_64` + Ops volumes).
Pin and record on the live host before claiming a QEMU receipt:

| Pin | Candidate |
| --- | --- |
| VMM | `qemu-system-x86_64` (KVM) |
| Image builder | Ops / Nanos volume layout in `internal/guest/qemu.go` |
| Guest workload | Stateless Go HTTP image (RFC §1 first slice) |
| Networking | QEMU user-mode hostfwd to `guest_port` |

Live QEMU wake timing and image digests are still open; the HTTP stand-in
qualifies the edge↔activator↔idle contract only.

## Fixed-host edge upstream publication

When the controller edge should forward after demand handoff to a known
host-gateway or guest address (host already running):

```text
sufuree controller ... \
  --edge-listen 127.0.0.1:8081 \
  --edge-upstream web=127.0.0.1:9
```

`--edge-upstream` may be repeated. Keys are App **name** or resource UID.
Co-located edge+host can instead use `edge.HostWake` (see the qual test)
so each request acquires a host-agent lease and wakes the guest directly.
