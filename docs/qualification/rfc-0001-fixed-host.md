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

`--edge-upstream` publishes a **lease-holding host gateway** dial address
(App name or UID → `host:port`). The gateway must hold a host activity lease
for the request lifetime. Do **not** point it at a raw guest port: demand
handoff alone does not stop idle shutdown.

```text
sufuree controller ... \
  --edge-listen 127.0.0.1:8081 \
  --edge-upstream web=127.0.0.1:gateway-port
```

Second-hop contract: the published upstream is the host agent's workload
gateway (`gateway.Gateway` / guest dial address after `Activator.Acquire`),
not a nested `edge.Server`. A second `edge.Server` would strip `/svc/<name>/`
again and answer `/healthz` locally without waking the guest. The public
edge removes the service prefix once, then forwards the remaining path to
that host gateway, which already holds the activity lease.

For a co-located public edge that wakes the guest directly (no separate host
gateway hop), use `edge.HostWake` (see the qual test) or
`reconcile.EdgeActivator.HostLeases` so each forward acquires a host-agent
lease and releases it when proxying finishes.
