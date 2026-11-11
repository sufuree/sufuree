# RFC-0001 live QEMU / guest image pin (F05 slice)

Pin one guest kernel/image builder/VMM/architecture before claiming a live
fixed-host QEMU receipt. The CI HTTP stand-in
(`TestFixedHostGuestWakeProxyIdle`) does **not** satisfy this gate.

## Pin file

Canonical pin: [`pins/qemu-guest.json`](pins/qemu-guest.json)
(`schema: sufuree.qemu-guest-pin/v1`).

| Field | Meaning |
| --- | --- |
| `arch` | Guest/host architecture (`x86_64`) |
| `vmm.bin` | QEMU binary (`qemu-system-x86_64`) |
| `ops.bin` | Ops CLI used for TFS volumes |
| `image.sha256` | Digest of the bootable `.raw` image (fill after first good run) |
| `image.guest_port` | Port the workload listens on inside the guest |
| `image.runtime` | Intended runtime (`nanos`) |
| `accel` | Must be `kvm` for `guest.QEMU` today |

Builds and image assembly happen **outside** the ingress (RFC §4). Nanos,
Unikraft, Firecracker and QEMU are not interchangeable formats.

## Build a qualification image (operator)

1. Install Ops and a KVM-capable `qemu-system-x86_64` on the qualification host.
2. Build a minimal stateless Go HTTP unikernel (Nanos via Ops) that listens on
   `guest_port` (default 8080) and answers any path with HTTP 200.
3. Copy the resulting `.raw` image to a stable path; compute SHA-256.
4. Set `image.sha256` in `pins/qemu-guest.json` (or a private pin copy) to that
   digest. Empty `sha256` accepts any image for first measurement only.

## Run the live harness

Opt-in only; skipped in ordinary `go test ./internal/qual`:

```bash
SUFUREE_QUAL_QEMU=1 \
SUFUREE_QUAL_GUEST_IMAGE=/path/to/guest.raw \
SUFUREE_QUAL_RECEIPT_DIR="$PWD/docs/qualification/receipts" \
  go test ./internal/qual/ -run TestFixedHostQEMUWakeProxyIdle -count=1 -timeout 5m
```

Optional: `SUFUREE_QUAL_QEMU_PIN=/path/to/pin.json` overrides the repo pin.

The harness resolves tools and `/dev/kvm`, verifies the image digest when set,
then runs edge → `guest.QEMU` wake → proxy → idle → rewake. Receipt:
`docs/qualification/receipts/fixed-host-qemu-wake-proxy-idle.json`
(`aws_spend: false`, `launcher: guest.QEMU`, versions + digest + wake_ms).

## Still open after a green receipt

- Measured wake/rewake budgets for the pinned image
- Host AMI / nested-virt matrix when the host itself is a cloud VM
- Replacing empty `image.sha256` with the production digest in the repo pin
