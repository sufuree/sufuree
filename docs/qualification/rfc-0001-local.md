# RFC-0001 local qualification (no AWS)

This tree can prove control-plane Raft mutations and bootscope assignment /
credential authorize **without cloud spend**. AWS Pulumi/Lambda workers and
measured multi-host F-gates remain open.

## Scenario: `local-raft-mutations-bootscope`

Covered by `go test ./internal/qual/ -run TestLocalRaftMutationsAndBootscope -count=1`.

| Step | Expectation |
| --- | --- |
| One-voter Raft + `controlraft.Journal` | `Update` proposes `restore_store`; FSM `Import` is the only write to `control.db` |
| Memory executor | Demand join → create host → placement (no AWS) |
| `GET /v1/hosts/{id}/assignments` | Active placements include `session_tags.replica=inst/<replica>` |
| `POST /v1/hosts/{id}/credentials/{installation}` | Returns pinned tags when held; **403** when not held |

### Receipt

```bash
SUFUREE_QUAL_RECEIPT_DIR="$PWD/docs/qualification/receipts" \
  go test ./internal/qual/ -run TestLocalRaftMutationsAndBootscope -count=1
```

(`go test` runs with the package directory as cwd; use an absolute receipt dir.)
Writes `docs/qualification/receipts/local-raft-bootscope.json` with `aws_spend: false`.

## Host agent wiring (manual)

```text
sufuree host --config host.json \
  --controller http://127.0.0.1:8080 \
  --host-id <control-plane-host-uid>
```

With both flags, `bootcred.Guarded` authorizes session tags against the
controller before local STS AssumeRole. Omit the flags for the previous
host.json-only trust path.
