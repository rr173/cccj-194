#!/usr/bin/env bash
# End-to-end demo: generate input, run the host on both demo plugins,
# verify output against the independent reference transformer.
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${COLBATCH_PYTHON:-python3}"

echo "== generate fixtures (deterministic) =="
$PY scripts/gen_input.py --profile normalize  --rows 30 --seed 11 --out fixtures/normalize_small.jsonl
$PY scripts/gen_input.py --profile crossfield --rows 30 --seed 12 --out fixtures/crossfield_small.jsonl

echo "== run: normalize_text plugin =="
$PY -m colbatch.cli run --config examples/normalize_run.json
$PY tests/verify.py --config examples/normalize_run.json

echo "== run: crossfield_derive plugin =="
$PY -m colbatch.cli run --config examples/crossfield_run.json
$PY tests/verify.py --config examples/crossfield_run.json

echo "== manifest summary =="
$PY - <<'EOF'
import json
m = json.load(open("out/manifest.json"))
for k in ("status", "input_rows_read", "batches_formed", "delivered_batches",
          "delivered_rows_success", "delivered_rows_failure", "elapsed_ms"):
    print(f"  {k}: {m[k]}")
print("  counters:", json.dumps(m["counters"]))
EOF

echo "== preflight: quality profile (no workers, no output writes) =="
$PY -m colbatch.cli preflight --config examples/quality_run.json > out/preflight_report.json
echo "  report written to out/preflight_report.json (exit $?)"

echo "== gated run: reads the immutable snapshot bound to the report =="
$PY -m colbatch.cli run --config examples/quality_run.json
$PY - <<'EOF'
import json
m = json.load(open("out/manifest.json"))
assert m["input_source"] == "quality_gate_snapshot"
print("  gated run input_source:", m["input_source"])
print("  snapshot sha256:", m["input_snapshot"]["sha256"][:16], "...")
EOF
echo "demo OK"
