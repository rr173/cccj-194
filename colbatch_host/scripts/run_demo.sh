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
echo "demo OK"
