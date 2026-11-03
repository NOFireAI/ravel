#!/usr/bin/env bash
# investigations.test.sh -- the selective-investigations harness's cases
# (issue #3699). No AWS, no ravel-server, no network; runs in well under two
# minutes. CI's doc-scripts job runs it beside benchmarks/clickbench/kit/kit.test.sh.
#
# It syntax-checks every shell script of the harness, then runs
# investigations_cases.py: gen_corpus.py recounts, statements.toml and
# bands.toml shape, evaluate.py on the committed local-lane run
# (fixtures/local-smoke/, re-evaluated byte for byte) and on mutations of it,
# two wrong evaluators that must fail their cases, and the box/ scripts'
# dry runs. The scripts run, and are syntax-checked, under the bash that runs
# this file, so `/path/to/bash-3.2 investigations.test.sh` checks them under
# bash 3.2.
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
failed=0

while IFS= read -r -d '' script; do
  if ! "$BASH" -n "$script"; then
    echo "FAIL: bash -n ${script#"$HERE"/}" >&2
    failed=1
  fi
done < <(find "$HERE" -name '*.sh' -print0)

python3 -c 'import tomllib' 2>/dev/null || {
  echo "investigations.test.sh: python3 has no tomllib (needs 3.11 or later)" >&2
  exit 2
}

echo "investigations.test.sh: bash $BASH_VERSION at $BASH"
INV_BASH="$BASH" PYTHONDONTWRITEBYTECODE=1 python3 "$HERE/investigations_cases.py" -v || failed=1

if [ "$failed" -ne 0 ]; then
  echo "investigations.test.sh: FAILED" >&2
  exit 1
fi
echo "investigations.test.sh: all cases passed"
