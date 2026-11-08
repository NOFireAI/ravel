#!/usr/bin/env bash
# Check a MetricsBench Stage 1 run's figures against a band file (ADR-3694
# Stage 1, issue #3968).
#
# Usage: stage1-check.sh BANDS OUT-DIR
#
# BANDS is a JSON document:
#
#   {"schema": "metricsbench-stage1-bands/1", "profile": "ci",
#    "figures": [{"name": "leaf_count", "file": "stamps.json",
#                 "path": "stage1.leaves.leaf_count", "min": 1, "max": null,
#                 "basis": "why this band"}, ...]}
#
# file is relative to OUT-DIR. path is dot-separated keys; a segment
# key[field=value] selects the one element of the list under key whose field
# equals value (compared as text). min and max are numbers or null for no
# bound, at least one of them a number; basis is required. The band file's
# profile must be the run's (stamps.json's profile).
#
# Each figure must be present exactly once and inside [min, max]:
#   missing     the file, a key or a list element is absent, or the file is
#               not JSON
#   duplicated  a key on the path occurs twice in its object, a selector
#               matches more than one element, or the band file names the
#               figure twice
#   out of band the value is not a number (a bool or a string is not), or
#               lies outside [min, max]
#
# Exit 0 prints one line per figure: ok NAME = VALUE in [MIN, MAX] (FILE PATH).
# Exit 1 prints one line per failing figure on stderr, then a count.
# Exit 64 is bad usage; 65 a band file that cannot be used or whose profile is
# not the run's.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: stage1-check.sh BANDS OUT-DIR" >&2
  exit 64
fi
if [ ! -d "$2" ]; then
  echo "stage1-check: $2 is not a directory" >&2
  exit 64
fi

exec python3 -I - "$1" "$2" <<'PY'
import json, math, os, re, sys

bands_path, out_dir = sys.argv[1], sys.argv[2]
SCHEMA = "metricsbench-stage1-bands/1"


class Obj(dict):
    """A JSON object that remembers which of its keys occurred more than once."""
    dups = frozenset()


def pairs(items):
    o = Obj()
    seen, dups = set(), set()
    for k, v in items:
        if k in seen:
            dups.add(k)
        seen.add(k)
        o[k] = v
    o.dups = frozenset(dups)
    return o


def refuse(msg):
    print(f"stage1-check: {bands_path}: {msg}", file=sys.stderr)
    sys.exit(65)


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and not (isinstance(v, float) and math.isnan(v))


try:
    with open(bands_path) as f:
        bands = json.load(f, object_pairs_hook=pairs)
except (OSError, ValueError) as e:
    refuse(f"not a readable JSON document ({type(e).__name__}: {e})")
if not isinstance(bands, dict) or bands.get("schema") != SCHEMA:
    refuse(f"schema is not {SCHEMA}")
if bands.dups:
    refuse("duplicate top-level keys: " + ", ".join(sorted(bands.dups)))
figures = bands.get("figures")
if not isinstance(figures, list) or not figures:
    refuse("figures is not a non-empty list")
for i, fig in enumerate(figures):
    if not isinstance(fig, dict):
        refuse(f"figure {i} is not an object")
    for k in ("name", "file", "path", "basis"):
        if not isinstance(fig.get(k), str) or not fig[k]:
            refuse(f"figure {i} has no {k}")
    for k in ("min", "max"):
        if k not in fig or not (fig[k] is None or is_number(fig[k])):
            refuse(f"figure {fig['name']}: {k} is not a number or null")
    if fig["min"] is None and fig["max"] is None:
        refuse(f"figure {fig['name']}: neither min nor max is set")
    if fig["min"] is not None and fig["max"] is not None and fig["min"] > fig["max"]:
        refuse(f"figure {fig['name']}: min {fig['min']} is above max {fig['max']}")

try:
    with open(os.path.join(out_dir, "stamps.json")) as f:
        run_profile = json.load(f).get("profile")
except (OSError, ValueError, AttributeError) as e:
    refuse(f"the run's profile cannot be read from {out_dir}/stamps.json ({type(e).__name__})")
if bands.get("profile") != run_profile:
    refuse(f"the band file is for profile {bands.get('profile')!r}, the run is {run_profile!r}")

SEGMENT = re.compile(r"^([^\[\]]+)(?:\[([^=\[\]]+)=([^\[\]]*)\])?$")
docs = {}


def load(name):
    if name not in docs:
        try:
            with open(os.path.join(out_dir, name)) as f:
                docs[name] = (json.load(f, object_pairs_hook=pairs), None)
        except FileNotFoundError:
            docs[name] = (None, f"{name} does not exist")
        except (OSError, ValueError) as e:
            docs[name] = (None, f"{name} is not readable JSON ({type(e).__name__})")
    return docs[name]


def resolve(doc, path):
    """(value, None) or (None, (kind, why))."""
    node, walked = doc, []
    for seg in path.split("."):
        m = SEGMENT.match(seg)
        if not m:
            return None, ("missing", f"path segment {seg!r} does not parse")
        key, field, want = m.groups()
        here = ".".join(walked) or "the top level"
        if not isinstance(node, dict):
            return None, ("missing", f"{here} is {type(node).__name__} {json.dumps(node)[:120]}, not an object")
        if key in node.dups:
            return None, ("duplicated", f"{key} occurs more than once in {here}")
        if key not in node:
            return None, ("missing", f"no {key} in {here}")
        node = node[key]
        walked.append(key)
        if field is not None:
            if not isinstance(node, list):
                return None, ("missing", f"{'.'.join(walked)} is not a list")
            hits = [e for e in node if isinstance(e, dict) and field in e and str(e[field]) == want]
            if not hits:
                return None, ("missing", f"no element of {'.'.join(walked)} has {field}={want}")
            if len(hits) > 1:
                return None, ("duplicated", f"{len(hits)} elements of {'.'.join(walked)} have {field}={want}")
            node = hits[0]
            walked[-1] = seg
    return node, None


def fmt(v):
    return "null" if v is None else json.dumps(v)


counts = {}
for fig in figures:
    counts[fig["name"]] = counts.get(fig["name"], 0) + 1

ok, bad = [], []
for fig in figures:
    name, where = fig["name"], f"({fig['file']} {fig['path']})"
    if counts[name] > 1:
        bad.append(f"duplicated {name}: the band file names it {counts[name]} times {where}")
        continue
    doc, err = load(fig["file"])
    if err:
        bad.append(f"missing {name}: {err} {where}")
        continue
    value, why = resolve(doc, fig["path"])
    if why:
        bad.append(f"{why[0]} {name}: {why[1]} {where}")
        continue
    band = f"[{fmt(fig['min'])}, {fmt(fig['max'])}]"
    if not is_number(value):
        bad.append(f"out of band {name} = {json.dumps(value)[:120]}: not a number, band {band} {where}")
    elif (fig["min"] is not None and value < fig["min"]) or (fig["max"] is not None and value > fig["max"]):
        bad.append(f"out of band {name} = {fmt(value)} not in {band} {where}")
    else:
        ok.append(f"ok {name} = {fmt(value)} in {band} {where}")

if bad:
    for line in bad:
        print(line, file=sys.stderr)
    print(f"stage1-check: {len(bad)} of {len(figures)} figures failed", file=sys.stderr)
    sys.exit(1)
for line in ok:
    print(line)
PY
