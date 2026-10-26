#!/usr/bin/env bash
# Capture the EFFECTIVE version, build and query-engine configuration of each
# running MetricsBench comparator as one JSON document, so a run report records
# what actually ran rather than what docker-compose.yml pinned.
#
# Usage:
#   capture-effective-config.sh [--out FILE] [--prometheus URL]
#     [--victoriametrics URL] [--mimir URL] [--ravel URL]
#
# Defaults are the compose ports on loopback: http://127.0.0.1:9090 (Prometheus),
# http://127.0.0.1:8428 (VictoriaMetrics), http://127.0.0.1:9009 (Mimir),
# http://127.0.0.1:4318 (Ravel, from ravel.yml).
# Without --out the document goes to stdout.
#
# What is read, per comparator:
#   prometheus       GET /api/v1/status/buildinfo (version, revision)
#                    GET /api/v1/status/flags     (runtime_flags)
#                    GET /api/v1/status/config    (config_settings: values set
#                    in prometheus.yml that have no flag, today the TSDB
#                    out_of_order_time_window)
#   victoriametrics  GET /metrics, the vm_app_version series (version from its
#                    short_version label, revision from its version label).
#                    Its /api/v1/status/buildinfo answers a fixed
#                    Prometheus-compatibility version, not its own, so it is
#                    not used.
#                    GET /flags                   (runtime_flags: the flags set
#                    on the command line, one -name="value" per line)
#   mimir            GET /prometheus/api/v1/status/buildinfo (version, revision)
#                    GET /config (runtime_flags: the effective YAML, defaults
#                    included). Mimir's /api/v1/status/flags is an empty
#                    compatibility stub, so it is not used.
#                    querier.query_engine is read out of /config, and
#                    query_engine_default_for_version records the documented
#                    default for the reported version: prometheus for
#                    2.14-2.16, mimir from 2.17.
#   ravel            GET /api/v1/status/buildinfo (version, revision). The
#                    ravel-server 0.26.0 image answers an empty revision, which
#                    is recorded as unavailable and makes the capture exit 1;
#                    its image digest is then the build's identity.
#                    runtime_flags: the container's .Config.Cmd from
#                    `docker inspect` (the arguments ravel.yml passes to the
#                    ravel-server entrypoint), as {"source": ..., "cmd": [...]}.
#                    Ravel serves no flags or config endpoint, so the command
#                    line is the only record of its flags, and it is
#                    unavailable when docker is not reachable. Settings in the
#                    mounted limits file are not read; ravel.yml names it.
#   every one        the container's image ID, .Config.Image, RepoDigests,
#                    Architecture and Os, via `docker compose ps` and
#                    `docker inspect`, when docker is reachable; otherwise the
#                    literal string "unavailable: docker not reachable".
#                    `docker compose ps` reads docker-compose.yml and
#                    ravel.yml together, with RAVEL_IMAGE set to a placeholder
#                    when unset (ravel.yml requires it, and a listing pulls
#                    no image), so the ravel-server container is found.
#
# A field that could not be fetched is {"unavailable": "<reason>"}; no field is
# omitted or left empty. The document also records captured_at (UTC ISO 8601),
# script_revision (git HEAD of this checkout) and every URL queried.
#
# Exit codes:
#   0   every comparator answered its version call; the document is complete
#       for version and revision
#   1   at least one comparator's version is unavailable; the document is
#       still written, because a partial capture is evidence
#   64  usage error
#   70  the capture crashed or could not write the document; no document, or
#       a stale one from an earlier run, is at --out
#
# METRICSBENCH_DOCKER names the docker binary (default: docker); the test
# suite points it at a stub.
set -uo pipefail

usage() {
  sed -n '7,8p' "$0" >&2
}

here="$(cd "$(dirname "$0")" && pwd)"
out=""
prom_url="http://127.0.0.1:9090"
vm_url="http://127.0.0.1:8428"
mimir_url="http://127.0.0.1:9009"
ravel_url="http://127.0.0.1:4318"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out | --prometheus | --victoriametrics | --mimir | --ravel)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "capture-effective-config.sh: $1 needs a value" >&2
        usage
        exit 64
      fi
      case "$1" in
        --out) out="$2" ;;
        --prometheus) prom_url="$2" ;;
        --victoriametrics) vm_url="$2" ;;
        --mimir) mimir_url="$2" ;;
        --ravel) ravel_url="$2" ;;
      esac
      shift 2
      ;;
    -h | --help)
      usage
      exit 64
      ;;
    *)
      echo "capture-effective-config.sh: unknown argument: $1" >&2
      usage
      exit 64
      ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "capture-effective-config.sh: python3 is required" >&2
  exit 1
fi

script_revision="$(git -C "${here}" rev-parse HEAD 2>/dev/null)" || script_revision="unavailable"

rc=0
python3 -I - "${here}/docker-compose.yml" "${here}/ravel.yml" "${script_revision}" "${out}" \
  "${prom_url%/}" "${vm_url%/}" "${mimir_url%/}" "${ravel_url%/}" \
  "${METRICSBENCH_DOCKER:-docker}" <<'PY' || rc=$?
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

(
    compose_file,
    ravel_compose_file,
    script_revision,
    out,
    prom_url,
    vm_url,
    mimir_url,
    ravel_url,
    docker,
) = sys.argv[1:10]


def crash_hook(exc_type, exc, tb):
    # A crash must not share exit 1 with a partial capture: 1 means "document
    # written, a version is unavailable"; a crash wrote nothing. 70 is EX_SOFTWARE.
    import traceback

    traceback.print_exception(exc_type, exc, tb)
    sys.stderr.write("capture-effective-config.sh: crashed before writing the document\n")
    sys.stderr.flush()
    os._exit(70)


sys.excepthook = crash_hook
TIMEOUT_S = 10


def unavailable(reason):
    return {"unavailable": reason}


def fetch(url):
    """Return (body, None) or (None, reason)."""
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT_S) as resp:
            return resp.read().decode("utf-8", "replace"), None
    except urllib.error.HTTPError as e:
        return None, f"GET {url}: HTTP {e.code}"
    except Exception as e:  # connection refused, timeout, DNS
        return None, f"GET {url}: {type(e).__name__}: {e}"


def nonempty(value, what, url):
    if isinstance(value, str) and value.strip():
        return value
    return unavailable(f"{what} missing or empty in response from {url}")


def buildinfo_json(url):
    body, err = fetch(url)
    if err:
        return unavailable(err), unavailable(err)
    try:
        data = json.loads(body)["data"]
        if not isinstance(data, dict):
            raise TypeError(f"data is {type(data).__name__}, not an object")
        version, revision = data.get("version"), data.get("revision")
    except Exception as e:
        reason = f"GET {url}: response is not buildinfo JSON: {type(e).__name__}: {e}"
        return unavailable(reason), unavailable(reason)
    return nonempty(version, "data.version", url), nonempty(revision, "data.revision", url)


def flags_json(url):
    body, err = fetch(url)
    if err:
        return unavailable(err)
    try:
        data = json.loads(body)["data"]
    except Exception as e:
        return unavailable(f"GET {url}: response is not flags JSON: {type(e).__name__}")
    if not isinstance(data, dict) or not data:
        return unavailable(f"GET {url}: empty flags map")
    return data


PROM_OOO_WINDOW = re.compile(r'^\s*out_of_order_time_window:\s*(\S+)\s*$', re.M)


def prom_config_settings(url):
    """Settings that live in prometheus.yml rather than on the command line,
    so /api/v1/status/flags cannot show them, read from the loaded config."""
    body, err = fetch(url)
    if err:
        return unavailable(err)
    try:
        yaml_text = json.loads(body)["data"]["yaml"]
        if not isinstance(yaml_text, str):
            raise TypeError(f"data.yaml is {type(yaml_text).__name__}, not a string")
    except Exception as e:
        return unavailable(f"GET {url}: response is not config JSON: {type(e).__name__}: {e}")
    m = PROM_OOO_WINDOW.search(yaml_text)
    return {
        "storage.tsdb.out_of_order_time_window": m.group(1)
        if m
        else unavailable(f"GET {url}: out_of_order_time_window not in the loaded config"),
    }


VM_APP_VERSION = re.compile(r'^vm_app_version\{(?P<labels>[^}]*)\}', re.M)
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def vm_buildinfo(url):
    body, err = fetch(url)
    if err:
        return unavailable(err), unavailable(err)
    m = VM_APP_VERSION.search(body)
    if not m:
        reason = f"GET {url}: no vm_app_version series"
        return unavailable(reason), unavailable(reason)
    labels = dict(LABEL.findall(m.group("labels")))
    return nonempty(labels.get("short_version"), "vm_app_version short_version", url), nonempty(
        labels.get("version"), "vm_app_version version", url
    )


VM_FLAG = re.compile(r'^-([^=\s]+)=(.*)$')


def vm_flags(url):
    body, err = fetch(url)
    if err:
        return unavailable(err)
    flags = {}
    for line in body.splitlines():
        m = VM_FLAG.match(line.strip())
        if not m:
            continue
        value = m.group(2)
        try:
            value = json.loads(value)
        except Exception:
            pass
        flags[m.group(1)] = value
    if not flags:
        return unavailable(f"GET {url}: no -flag=value lines")
    return flags


def mimir_query_engine(config_yaml, url):
    """querier.query_engine out of /config's YAML, without a YAML library."""
    in_querier = False
    for line in config_yaml.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line[0].isspace():
            in_querier = line.rstrip() == "querier:"
            continue
        if in_querier:
            m = re.match(r'^\s+query_engine:\s*(.*?)\s*$', line)
            if m and m.group(1):
                return m.group(1).strip("'\"")
    return unavailable(f"querier.query_engine not present in {url}")


def mimir_default_engine(version):
    if not isinstance(version, str):
        return unavailable("mimir version unavailable")
    m = re.match(r'^v?(\d+)\.(\d+)', version)
    if not m:
        return unavailable(f"cannot parse mimir version {version!r}")
    major, minor = int(m.group(1)), int(m.group(2))
    if (major, minor) >= (2, 17):
        return "mimir"
    if (major, minor) >= (2, 14):
        return "prometheus"
    return unavailable(f"no documented default recorded for mimir {version}")


DOCKER_UNREACHABLE = "unavailable: docker not reachable"


def run_docker(*args, env=None):
    try:
        proc = subprocess.run(
            [docker, *args], capture_output=True, text=True, timeout=30, check=False, env=env
        )
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if proc.returncode != 0:
        return None, f"docker {args[0]} exited {proc.returncode}: {proc.stderr.strip()[:200]}"
    return proc.stdout, None


def compose_containers():
    """Map compose service name to container ID, or return a reason string.

    Every failure is a plain "unavailable: ..." string, never a dict, so that
    image_of can tell a failed listing from a listing that lacks a service and
    the document records the real cause (a daemon that answers `info` but
    whose compose plugin fails is the case that used to read as "no running
    container")."""
    _, err = run_docker("info", "--format", "{{.ServerVersion}}")
    if err:
        return DOCKER_UNREACHABLE
    # ravel.yml refuses interpolation without RAVEL_IMAGE; ps only reads
    # container labels, so a placeholder never names an image that is used.
    env = dict(os.environ)
    if not env.get("RAVEL_IMAGE"):
        env["RAVEL_IMAGE"] = "unset-placeholder-for-compose-ps"
    stdout, err = run_docker(
        "compose", "-f", compose_file, "-f", ravel_compose_file, "ps", "--format", "json", env=env
    )
    if err:
        return f"unavailable: {err}"
    stdout = stdout.strip()
    try:
        if stdout.startswith("["):
            rows = json.loads(stdout)
        else:
            rows = [json.loads(line) for line in stdout.splitlines() if line.strip()]
        return {r.get("Service"): r.get("ID") or r.get("Name") for r in rows}
    except Exception as e:
        return f"unavailable: docker compose ps: unparseable output: {type(e).__name__}: {e}"


def image_of(containers, service):
    if not isinstance(containers, dict):
        return containers
    cid = containers.get(service)
    if not cid:
        return unavailable(f"no running container for compose service {service}")
    stdout, err = run_docker("inspect", cid)
    if err:
        return unavailable(err)
    try:
        c = json.loads(stdout)[0]
    except Exception as e:
        return unavailable(f"docker inspect {cid}: {type(e).__name__}")
    image_id = c.get("Image")
    record = {
        "container": cid,
        "image_id": image_id or unavailable("container has no Image"),
        "config_image": c.get("Config", {}).get("Image") or unavailable("container has no .Config.Image"),
    }
    stdout, err = run_docker("image", "inspect", image_id or "")
    try:
        img = json.loads(stdout)[0] if not err else None
    except Exception as e:
        img, err = None, f"docker image inspect: {type(e).__name__}"
    for key, field in (("repo_digests", "RepoDigests"), ("architecture", "Architecture"), ("os", "Os")):
        if img is None:
            record[key] = unavailable(err)
        else:
            record[key] = img.get(field) or unavailable(f"image has no {field}")
    return record


def ravel_runtime_flags(containers, service):
    """The container's command line: Ravel serves no flags endpoint."""
    source = f"docker inspect .Config.Cmd of compose service {service}"
    if not isinstance(containers, dict):
        return unavailable(f"{source}: {containers}")
    cid = containers.get(service)
    if not cid:
        return unavailable(f"no running container for compose service {service}")
    stdout, err = run_docker("inspect", cid)
    if err:
        return unavailable(err)
    try:
        cmd = json.loads(stdout)[0].get("Config", {}).get("Cmd")
    except Exception as e:
        return unavailable(f"docker inspect {cid}: {type(e).__name__}")
    if not isinstance(cmd, list) or not cmd:
        return unavailable(f"docker inspect {cid}: empty .Config.Cmd")
    return {"source": source, "cmd": cmd}


containers = compose_containers()

urls = {
    "prometheus": {
        "buildinfo": f"{prom_url}/api/v1/status/buildinfo",
        "runtime_flags": f"{prom_url}/api/v1/status/flags",
        "config_settings": f"{prom_url}/api/v1/status/config",
    },
    "victoriametrics": {
        "buildinfo": f"{vm_url}/metrics",
        "runtime_flags": f"{vm_url}/flags",
    },
    "mimir": {
        "buildinfo": f"{mimir_url}/prometheus/api/v1/status/buildinfo",
        "runtime_flags": f"{mimir_url}/config",
    },
    "ravel": {
        "buildinfo": f"{ravel_url}/api/v1/status/buildinfo",
    },
}

doc = {
    "captured_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "script_revision": script_revision,
    "urls_queried": urls,
    "comparators": {},
}

version, revision = buildinfo_json(urls["prometheus"]["buildinfo"])
doc["comparators"]["prometheus"] = {
    "version": version,
    "revision": revision,
    "runtime_flags": flags_json(urls["prometheus"]["runtime_flags"]),
    "config_settings": prom_config_settings(urls["prometheus"]["config_settings"]),
    "image": image_of(containers, "prometheus"),
}

version, revision = vm_buildinfo(urls["victoriametrics"]["buildinfo"])
doc["comparators"]["victoriametrics"] = {
    "version": version,
    "revision": revision,
    "runtime_flags": vm_flags(urls["victoriametrics"]["runtime_flags"]),
    "image": image_of(containers, "victoriametrics"),
}

version, revision = buildinfo_json(urls["mimir"]["buildinfo"])
config_body, config_err = fetch(urls["mimir"]["runtime_flags"])
if config_err:
    runtime_flags = unavailable(config_err)
    engine = unavailable(config_err)
elif not config_body.strip():
    runtime_flags = unavailable(f"GET {urls['mimir']['runtime_flags']}: empty body")
    engine = runtime_flags
else:
    runtime_flags = {"format": "yaml", "effective_config": config_body}
    engine = mimir_query_engine(config_body, urls["mimir"]["runtime_flags"])
doc["comparators"]["mimir"] = {
    "version": version,
    "revision": revision,
    "runtime_flags": runtime_flags,
    "querier_query_engine": engine,
    "query_engine_default_for_version": mimir_default_engine(version),
    "image": image_of(containers, "mimir"),
}

version, revision = buildinfo_json(urls["ravel"]["buildinfo"])
doc["comparators"]["ravel"] = {
    "version": version,
    "revision": revision,
    "runtime_flags": ravel_runtime_flags(containers, "ravel-server"),
    "image": image_of(containers, "ravel-server"),
}

text = json.dumps(doc, indent=2, sort_keys=False) + "\n"
if out:
    try:
        directory = os.path.dirname(os.path.abspath(out))
        fd, tmp = tempfile.mkstemp(dir=directory, prefix=".capture-", suffix=".json")
        with os.fdopen(fd, "w") as f:
            f.write(text)
        os.replace(tmp, out)
    except OSError as e:
        sys.stderr.write(f"capture-effective-config.sh: cannot write {out}: {e}\n")
        sys.exit(70)
else:
    sys.stdout.write(text)

missing = [
    name
    for name, c in doc["comparators"].items()
    if not (isinstance(c["version"], str) and isinstance(c["revision"], str))
]
if missing:
    sys.stderr.write(
        "capture-effective-config.sh: version or revision unavailable for: " + ", ".join(missing) + "\n"
    )
    sys.exit(1)
sys.exit(0)
PY

if [[ ${rc} -ne 0 && ${rc} -ne 1 ]]; then
  echo "capture-effective-config.sh: capture failed before writing a document (python exit ${rc})" >&2
  exit 70
fi
exit "${rc}"
