#!/usr/bin/env bash
# Cases for scripts/ci-create-bucket.sh (issues #2036 and #2257). The script
# runs the aws CLI, so each case puts a stub `aws` first on PATH that logs every
# call. create-bucket answers one scripted response per call (exit status and
# stderr); every other call answers what a compliant bucket would, unless the
# case overrides it. A green CI run only ever takes the clean-create path with
# every read-back passing, so the retry classification and each read-back
# failure are covered here or nowhere. The cases at the end run the create Job
# of deploy/k8s/floci.yaml, the other launcher that checks a lifecycle
# document in shell, against a stub curl.
#
# Run: bash scripts/ci-create-bucket.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRIPT="${HERE}/ci-create-bucket.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/ci-create-bucket-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

STUB_BIN="${TMP}/stub-bin"
mkdir -p "${STUB_BIN}"
# create-bucket response file: one line per call, "<exit status><TAB><stderr
# text>". A call past the last line repeats the last line.
#
# Every other call is keyed by its --query value, or by its subcommand when it
# has none, and answers the compliant value for that key. Override file: one
# line per override, "<key substring>[@<n>]<TAB><exit status>[:<error
# code>]<TAB><stdout>". A key written "=<key>" matches only that whole key, so
# "=Status" names the versioning read and not the lifecycle query that also
# mentions Status. "@<n>" limits the override to the key's nth call; the
# error code (default StubbedFailure) is the one a failing call names on
# stderr; each "|" in stdout stands for a tab, since the lifecycle read-back
# prints tab-separated fields.
cat >"${STUB_BIN}/aws" <<'STUB'
#!/usr/bin/env bash
echo "$*" >>"${FAKE_AWS_LOG}"
sub=
query=
prev=
for arg in "$@"; do
  [ "${prev}" = s3api ] && sub=${arg}
  [ "${prev}" = --query ] && query=${arg}
  prev=${arg}
done
if [ "${sub}" = create-bucket ]; then
  n=$(( $(cat "${FAKE_AWS_CALLS}") + 1 ))
  echo "${n}" >"${FAKE_AWS_CALLS}"
  line=$(sed -n "${n}p" "${FAKE_AWS_RESPONSES}")
  [ -z "${line}" ] && line=$(tail -n 1 "${FAKE_AWS_RESPONSES}")
  rc=${line%%$'\t'*}
  msg=${line#*$'\t'}
  [ -n "${msg}" ] && echo "${msg}" >&2
  exit "${rc}"
fi
key=${query:-${sub}}
n=$(( $(cat "${FAKE_AWS_CALLS}.${sub}" 2>/dev/null || echo 0) + 1 ))
echo "${n}" >"${FAKE_AWS_CALLS}.${sub}"
case "${key}" in
  ObjectLockConfiguration.ObjectLockEnabled | Status) out=Enabled ;;
  *'length(Rules)'*) out=${FAKE_AWS_LIFECYCLE_OK} ;;
  *) out= ;;
esac
rc=0
code=StubbedFailure
while IFS=$'\t' read -r match orc oout; do
  only=
  if [[ "${match}" == *@* ]]; then
    only=${match##*@}
    match=${match%@*}
  fi
  hit=0
  case "${match}" in
    '') ;;
    =*) [ "${key}" = "${match#=}" ] && hit=1 ;;
    *) [[ "${key}" == *"${match}"* ]] && hit=1 ;;
  esac
  if [ "${hit}" = 1 ] && { [ -z "${only}" ] || [ "${only}" = "${n}" ]; }; then
    rc=${orc%%:*}
    [[ "${orc}" == *:* ]] && code=${orc#*:}
    out=${oout}
  fi
done <"${FAKE_AWS_OVERRIDES}"
[ -n "${out}" ] && echo "${out//|/$'\t'}"
[ "${rc}" -ne 0 ] && echo "An error occurred (${code}) when calling ${sub}" >&2
exit "${rc}"
STUB
chmod +x "${STUB_BIN}/aws"

CONN=$'255\tCould not connect to the endpoint URL: "http://127.0.0.1:9000/b"'
OWNED=$'254\tAn error occurred (BucketAlreadyOwnedByYou) when calling the CreateBucket operation: owned'
BADKEY=$'254\tAn error occurred (InvalidAccessKeyId) when calling the CreateBucket operation: bad key'
SLOW=$'254\tAn error occurred (SlowDown) when calling the CreateBucket operation: slow down'
UNAVAIL=$'254\tAn error occurred (ServiceUnavailable) when calling the CreateBucket operation: busy'
OK=$'0\t'

# The lifecycle read-back's fields for the compliant rule: rule count, Status,
# scope, expired delete markers, NoncurrentDays, DaysAfterInitiation.
WHOLE='{"f":{"Prefix":""},"p":null}'
LIFECYCLE_OK="1|Enabled|${WHOLE}|true|1|7"

# The stub answers a bucket that already carries every setting, so a run after
# the create reads Object Lock, versioning and the lifecycle once each and puts
# nothing.
AFTER_CREATE=3

# A fresh bucket: versioning reads unset and the lifecycle configuration absent
# on the first read, so both are put and read back. After the create: Object
# Lock, versioning, its put and read-back, lifecycle, its put and read-back.
FRESH=$'=Status@1\t0\tNone\nlength(Rules)@1\t254:NoSuchLifecycleConfiguration\t'
FRESH_AFTER_CREATE=7

# Overrides for the next case_ call, one "<key>\t<exit>\t<stdout>" per line.
override=

# case_ <name> <want exit> <want create calls> <want total calls> <response line>...
case_() {
  local name=$1 want_rc=$2 want_creates=$3 want_total=$4
  shift 4
  local dir="${TMP}/${name}"
  mkdir -p "${dir}"
  printf '%s\n' "$@" >"${dir}/responses"
  printf '%s\n' "${override}" >"${dir}/overrides"
  override=
  echo 0 >"${dir}/calls"
  : >"${dir}/log"
  local rc=0
  PATH="${STUB_BIN}:${PATH}" \
    FAKE_AWS_RESPONSES="${dir}/responses" FAKE_AWS_CALLS="${dir}/calls" \
    FAKE_AWS_OVERRIDES="${dir}/overrides" FAKE_AWS_LOG="${dir}/log" \
    FAKE_AWS_LIFECYCLE_OK="${LIFECYCLE_OK}" CI_CREATE_BUCKET_BACKOFF_SECONDS=0 \
    bash "${SCRIPT}" http://127.0.0.1:9000 b >"${dir}/out" 2>&1 || rc=$?
  local creates total unlocked
  creates=$(cat "${dir}/calls")
  total=$(wc -l <"${dir}/log" | tr -d ' ')
  unlocked=$(grep ' create-bucket ' "${dir}/log" | grep -vc -- '--object-lock-enabled-for-bucket')
  if [ "${rc}" = "${want_rc}" ] && [ "${creates}" = "${want_creates}" ] &&
    [ "${total}" = "${want_total}" ] && [ "${unlocked}" = 0 ]; then
    passes=$((passes + 1))
  else
    fails=$((fails + 1))
    echo "FAIL ${name}: want exit ${want_rc} after ${want_creates} create(s) and ${want_total} call(s)," \
      "got exit ${rc} after ${creates} and ${total}, ${unlocked} create(s) without Object Lock"
    sed 's/^/    /' "${dir}/out"
  fi
}

# log_has <case name> <fixed string>: the case's aws call log has a line
# containing the string.
log_has() {
  if grep -qF -- "$2" "${TMP}/$1/log"; then passes=$((passes + 1)); else
    fails=$((fails + 1))
    echo "FAIL $1: no aws call containing: $2"
    sed 's/^/    /' "${TMP}/$1/log"
  fi
}

# log_lacks <case name> <fixed string>: no aws call in the case's log contains
# the string.
log_lacks() {
  if grep -qF -- "$2" "${TMP}/$1/log"; then
    fails=$((fails + 1))
    echo "FAIL $1: an aws call contains: $2"
    sed 's/^/    /' "${TMP}/$1/log"
  else passes=$((passes + 1)); fi
}

override="${FRESH}"
case_ clean-create 0 1 $((1 + FRESH_AFTER_CREATE)) "${OK}"
case_ bad-credentials-fail-at-once 254 1 1 "${BADKEY}"
case_ already-owned-on-first-call-succeeds 0 1 $((1 + AFTER_CREATE)) "${OWNED}"
case_ connection-error-then-created 0 2 $((2 + AFTER_CREATE)) "${CONN}" "${OK}"
case_ connection-error-then-already-owned 0 2 $((2 + AFTER_CREATE)) "${CONN}" "${OWNED}"
case_ slowdown-is-retried 0 2 $((2 + AFTER_CREATE)) "${SLOW}" "${OK}"
case_ service-unavailable-is-retried 0 3 $((3 + AFTER_CREATE)) "${UNAVAIL}" "${UNAVAIL}" "${OK}"
case_ persistent-connection-error-gives-up 1 5 5 "${CONN}"
case_ transient-then-permanent-fails-with-its-code 254 2 2 "${CONN}" "${BADKEY}"

# A fresh bucket gets both puts, each after its read.
log_has clean-create 'put-bucket-versioning --bucket b --versioning-configuration Status=Enabled'
log_has clean-create 'put-bucket-lifecycle-configuration --bucket b --lifecycle-configuration {"Rules":[{"ID":"ravel","Filter":{"Prefix":""},"Status":"Enabled","Expiration":{"ExpiredObjectDeleteMarker":true},"NoncurrentVersionExpiration":{"NoncurrentDays":1},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
clean_sequence=$(awk '{ printf "%s ", $4 }' "${TMP}/clean-create/log")
want_sequence='create-bucket get-object-lock-configuration get-bucket-versioning put-bucket-versioning get-bucket-versioning get-bucket-lifecycle-configuration put-bucket-lifecycle-configuration get-bucket-lifecycle-configuration '
if [ "${clean_sequence}" = "${want_sequence}" ]; then passes=$((passes + 1)); else
  fails=$((fails + 1)); echo "FAIL clean-create: want calls ${want_sequence}, got ${clean_sequence}"
fi

# An existing bucket already Enabled and already carrying the rule issues no
# put: RustFS answers InternalError to a second PutBucketVersioning on an Object
# Lock bucket, so a rerun that put again would fail. Each setting is read once.
case_ existing-settled-bucket-issues-no-put 0 1 $((1 + AFTER_CREATE)) "${OWNED}"
log_lacks existing-settled-bucket-issues-no-put ' put-'
for sub in get-object-lock-configuration get-bucket-versioning get-bucket-lifecycle-configuration; do
  reads=$(grep -c " ${sub} " "${TMP}/existing-settled-bucket-issues-no-put/log")
  if [ "${reads}" = 1 ]; then passes=$((passes + 1)); else
    fails=$((fails + 1)); echo "FAIL existing-settled-bucket-issues-no-put: want 1 ${sub}, got ${reads}"
  fi
done
# Only the setting that does not already match is put.
override=$'length(Rules)@1\t254:NoSuchLifecycleConfiguration\t'
case_ existing-versioned-bucket-puts-only-the-lifecycle 0 1 $((1 + 5)) "${OWNED}"
log_lacks existing-versioned-bucket-puts-only-the-lifecycle 'put-bucket-versioning'
override=$'=Status@1\t0\tSuspended'
case_ existing-suspended-bucket-puts-only-versioning 0 1 $((1 + 5)) "${OWNED}"
log_lacks existing-suspended-bucket-puts-only-versioning 'put-bucket-lifecycle-configuration'
override=$'length(Rules)@1\t0\t'"1|Enabled|${WHOLE}|true|30|7"
case_ existing-bucket-with-other-rule-gets-the-lifecycle-put 0 1 $((1 + 5)) "${OWNED}"
log_has existing-bucket-with-other-rule-gets-the-lifecycle-put 'put-bucket-lifecycle-configuration'

# A put that fails non-transiently stops the run at once with its exit status.
override="${FRESH}"$'\nput-bucket-versioning\t254\t'
case_ versioning-put-fails 254 1 4 "${OK}"
override="${FRESH}"$'\nput-bucket-lifecycle-configuration\t254\t'
case_ lifecycle-put-fails 254 1 7 "${OK}"
override="${FRESH}"$'\nput-bucket-versioning\t254:AccessDenied\t'
case_ versioning-put-access-denied-fails-at-once 254 1 4 "${OK}"
override="${FRESH}"$'\nput-bucket-lifecycle-configuration\t254:MalformedXML\t'
case_ lifecycle-put-malformed-fails-at-once 254 1 7 "${OK}"

# A put that fails transiently is retried like the create, and one that keeps
# failing transiently gives up after the same five attempts.
override="${FRESH}"$'\nput-bucket-versioning@1\t254:ServiceUnavailable\t'
case_ versioning-put-unavailable-is-retried 0 1 $((2 + FRESH_AFTER_CREATE)) "${OK}"
override="${FRESH}"$'\nput-bucket-versioning@1\t255\t\nput-bucket-versioning@2\t254:InternalError\t'
case_ versioning-put-connection-error-is-retried 0 1 $((3 + FRESH_AFTER_CREATE)) "${OK}"
override="${FRESH}"$'\nput-bucket-lifecycle-configuration@1\t254:ServiceUnavailable\t'
case_ lifecycle-put-unavailable-is-retried 0 1 $((2 + FRESH_AFTER_CREATE)) "${OK}"
override="${FRESH}"$'\nput-bucket-lifecycle-configuration@1\t254:SlowDown\t\nput-bucket-lifecycle-configuration@2\t254:AccessDenied\t'
case_ lifecycle-put-transient-then-permanent-fails 254 1 8 "${OK}"
override="${FRESH}"$'\nput-bucket-versioning\t254:ServiceUnavailable\t'
case_ versioning-put-persistently-unavailable-gives-up 1 1 8 "${OK}"
override="${FRESH}"$'\nput-bucket-lifecycle-configuration\t255\t'
case_ lifecycle-put-persistent-connection-error-gives-up 1 1 11 "${OK}"

# Each read that does not confirm its setting fails the run, and the run stops
# at the first one. A bucket without Object Lock is refused before anything is
# put on it.
override=$'ObjectLockEnabled\t254\t'
case_ existing-bucket-without-object-lock-fails 1 1 2 "${OWNED}"
override=$'ObjectLockEnabled\t0\tNone'
case_ object-lock-not-enabled-fails 1 1 2 "${OK}"
log_lacks object-lock-not-enabled-fails ' put-'
override=$'=Status\t0\tSuspended'
case_ versioning-suspended-fails 1 1 5 "${OK}"
override=$'=Status\t254\t'
case_ versioning-read-error-fails 1 1 3 "${OK}"

# lifecycle_case_ <name> <want exit> <read fields>: a run whose lifecycle reads
# all answer the given fields ("|" between them). A passing document is read
# once and never put; a failing one is put and read back once more.
lifecycle_case_() {
  override="length(Rules)"$'\t0\t'"$3"
  if [ "$2" = 0 ]; then
    case_ "$1" "$2" 1 $((1 + AFTER_CREATE)) "${OK}"
  else
    case_ "$1" "$2" 1 $((3 + AFTER_CREATE)) "${OK}"
  fi
}
override=$'length(Rules)\t254\t'
case_ lifecycle-read-error-fails 1 1 $((1 + AFTER_CREATE)) "${OK}"
lifecycle_case_ disabled-rule-fails 1 "1|Disabled|${WHOLE}|true|1|7"
lifecycle_case_ second-rule-fails 1 "2|Enabled|${WHOLE}|true|1|7"
lifecycle_case_ prefixed-filter-fails 1 '1|Enabled|{"f":{"Prefix":"t/"},"p":null}|true|1|7'
lifecycle_case_ legacy-prefixed-rule-fails 1 '1|Enabled|{"f":null,"p":"t/"}|true|1|7'
lifecycle_case_ tag-filter-fails 1 '1|Enabled|{"f":{"Tag":{"Key":"k","Value":"v"}},"p":null}|true|1|7'
lifecycle_case_ no-filter-and-no-prefix-fails 1 '1|Enabled|{"f":null,"p":null}|true|1|7'
lifecycle_case_ no-rules-fails 1 '0|null|{"f":null,"p":null}|null|null|null'
lifecycle_case_ missing-expired-delete-marker-fails 1 "1|Enabled|${WHOLE}|null|1|7"
lifecycle_case_ wrong-noncurrent-days-fails 1 "1|Enabled|${WHOLE}|true|30|7"
lifecycle_case_ missing-multipart-abort-fails 1 "1|Enabled|${WHOLE}|true|1|null"
# The other whole-bucket spellings the server's rule parser accepts pass.
lifecycle_case_ empty-filter-passes 0 '1|Enabled|{"f":{},"p":null}|true|1|7'
lifecycle_case_ and-empty-prefix-passes 0 '1|Enabled|{"f":{"And":{"Prefix":""}},"p":null}|true|1|7'
lifecycle_case_ legacy-empty-prefix-passes 0 '1|Enabled|{"f":null,"p":""}|true|1|7'

# A read that fails transiently is retried like the puts, and one that keeps
# failing transiently gives up after the same five attempts.
override=$'ObjectLockEnabled@1\t254:ServiceUnavailable\t'
case_ object-lock-read-unavailable-is-retried 0 1 $((2 + AFTER_CREATE)) "${OK}"
override=$'length(Rules)@1\t255\t\nlength(Rules)@2\t254:SlowDown\t'
case_ lifecycle-read-transient-is-retried 0 1 $((3 + AFTER_CREATE)) "${OK}"
override=$'ObjectLockEnabled\t254:InternalError\t'
case_ object-lock-read-persistently-failing-gives-up 1 1 6 "${OK}"

# --- deploy/k8s/floci.yaml's create Job ------------------------------------
#
# The Job's shell is cut out of the manifest (the block scalar under the
# create-bucket container's `- |`, dedented) and run against a stub curl that
# answers a compliant bucket and serves the case's lifecycle document. It runs
# under busybox sh with busybox grep, sed, tr and wc first on PATH when busybox is
# installed, as in the curlimages/curl image.
FLOCI_YAML="${HERE}/../deploy/k8s/floci.yaml"
FLOCI_SCRIPT="${TMP}/floci-create-bucket.sh"
in_container=0
in_script=0
: >"${FLOCI_SCRIPT}"
while IFS= read -r line; do
  if [ "${in_script}" = 1 ]; then
    case "${line}" in
      '              '*) printf '%s\n' "${line:14}" >>"${FLOCI_SCRIPT}" ;;
      '') printf '\n' >>"${FLOCI_SCRIPT}" ;;
      *) break ;;
    esac
  elif [ "${line}" = '        - name: create-bucket' ]; then
    in_container=1
  elif [ "${in_container}" = 1 ] && [ "${line}" = '            - |' ]; then
    in_script=1
  fi
done <"${FLOCI_YAML}"
if grep -q 'bucket ${BUCKET} ready' "${FLOCI_SCRIPT}"; then passes=$((passes + 1)); else
  fails=$((fails + 1)); echo "FAIL floci: could not cut the create Job's shell out of ${FLOCI_YAML}"
fi

FLOCI_BIN="${TMP}/floci-bin"
mkdir -p "${FLOCI_BIN}"
# The stub curl serves one bucket from the files in FLOCI_STATE: "bucket" and
# "lock" exist once the bucket does and has Object Lock, and "versioning" and
# "lifecycle" hold the stored documents (an absent lifecycle answers 404
# NoSuchLifecycleConfiguration). A PUT is stored only when FLOCI_PUTS_STICK is
# set; otherwise it answers 200 and changes nothing, so a refused document reads
# back unchanged. Every call is logged to FLOCI_LOG as "<method> <path>".
cat >"${FLOCI_BIN}/curl" <<'STUB'
#!/usr/bin/env bash
url=
head=0
method=GET
fail=0
out=1
lock=0
data=
wfmt=
prev=
for arg in "$@"; do
  case "${arg}" in
    http://*) url=${arg} ;;
    -I) head=1 ;;
    -fsS) fail=1 ;;
    *object-lock-enabled:*) lock=1 ;;
  esac
  [ "${prev}" = -X ] && method=${arg}
  [ "${prev}" = -o ] && out=0
  [ "${prev}" = --data-binary ] && data=${arg}
  [ "${prev}" = -w ] && wfmt=${arg}
  prev=${arg}
done
[ "${head}" = 1 ] && method=HEAD
s=${FLOCI_STATE}
echo "${method} ${url#http://floci.ravel-system.svc:4566}" >>"${FLOCI_LOG}"
code=200
body=
case "${method} ${url}" in
  *'/_floci/health') body='{"services":{"s3":"running"}}' ;;
  'HEAD '*) [ -e "${s}/bucket" ] || code=404 ;;
  'PUT '*'?versioning' | 'PUT '*'?lifecycle')
    [ -z "${FLOCI_PUTS_STICK:-}" ] || printf '%s' "${data}" >"${s}/${url##*\?}" ;;
  'PUT '*)
    if [ -n "${FLOCI_PUTS_STICK:-}" ] && [ ! -e "${s}/bucket" ]; then
      : >"${s}/bucket"
      [ "${lock}" = 0 ] || : >"${s}/lock"
    fi ;;
  'GET '*'?object-lock')
    if [ -e "${s}/lock" ]; then
      body='<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>'
    else
      code=404
      body='<Error><Code>ObjectLockConfigurationNotFoundError</Code></Error>'
    fi ;;
  'GET '*'?versioning')
    body=$(cat "${s}/versioning" 2>/dev/null) ||
      body='<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>' ;;
  'GET '*'?lifecycle')
    if ! body=$(cat "${s}/lifecycle" 2>/dev/null); then
      code=404
      body='<Error><Code>NoSuchLifecycleConfiguration</Code></Error>'
    fi ;;
esac
if [ "${fail}" = 1 ] && [ "${code}" -ge 400 ]; then
  echo "curl: (22) The requested URL returned error: ${code}" >&2
  exit 22
fi
[ "${out}" = 0 ] || printf '%s\n' "${body}"
[ -z "${wfmt}" ] || printf '%b' "${wfmt/'%{http_code}'/${code}}"
exit 0
STUB
chmod +x "${FLOCI_BIN}/curl"
# The health wait never sleeps in a case: a stub that answers it wrong fails
# after 60 quick polls rather than two minutes.
printf '#!/bin/sh\nexit 0\n' >"${FLOCI_BIN}/sleep"
chmod +x "${FLOCI_BIN}/sleep"
floci_sh=(sh)
if command -v busybox >/dev/null 2>&1; then
  floci_sh=(busybox sh)
  for applet in grep sed tr wc; do ln -s "$(command -v busybox)" "${FLOCI_BIN}/${applet}"; done
else
  echo "note: busybox not installed; the floci cases run under sh with the host's grep, sed, tr and wc"
fi

RULE_ACTIONS='<Status>Enabled</Status><Expiration><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration><NoncurrentVersionExpiration><NoncurrentDays>1</NoncurrentDays></NoncurrentVersionExpiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>7</DaysAfterInitiation></AbortIncompleteMultipartUpload>'

VERSIONED='<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>'

# floci_run_ <dir> [VAR=value ...]: run the Job's shell once against the state
# in <dir>/state, appending to <dir>/log and writing <dir>/out; prints the exit
# status.
floci_run_() {
  local dir=$1 rc=0
  shift
  : >"${dir}/out"
  env PATH="${FLOCI_BIN}:${PATH}" FLOCI_STATE="${dir}/state" FLOCI_LOG="${dir}/log" \
    BUCKET=ravel ENDPOINT=http://floci.ravel-system.svc:4566 "$@" \
    "${floci_sh[@]}" "${FLOCI_SCRIPT}" >"${dir}/out" 2>&1 || rc=$?
  printf '%s\n' "${rc}"
}

# floci_case_ <name> <want exit> <lifecycle document>: a run against an existing
# Object Lock bucket with versioning Enabled that stores the given lifecycle
# document. A passing run prints the ready line last and issues no PUT, and a
# refused one ends on the scope refusal, so a case cannot pass on some other
# check's verdict.
floci_case_() {
  local name=$1 want_rc=$2 dir="${TMP}/floci-$1" rc want_line puts
  mkdir -p "${dir}/state"
  : >"${dir}/state/bucket"
  : >"${dir}/state/lock"
  printf '%s\n' "${VERSIONED}" >"${dir}/state/versioning"
  printf '%s\n' "$3" >"${dir}/state/lifecycle"
  : >"${dir}/log"
  rc=$(floci_run_ "${dir}")
  puts=$(grep -c '^PUT ' "${dir}/log")
  want_line='bucket ravel ready'
  [ "${want_rc}" = 0 ] || want_line='lifecycle rule is not scoped to the whole bucket'
  if [ "${rc}" = "${want_rc}" ] && tail -n 1 "${dir}/out" | grep -qF "${want_line}" &&
    { [ "${want_rc}" != 0 ] || [ "${puts}" = 0 ]; }; then
    passes=$((passes + 1))
  else
    fails=$((fails + 1))
    echo "FAIL floci ${name}: want exit ${want_rc} and \"${want_line}\" last, got exit ${rc} after ${puts} PUT(s)"
    sed 's/^/    /' "${dir}/out"
  fi
}
# floci_rule_ <name> <want exit> <rule children before the actions>
floci_rule_() {
  floci_case_ "$1" "$2" "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Rule><ID>ravel</ID>$3${RULE_ACTIONS}</Rule></LifecycleConfiguration>"
}

# Every spelling the server's rule parser reads as the whole bucket passes.
floci_rule_ filter-empty-prefix 0 '<Filter><Prefix></Prefix></Filter>'
floci_rule_ filter-self-closing-prefix 0 '<Filter><Prefix/></Filter>'
floci_rule_ filter-and-empty-prefix 0 '<Filter><And><Prefix></Prefix></And></Filter>'
floci_rule_ filter-and-self-closing-prefix 0 '<Filter><And><Prefix/></And></Filter>'
floci_rule_ empty-filter 0 '<Filter></Filter>'
floci_rule_ self-closing-filter 0 '<Filter/>'
floci_rule_ legacy-empty-prefix 0 '<Prefix></Prefix>'
floci_rule_ legacy-self-closing-prefix 0 '<Prefix/>'
floci_rule_ space-inside-the-prefix-tag 0 '<Filter><Prefix ></Prefix></Filter>'
floci_rule_ space-inside-the-and-prefix-tag 0 '<Filter><And><Prefix ></Prefix></And></Filter>'
floci_rule_ space-inside-the-legacy-prefix-tag 0 '<Prefix ></Prefix>'
floci_rule_ space-inside-a-self-closing-prefix-tag 0 '<Filter><Prefix /></Filter>'
floci_rule_ newline-inside-the-prefix-tag 0 '<Filter><Prefix
></Prefix></Filter>'
floci_case_ pretty-printed 0 '<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>ravel</ID>
    <Filter>
      <Prefix></Prefix>
    </Filter>
    <Status>Enabled</Status>
    <Expiration>
      <ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
    </Expiration>
    <NoncurrentVersionExpiration>
      <NoncurrentDays>1</NoncurrentDays>
    </NoncurrentVersionExpiration>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>'
floci_case_ legacy-prefix-after-the-actions 0 "<LifecycleConfiguration><Rule><ID>ravel</ID>${RULE_ACTIONS}<Prefix/></Rule></LifecycleConfiguration>"

# Every other filter is refused. The server's parser reads each as
# unrecognized or as narrower than the whole bucket, except text beside a
# Filter's one Prefix, which it ignores; the check refuses that too, since it
# accepts only the exact spellings.
floci_rule_ filter-empty-and 1 '<Filter><And></And></Filter>'
floci_rule_ filter-self-closing-and 1 '<Filter><And/></Filter>'
floci_rule_ filter-unknown-child 1 '<Filter><Unknown/></Filter>'
floci_rule_ filter-unknown-child-beside-prefix 1 '<Filter><Prefix></Prefix><Unknown/></Filter>'
floci_rule_ filter-unknown-child-before-prefix 1 '<Filter><Unknown/><Prefix/></Filter>'
floci_rule_ and-unknown-child 1 '<Filter><And><Prefix/><Unknown/></And></Filter>'
floci_rule_ filter-repeated-prefix 1 '<Filter><Prefix></Prefix><Prefix></Prefix></Filter>'
floci_rule_ and-repeated-prefix 1 '<Filter><And><Prefix/><Prefix/></And></Filter>'
floci_rule_ filter-text 1 '<Filter>x</Filter>'
floci_rule_ filter-text-beside-prefix 1 '<Filter>x<Prefix></Prefix></Filter>'
floci_rule_ legacy-prefix-beside-filter 1 '<Prefix></Prefix><Filter></Filter>'
floci_rule_ legacy-prefix-beside-filter-prefix 1 '<Filter><Prefix/></Filter><Prefix/>'
floci_rule_ two-filters 1 '<Filter/><Filter/>'
floci_rule_ repeated-legacy-prefix 1 '<Prefix/><Prefix/>'
floci_rule_ space-prefix 1 '<Filter><Prefix> </Prefix></Filter>'
floci_rule_ newline-prefix 1 '<Prefix>
</Prefix>'
# A space inside the open tag hides nothing: the server still reads the text
# after it as a prefix of spaces, at every level a Prefix can sit.
floci_rule_ spaced-tag-space-prefix 1 '<Filter><Prefix > </Prefix></Filter>'
floci_rule_ spaced-tag-space-and-prefix 1 '<Filter><And><Prefix > </Prefix></And></Filter>'
floci_rule_ spaced-tag-space-legacy-prefix 1 '<Prefix > </Prefix>'
floci_rule_ spaced-tag-newline-prefix 1 '<Filter><Prefix
>
</Prefix></Filter>'
floci_rule_ and-space-prefix 1 '<Filter><And><Prefix> </Prefix></And></Filter>'
floci_rule_ non-empty-prefix 1 '<Filter><Prefix>t/</Prefix></Filter>'
floci_rule_ tag 1 '<Filter><Tag><Key>k</Key><Value>v</Value></Tag></Filter>'
floci_rule_ and-prefix-and-size 1 '<Filter><And><Prefix/><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></And></Filter>'
floci_rule_ prefix-inside-an-unknown-element 1 '<Unknown><Prefix/></Unknown>'
floci_rule_ prefix-inside-an-unknown-element-with-siblings 1 '<Unknown><ID>x</ID><Prefix/><ID>y</ID></Unknown>'
floci_rule_ no-filter 1 ''
# A Filter, Prefix or And written with a namespace prefix is refused wherever
# it sits: the server reads it as an unrecognized filter and only warns.
NS='xmlns:s3="http://s3.amazonaws.com/doc/2006-03-01/"'
floci_rule_ namespaced-filter 1 "<s3:Filter ${NS}><s3:Prefix></s3:Prefix></s3:Filter>"
floci_rule_ namespaced-filter-beside-legacy-prefix 1 "<Prefix/><s3:Filter ${NS}><s3:Prefix>t/</s3:Prefix></s3:Filter>"
floci_rule_ namespaced-prefix-beside-filter 1 "<Filter/><s3:Prefix ${NS}>t/</s3:Prefix>"
floci_rule_ namespaced-prefix-inside-filter 1 "<Filter ${NS}><s3:Prefix/></Filter>"
floci_rule_ namespaced-and-beside-filter 1 "<Filter><Prefix/></Filter><s3:And ${NS}/>"
floci_rule_ namespaced-and-inside-filter 1 "<Filter ${NS}><s3:And><Prefix/></s3:And></Filter>"
floci_rule_ namespaced-prefix-after-a-newline 1 "<Filter/><s3:Prefix
${NS}>t/</s3:Prefix>"

# floci_calls_ <case> <want> <got>: one comparison of a floci call sequence.
floci_calls_() {
  if [ "$3" = "$2" ]; then passes=$((passes + 1)); else
    fails=$((fails + 1)); echo "FAIL floci $1: want calls \"$2\", got \"$3\""
  fi
}
# The calls a floci run made after the health wait, "|" after each.
floci_log_() { grep -v '_floci/health' "$1" | tr '\n' '|'; }

# The Job reads before it puts. On an empty store it creates the bucket with
# Object Lock and puts both settings, each between its read and its read-back;
# a second run on the same store finds everything set and issues no PUT.
d="${TMP}/floci-fresh-then-rerun"
mkdir -p "${d}/state"
: >"${d}/log"
check_rc=$(floci_run_ "${d}" FLOCI_PUTS_STICK=1)
floci_calls_ fresh-run-exit 0 "${check_rc}"
floci_calls_ fresh-run \
  'HEAD /ravel|PUT /ravel|HEAD /ravel|GET /ravel?object-lock|GET /ravel?versioning|PUT /ravel?versioning|GET /ravel?versioning|GET /ravel?lifecycle|PUT /ravel?lifecycle|GET /ravel?lifecycle|' \
  "$(floci_log_ "${d}/log")"
: >"${d}/log"
check_rc=$(floci_run_ "${d}" FLOCI_PUTS_STICK=1)
floci_calls_ rerun-exit 0 "${check_rc}"
floci_calls_ rerun-issues-no-put \
  'HEAD /ravel|GET /ravel?object-lock|GET /ravel?versioning|GET /ravel?lifecycle|' \
  "$(floci_log_ "${d}/log")"
floci_calls_ rerun-says-so 2 "$(grep -c 'not putting it' "${d}/out")"

# Only the setting that does not already match is put.
d="${TMP}/floci-suspended"
mkdir -p "${d}/state"
: >"${d}/state/bucket"
: >"${d}/state/lock"
printf '%s\n' '<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>' \
  >"${d}/state/versioning"
cp "${TMP}/floci-filter-empty-prefix/state/lifecycle" "${d}/state/lifecycle"
: >"${d}/log"
floci_calls_ suspended-exit 0 "$(floci_run_ "${d}" FLOCI_PUTS_STICK=1)"
floci_calls_ suspended-puts-only-versioning \
  'HEAD /ravel|GET /ravel?object-lock|GET /ravel?versioning|PUT /ravel?versioning|GET /ravel?versioning|GET /ravel?lifecycle|' \
  "$(floci_log_ "${d}/log")"
d="${TMP}/floci-other-rule"
mkdir -p "${d}/state"
: >"${d}/state/bucket"
: >"${d}/state/lock"
printf '%s\n' "${VERSIONED}" >"${d}/state/versioning"
printf '%s\n' "<LifecycleConfiguration><Rule><ID>other</ID><Filter><Prefix/></Filter>${RULE_ACTIONS//<NoncurrentDays>1</<NoncurrentDays>30<}</Rule></LifecycleConfiguration>" \
  >"${d}/state/lifecycle"
: >"${d}/log"
floci_calls_ other-rule-exit 0 "$(floci_run_ "${d}" FLOCI_PUTS_STICK=1)"
floci_calls_ other-rule-puts-only-the-lifecycle \
  'HEAD /ravel|GET /ravel?object-lock|GET /ravel?versioning|GET /ravel?lifecycle|PUT /ravel?lifecycle|GET /ravel?lifecycle|' \
  "$(floci_log_ "${d}/log")"
# A bucket without Object Lock is refused before anything is put on it.
d="${TMP}/floci-no-object-lock"
mkdir -p "${d}/state"
: >"${d}/state/bucket"
: >"${d}/log"
floci_calls_ no-object-lock-exit 1 "$(floci_run_ "${d}" FLOCI_PUTS_STICK=1)"
floci_calls_ no-object-lock-puts-nothing 'HEAD /ravel|GET /ravel?object-lock|' \
  "$(floci_log_ "${d}/log")"

# Usage errors never call aws.
rc=0
PATH="${STUB_BIN}:${PATH}" bash "${SCRIPT}" only-one-arg >/dev/null 2>&1 || rc=$?
if [ "${rc}" = 64 ]; then passes=$((passes + 1)); else
  fails=$((fails + 1)); echo "FAIL usage: want exit 64, got ${rc}"
fi

echo "${passes} passed, ${fails} failed"
[ "${fails}" -eq 0 ]
