#!/bin/sh
# assert-gh-auth.sh [hostname]
# Make sure that gh can complete an authenticated API call. Exit 0 on
# success; exit 1 otherwise. Run it before a landing sequence: a dead
# keychain token found at push time strands committed work. `gh auth
# status` alone is not enough; it can report a token the API rejects.
set -u

host=${1:-github.com}

gh auth status --hostname "$host" >/dev/null 2>&1 || {
  echo "guard: gh has no stored credentials for $host." >&2
  echo "guard: run 'gh auth login --hostname $host' and retry." >&2
  exit 1
}

login=''
login=$(gh api user -q .login 2>/dev/null) || {
  echo "guard: gh has credentials for $host but the API rejects them." >&2
  echo "guard: run 'gh auth refresh --hostname $host' and retry." >&2
  exit 1
}

echo "guard: gh authenticated to $host as $login"
exit 0
