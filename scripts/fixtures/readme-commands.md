# Fixture: marked runnable command blocks

This file exercises scripts/check_readme_commands.py. It is not the README; it
is a controlled sample covering each case the extractor and evaluator must
handle. Ticket #175 owns the real README.

## A marked block

<!-- ravel:run json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" http://127.0.0.1:4318/api/v1/query?query=up
```

## An unmarked block that must never run

This fence has no `ravel:run` marker, so the extractor must ignore it. If it
ever runs, the extractor is broken.

```sh
echo THIS_MUST_NEVER_RUN
```

## A marked block whose command spans multiple lines

<!-- ravel:run status=200 -->
```sh
curl -s \
  -H "Authorization: Bearer demo-token" \
  "http://127.0.0.1:4318/api/v1/query?query=up"
```

## A marked block that exits 0 but returns an error envelope

The command below exits 0 (curl succeeds), yet the server answers with a
`{"status":"error"}` body. The block asserts `json:.status=success`, so the
check must FAIL here even though the process exit status is 0. This is the
exact defect ADR-0081 exists to catch.

<!-- ravel:run json:.status=success -->
```sh
curl -s -H "Authorization: Bearer demo-token" "http://127.0.0.1:4318/api/v1/query?query=broken"
```
