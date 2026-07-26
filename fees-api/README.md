# PaveBank Fees API

Encore Go implementation of the PaveBank Fees API take-home: a billing-period
fees service backed by Temporal orchestration and a Postgres ledger.

## Architecture Map

The intended production path is deliberately thin at the HTTP layer:

```text
downstream services
  -> Encore REST API
  -> Temporal client
  -> one BillWorkflow per (clientId, currency, period)
  -> idempotent Activities
  -> Postgres ledger
  -> GET/LIST read directly from the ledger
```

Temporal owns live lifecycle orchestration. Postgres owns the permanent audit
record and all queryable invoice facts.

## Local Smoke

Start Temporal:

```bash
temporal server start-dev
```

In another terminal, start Encore from this directory:

```bash
encore run
```

Then call the list endpoint:

```bash
curl http://localhost:4000/v1/bills
```

Expected response:

```json
{"bills":[],"nextCursor":"","hasMore":false}
```

The worker connects to Temporal at `127.0.0.1:7233`, namespace `default`, and
polls task queue `fees`. Encore provisions the `feesdb` Postgres database and
applies the ledger migrations.

## E2E Tests

With Temporal and Encore already running:

```bash
PAVEBANK_E2E=1 go test -v ./e2e -run TestFeesLifecycleE2E -count=1
```

The E2E suite first performs a preflight `GET /v1/bills`. If the app is not
reachable, it fails with startup instructions instead of misleading lifecycle
assertion errors.

## Tests

Run the default suite with verbose output so each test name and result is printed:

```bash
encore test -v ./...
```

To capture a simple count of tests run, passed, failed, and skipped:

```bash
encore test -v ./... 2>&1 | tee /tmp/pavebank-test.log
awk '/^--- PASS:/{pass++} /^--- FAIL:/{fail++} /^--- SKIP:/{skip++} END{printf "run=%d pass=%d fail=%d skip=%d\n", pass+fail+skip, pass, fail, skip}' /tmp/pavebank-test.log
```
