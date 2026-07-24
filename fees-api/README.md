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

## Build Progress

- Step #1 complete: Encore service scaffold, Temporal client/worker startup, and
  placeholder `GET /v1/bills`.
- Step #2 complete: opt-in E2E client/dashboard and README skeleton.
- Steps #3-#7 complete: ledger schema, domain helpers, activities, workflow, and
  `POST /v1/bills`.
- Step #8 complete: `POST /v1/bills/{billId}/line-items` calls the
  `addLineItem` Workflow Update, returning `201` for fresh items, `200` for
  duplicate references, `400` for currency mismatch, `409` for closed bills, and
  `404` when no bill exists.
- Step #9 complete: `POST /v1/bills/{billId}/close` signals the bill workflow,
  waits for the seal, then returns the ledger invoice with computed total and
  itemized line items. Re-closing a sealed bill returns the existing invoice
  directly from the ledger, so it also works after the workflow is no longer
  signalable.
- Step #10 complete: `GET /v1/bills/{billId}` and `GET /v1/bills` read directly
  from Postgres, compute totals/counts from `line_items`, support optional
  itemized detail on GET, and provide filtered cursor pagination for LIST.
- Steps #11-#12 upcoming: auto-close edge cases and final docs.

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

## E2E Dashboard

Build Plan #2 adds a lightweight black-box client in `e2e/`. It is opt-in so the
normal test suite stays useful while later build steps are still incomplete.

With Temporal and Encore already running:

```bash
PAVEBANK_E2E=1 PAVEBANK_API_BASE_URL=http://localhost:4000 go test -v ./e2e
```

The E2E suite first performs a preflight `GET /v1/bills`. If the app is not
reachable, it fails with startup instructions instead of misleading lifecycle
assertion errors.

Immediately after Step #2, the preflight should pass and the lifecycle assertions
should fail cleanly because `POST /v1/bills`, add-line-item, close, and bill detail
endpoints are not implemented yet. As later build steps land, the same dashboard
turns green incrementally:

- open bill returns `201` with `Location`
- add distinct items returns `201`
- duplicate reference returns `200` without changing the total
- currency mismatch returns `400`
- close returns `200` with total and itemized line items
- add after close returns `409`
- re-close returns the same sealed invoice facts
- GET and LIST expose the computed ledger total

After Step #10, GET and LIST are implemented and covered by unit/integration
tests. The monolithic E2E test's read assertions are expected to pass once the
local Temporal and Encore stack is running.

## Configuration Notes

Build Plan #1 intentionally uses local Temporal defaults and does not declare
Encore secrets, because declared Encore secrets must be set before the app can run.
When this app targets non-local environments, introduce secret-backed config for:

- `TemporalTarget`
- `TemporalNamespace`
- Temporal Cloud TLS credentials, if needed

## Tests

Run the default suite with verbose output so each test name and result is printed:

```bash
encore test -v ./...
```

The Step #2 E2E package is skipped unless `PAVEBANK_E2E=1` is set.

Run the optional live Temporal smoke test with Temporal already running:

```bash
PAVEBANK_LIVE_TEMPORAL=1 encore test -v ./...
```

To capture a simple count of tests run, passed, failed, and skipped:

```bash
encore test -v ./... 2>&1 | tee /tmp/pavebank-test.log
awk '/^--- PASS:/{pass++} /^--- FAIL:/{fail++} /^--- SKIP:/{skip++} END{printf "run=%d pass=%d fail=%d skip=%d\n", pass+fail+skip, pass, fail, skip}' /tmp/pavebank-test.log
```

Run e2e tests with Encore and Temporal already running:

```bash
PAVEBANK_E2E=1 go test -v ./e2e -run TestFeesLifecycleE2E -count=1
```
