# Fees API

Encore Go implementation of a billing-period Fees API backed by Temporal
orchestration and a PostgreSQL ledger.

## Architecture

```text
downstream services
  -> Encore REST API
  -> Temporal Cloud or the local Temporal dev server
  -> one BillWorkflow per (clientId, currency, period)
  -> feeworker task queue and idempotent Activities
  -> Encore Pub/Sub
  -> PostgreSQL ledger
  -> GET/LIST read directly from the ledger
```

Temporal owns live lifecycle orchestration. PostgreSQL owns the permanent audit
record and queryable invoice facts. Encore provisions the `feesdb` database,
applies its migrations, and provisions the `update-line-items` Pub/Sub topic and
subscription.

## Local development

Prerequisites:

- Go 1.26 or newer
- Encore CLI
- Temporal CLI
- Docker, for Encore's local PostgreSQL infrastructure

Start the local Temporal server:

```bash
temporal server start-dev
```

From this directory, start Encore in another terminal:

```bash
encore run
```

The local app always uses Temporal at `127.0.0.1:7233`, namespace `default`,
without API-key authentication. The worker polls task queue `feeworker`.

Smoke-test the API:

```bash
curl http://localhost:4000/v1/bills
```

Expected response:

```json
{"bills":[],"nextCursor":"","hasMore":false}
```

## Automated tests

Run the Encore test suite:

```bash
encore test -v ./...
```
To capture a simple count of tests run, passed, failed, and skipped:

```bash
encore test -v ./... 2>&1 | tee /tmp/pavebank-test.log
awk '/^--- PASS:/{pass++} /^--- FAIL:/{fail++} /^--- SKIP:/{skip++} END{printf "run=%d pass=%d fail=%d skip=%d\n", pass+fail+skip, pass, fail, skip}' /tmp/pavebank-test.log
```

Check Temporal workflow determinism:

```bash
go install go.temporal.io/sdk/contrib/tools/workflowcheck@latest
workflowcheck ./...
```

With Temporal and Encore already running, execute the full lifecycle test:

Local
```bash
PAVEBANK_E2E=1 go test -v ./e2e -run TestFeesLifecycleE2E -count=1
```

## Temporal Cloud bootstrap

1. In Temporal Cloud, create a single-region namespace named
   `fees-api-v3-dev`. Use AWS Singapore when available, one-day retention, and
   API-key authentication.
2. Copy the exact namespace identifier and Namespace Endpoint displayed by
   Temporal Cloud. This deployment uses namespace `fees-api-v3-dev.ebtwx` and
   endpoint `fees-api-v3-dev.ebtwx.tmprl.cloud:7233`.
3. Create a short-lived API key that can access the namespace.
4. Keep each service's `temporal.cue` file synchronized if the namespace is
   recreated and Temporal Cloud assigns new values.
5. Configure and verify a local Temporal CLI profile:

```bash
temporal config set --profile fees-api-v3-cloud --prop address --value 'fees-api-v3-dev.ebtwx.tmprl.cloud:7233'
temporal config set --profile fees-api-v3-cloud --prop namespace --value 'fees-api-v3-dev.ebtwx'
temporal config set --profile fees-api-v3-cloud --prop api_key --value '<api-key>'
temporal workflow list --profile fees-api-v3-cloud --limit 1 --output json
```

The Go SDK receives `client.NewAPIKeyStaticCredentials` only in a non-local
Encore environment. API-key credentials enable TLS automatically.

## Encore Cloud bootstrap and deployment

1. Create a fresh Encore app named `fees-api-v3-temporal`.
2. Link this local app to the generated app id:

```bash
encore app link '<new-app-id>' --force
```

3. In Encore Cloud, link GitHub repository
   `conghan-tay/Pave_FeesApi`, set **Root Directory** to `fees-api-V3`, and map
   branch `deploy-01` to the primary development environment.
4. Store the Temporal key for Encore's development environment:

```bash
encore secret set --type dev TemporalAPIKey
```

5. Encore also validates declared secrets locally. Set a dummy local value;
   local mode ignores it and continues to use the unauthenticated dev server:

```bash
encore secret set --type local TemporalAPIKey
# Enter: unused-local
```

6. Commit and push branch `deploy-01`. The configured GitHub branch deployment
   builds the app and provisions its database and Pub/Sub infrastructure.

Follow deployment logs in Encore Cloud or with:

```bash
encore logs
```

## Cloud verification

Copy the deployed Encore base URL, then run:

```bash
PAVEBANK_E2E=1 \
PAVEBANK_API_BASE_URL='https://<encore-environment-host>' \
go test -v ./e2e -run TestFeesLifecycleE2E -count=1
```

Successful verification shows:

- a completed `BillWorkflow` in Temporal Cloud;
- an active poller on task queue `feeworker`;
- a closed bill with three finalized line items and total `3750`;
- Encore traces spanning REST calls, service calls, Pub/Sub, and PostgreSQL.

## Troubleshooting

- `connection refused 127.0.0.1:7233` in Encore Cloud means a service still
  received local configuration; check the service's `temporal.cue` file.
- `Unauthenticated` from Temporal means the Encore development secret is absent,
  expired, revoked, or not authorized for the namespace.
- `Namespace not found` usually means the namespace value differs from the exact
  identifier shown in Temporal Cloud.
- An accepted API call that never updates the ledger usually means no worker is
  polling `feeworker`; inspect the Temporal Task Queue page and Encore worker
  logs.
- If Encore cannot find `encore.app`, verify the GitHub Root Directory is
  exactly `fees-api-V3`.

## Teardown

After testing:

1. Revoke the new Temporal API key.
2. Delete the `fees-api-v3-dev` namespace in Temporal Cloud.
3. Remove the local profile:

```bash
temporal config delete-profile --profile fees-api-v3-cloud
```

4. Delete the new Encore app/environment and its provisioned infrastructure in
   Encore Cloud.

