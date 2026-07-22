# PaveBank Fees API

Encore Go scaffold for the PaveBank Fees API take-home.

## Local Smoke

Start Temporal:

```bash
temporal server start-dev
```

In another terminal, start Encore from this directory:

```bash
encore run
```

Then call the scaffold endpoint:

```bash
curl http://localhost:4000/v1/bills
```

Expected response:

```json
{"bills":[],"nextCursor":"","hasMore":false}
```

The scaffold worker connects to Temporal at `127.0.0.1:7233`, namespace `default`,
and polls task queue `fees`. Encore provisions the `feesdb` Postgres database; the
ledger schema is intentionally deferred to Build Plan #3.

## Configuration Notes

Build Plan #1 intentionally uses local Temporal defaults and does not declare
Encore secrets, because declared Encore secrets must be set before the app can run.
When this app targets non-local environments, introduce secret-backed config for:

- `TemporalTarget`
- `TemporalNamespace`
- Temporal Cloud TLS credentials, if needed

## Tests

```bash
encore test ./...
```
