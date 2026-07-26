# PaveBank Fees API — REST Contracts (v1)

The API is a thin synchronous shell over Temporal. Writes map to Temporal client
calls (`StartWorkflow`, `UpdateWorkflow`, `SignalWorkflow`); reads bypass Temporal
and hit the Postgres ledger directly, so GET/LIST keep working after workflow
histories age out of retention.

The central change from the earlier draft: **add-line-item is a Workflow Update,
not a Signal.** This removes the old "202-async vs 201-sync" dilemma entirely — an
Update is synchronous by construction and carries a result or a typed rejection
back to the caller, so every outcome maps to a definite status code in one round
trip. See §FR2.

---

## Conventions

**Base:** `/v1`

**Money:** `{ "amount": "<integer-minor-units-as-string>", "currency": "<ISO-4217>" }`.
Amount is a *string* so a large stablecoin value can't be mangled by JSON's float
coercion; the client shifts the decimal based on `currency` (D5).

**JSON naming:** public API request/response fields use camelCase. SQL tables and
columns may remain snake_case (`amount_minor`, `closed_at`) because they are
internal ledger storage, not the wire contract.

**Idempotency:** carried by domain keys, not a transport header. `open` is
idempotent on the bill key; `add-line-item` on `(bill_id, reference)`; `close` on
bill state. No `Idempotency-Key` header is required.

**Errors:** RFC 9457 problem+json (`type`, `title`, `status`, `detail`, `instance`).

**How Update outcomes become status codes.** A Workflow Update has two phases the
API layer observes:

| Update phase | What happens | HTTP mapping |
|---|---|---|
| **Validator rejects** | Synchronous reject, *nothing written to workflow history* | `4xx` derived from the error `type` (e.g. `CurrencyMismatch` → 400, `BillNotOpen` → 409) |
| **Handler returns a value** | Activity ran to completion; returns `{Applied bool}` | `201` if `Applied`, `200` if idempotent no-op |
| **Handler returns an error** | e.g. close-race caught at the DB (`BillNotOpen`, non-retryable) | `409` |

The validator reject is the important win: a bad-currency item never touches the
event history, so a misconfigured caller can hammer the endpoint without bloating
the workflow.

---

## FR1 — Open a bill

```
POST /v1/bills
{ "clientId": "acme", "currency": "USD", "period": "2026-07" }
```

The API validates the period grammar (`^\d{4}-(0[1-9]|1[0-2])$`) **before**
`StartWorkflow` — this is the precondition `resolvePeriodEnd`'s panic relies on —
then checks `now < resolvePeriodEnd(period)`, then starts the workflow with ID
`bill-{clientId}-{currency}-{period}`.

| Outcome | Status | Notes |
|---|---|---|
| Created | `201` + `Location` whose path is `/v1/bills/{billId}` | bill resource; `Location` may be relative or absolute |
| Duplicate open (workflow ID exists) | `409` `type: bill-already-open` | keys off Temporal's `WorkflowExecutionAlreadyStarted`; the workflow ID's point-uniqueness *is* the lock (D1) |
| Period already elapsed | `422` `type: period-elapsed` | |
| Malformed period / currency | `400` | validation problem |

**Bill resource** (`status` is the ledger's two-state view; internal
`DRAINING`/`CLOSING` are never exposed):

```json
{
  "billId": "bill-acme-USD-2026-07",
  "clientId": "acme",
  "currency": "USD",
  "period": "2026-07",
  "status": "OPEN",
  "totalMinorAmount" : "0",
  "currency" : "USD",
  "itemCount": 0,
  "openedAt": "2026-07-03T14:21:00Z",
  "closedAt": null
}
```

`total` and `itemCount` are **computed on read** from `line_items`
(`COALESCE(SUM(amount_minor),0)`, `COUNT(*)`), not stored columns — so they can't
drift from the append-only rows.

---

## FR2 — Add a line item  *(Workflow Update)*

```
POST /v1/bills/{billId}/line-items
{
  "reference": "pay-svc-evt-98213",
  "minorAmount" : "1500",
  "currency" : "USD",
  "feeType": "wire_transfer",
  "description": "Outbound USD wire"
}
```

Handler calls `UpdateWorkflow` on the bill's workflow ID with the `addLineItem`
update. **Plain Update, not Update-With-Start** — a fee event with no open bill
must fail loudly (FR1 edge case: never auto-open a billing period), so a missing
workflow is a `404`, never a lazy create. (Update-With-Start is also explicitly
*non-atomic* — a failed update can still leave a started workflow — which is a
second reason to avoid it here.)

Outcomes, each tied to the Update phase that produces it:

| Outcome | Produced by | Status |
|---|---|---|
| New item applied | handler returns `{Applied: true}` | `201 Created` |
| Duplicate `reference` (idempotent replay) | handler returns `{Applied: false}` | `200 OK` |
| Currency mismatch | **validator** rejects (`CurrencyMismatch`) | `400` — nothing written to history |
| Bill already closed (visible in-memory) | **validator** rejects (`BillNotOpen`) | `409` |
| Bill closed *during* the call (race) | handler's activity → DB `WHERE EXISTS(status='OPEN')` fails → non-retryable `BillNotOpen` | `409` |
| No workflow for `billId` | `UpdateWorkflow` returns not-found | `404` `type: no-open-bill` |

**Why this is strictly better than the Signal design it replaces.** The old plan
had to choose between `202` (honest about a Signal's fire-and-forget nature but
useless to a caller who needs the reject) and a `201` faked by signal-then-poll-a-
Query. Update makes the wait-for-completion intrinsic: the caller gets the real
`201`/`200`/`400`/`409` synchronously, no polling, and the two rejects that used to
be invisible log lines are now typed errors the client can branch on.

**Fresh vs. duplicate without a second query.** The persist Activity returns a bool
(`true` = row inserted, `false` = `ON CONFLICT DO NOTHING` no-op), which the handler
passes back as `Applied`. So `201` vs `200` is decided from the Update result alone —
no follow-up read to tell "created" from "already there."

**Client call stage.** Use `UpdateWorkflow` and wait for the **Completed** stage
(not just Accepted) — the API is synchronous end-to-end and wants the handler's
return value, so it blocks for completion. This does require a live worker; that's
acceptable for a write path and is the normal Update contract.

---

## FR3 — Close a bill

```
POST /v1/bills/{billId}/close
{ "reason": "explicit-early-close" }
```

Handler sends `SignalCloseBill`, then `.Get(ctx, nil)` to confirm the workflow
completed the seal, then **reads the sealed bill back from the ledger** (including
computed total and items) and returns it.

Close stays a **Signal**, deliberately, even though FR3 wants the sealed total
returned. Two reasons: (1) the close *decision* is fire-and-forget — the caller
isn't blocking on a value the signal produces, it reads the result from the ledger
afterward; (2) the workflow drains in-flight line-item Update handlers
(`AllHandlersFinished`) before sealing, so "close" is a lifecycle trigger, not a
request/response. If you wanted the seal to *return* the invoice synchronously,
close becomes a good Update candidate — noted as a future option, not today's design.

Returns total **and** items (FR3 requires both), read from the ledger:

```json
{
  "billId": "bill-acme-USD-2026-07",
  "status": "CLOSED",
  "totalMinorAmount" : "43700",
  "currency" : "USD",
  "itemCount": 12,
  "closedAt": "2026-07-14T09:02:11Z",
  "lineItems": [
    {
      "reference": "pay-svc-evt-98213",
      "minorAmount" : "1500",
      "currency": "USD",
      "feeType": "wire_transfer",
      "description": "Outbound USD wire",
      "appliedAt": "2026-07-03T14:22:03Z"
    }
  ]
}
```

| Outcome | Status |
|---|---|
| Closed now | `200` |
| Already closed (FR10 idempotent) | `200` — same sealed invoice facts, read from ledger |
| No such bill | `404` |

Fresh-close and re-close return the same sealed invoice facts — identity, lifecycle
state, computed total, item count, close timestamp, and itemized lines. JSON object
field order and line-item array order are not part of the idempotency guarantee.
The caller can't tell (and shouldn't care) who won the seal race. `200` not `201`:
close mutates an existing resource's state, it doesn't create one. A closed
`bills` row *is* the invoice (there is no separate invoice resource to create).

---

## FR6 — Get bill

```
GET /v1/bills/{billId}
GET /v1/bills/{billId}?includeLineItems=true
GET /v1/bills/{billId}?includeLineItems=true&cursor=...&limit=50
```

Pure ledger read — works while OPEN and after the workflow ages out of retention.
`total`/`itemCount` computed via `SUM`/`COUNT` across all rows. `includeLineItems=true`
appends one cursor-paginated page of itemized rows (default 50, cap 200), plus
`nextCursor` and `hasMore`. The default omits `lineItems` and pagination metadata
to keep the hot read cheap. `404` if absent.

```json
{
  "billId": "bill-acme-USD-2026-07",
  "clientId": "acme",
  "currency": "USD",
  "period": "2026-07",
  "status": "OPEN",
  "totalMinorAmount": "43700",
  "itemCount": 12,
  "openedAt": "2026-07-03T14:21:00Z",
  "closedAt": null,
  "lineItems": [
    {
      "reference": "pay-svc-evt-98213",
      "minorAmount": "1500",
      "currency": "USD",
      "feeType": "wire_transfer",
      "description": "Outbound USD wire",
      "appliedAt": "2026-07-03T14:22:03Z"
    }
  ],
  "nextCursor": "b3BhcXVlLWtleQ",
  "hasMore": true
}
```

The line-item cursor is opaque and bill-scoped. Closed bills provide a stable
page traversal because invoice line items are immutable. Open bills provide
deterministic keyset pagination over rows visible to each request; concurrent
line-item commits can change later pages until the bill is closed.

`cursor` and `limit` are valid only with `includeLineItems=true`; otherwise the
API returns `400 invalid-request`.

**Transient status note.** There's a brief window at close where the workflow has
stopped accepting items (in-memory status flipped) but the seal `UPDATE` hasn't
committed, so a GET can still read `OPEN`. This is correct: only the workflow's
decision gates writes, and the readable status catches up the instant the seal
commits. Callers must not treat GET's `OPEN` as a guarantee they can still add
items — the add path is authoritative for that, and will reject.

---

## FR7 — List bills

```
GET /v1/bills?clientId=acme&status=OPEN&currency=USD&period=2026-07
```

Filters optional and combinable, served off `idx_bills_client_status`,
`idx_bills_period`, `idx_bills_currency`. Cursor-paginated (`?cursor=...&limit=50`,
default 50, cap 200):

```json
{
  "bills": [ /* bill resources; total/itemCount computed per row */ ],
  "nextCursor": "b3BhcXVlLWtleQ==",
  "hasMore": true
}
```

Line items never inline in a list (N+1 fan-out) — drill into
`GET /{billId}?includeLineItems=true`. If computing `SUM`/`COUNT` per row in LIST
becomes a latency problem at scale, that's the trigger to denormalize `total` onto
`bills` (see LEDGER.md §2.1 tradeoff), not before.

---

## Status-code summary (NFR7)

| Verb + path | Success | Notable failures |
|---|---|---|
| `POST /bills` | `201` | `409` dup, `422` elapsed, `400` malformed |
| `POST /bills/{id}/line-items` | `201` new / `200` duplicate | `400` currency (validator), `409` closed (validator or DB race), `404` no bill |
| `POST /bills/{id}/close` | `200` | `404` |
| `GET /bills/{id}` | `200` | `404` |
| `GET /bills` | `200` | `400` bad filter |

---

## Points worth raising in the interview

1. **Update collapses the 202/201 question.** The earlier design agonized over
   async-vs-sync for add-line-item; Update makes it moot — synchronous result,
   typed rejects, and no event-history bloat for rejected input. This is the
   Temporal-recommended shape for a write that can be rejected and whose caller
   needs to know.

2. **Two guards for two invariants.** Idempotency lives in the DB (`(bill_id,
   reference)` unique + `ON CONFLICT`); lifecycle (open/closed) is enforced in the
   Update validator with the DB `WHERE EXISTS(status='OPEN')` as the race-proof
   backstop. Each invariant is enforced at the layer that owns it, not bolted onto
   one place.

3. **The close race is handled, not hand-waved.** A bill can seal between the
   validator's in-memory check and the handler's activity. The validator catches
   the easy already-closed case (clean 409, no history entry); the DB catches the
   racy one (non-retryable 409). The in-memory check is deliberately *not* made
   race-proof — it can't be, and doesn't need to be.

4. **Handler draining is mandatory, not optional.** Because add-line-item handlers
   await an Activity, the workflow must `Await(AllHandlersFinished)` before sealing
   or continuing-as-new, or an in-flight caller gets a NotFound on their Update.
   This is the one piece of Update wiring that's easy to miss and that Temporal's
   docs call out explicitly.

5. **Amount-as-string and no idempotency header** — same rationale as before:
   defensive against float coercion for high-exponent stablecoins, and the domain
   keys already provide semantic idempotency so a transport header would be
   redundant.
