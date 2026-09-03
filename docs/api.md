# HTTP API v0.2

LMT is CLI-first, but the CLI is a client of the same HTTP API used by automation.

The pre-stable protocol is versioned as:

```text
/api/v1alpha1
```

The project should not claim a stable `/api/v1` contract before the first stable release.

## 1. General conventions

Control endpoints use HTTP + JSON unless explicitly documented otherwise.

JSON field names use `snake_case`.

Timestamps exposed over the API use RFC 3339 UTC strings even though the database stores Unix milliseconds.

Identifiers are strings:

- Mirror: human-readable name;
- Node: human-readable name;
- Run: ULID;
- Attempt: integer scoped to Run.

## 2. Error shape

Errors use one stable envelope:

```json
{
  "error": {
    "code": "mirror_busy",
    "message": "mirror ubuntu already has a non-terminal run",
    "details": {
      "run_id": "01K..."
    }
  }
}
```

The machine-readable `code` is stable enough for the CLI to handle without parsing English text.

## 3. Health endpoints

```text
GET /health/live
GET /health/ready
```

`live` answers whether the process is alive.

`ready` additionally verifies that database initialization/migrations have completed and the server can serve authoritative requests.

These endpoints do not require operator authentication when bound to the trusted management network.

## 4. Mirror query endpoints

```text
GET /api/v1alpha1/mirrors
GET /api/v1alpha1/mirrors/{name}
```

Useful list filters may include:

```text
?node=mirror01
?enabled=true
?managed=true
```

The Mirror response combines current desired configuration metadata with derived status such as:

- owner node;
- current generation;
- enabled/managed;
- last successful Run;
- current active Run;
- next due time;
- catch-up pending.

The complete canonical TOML can be returned by a dedicated field/endpoint without duplicating every config field into ad-hoc API properties.

## 5. Manual synchronization

```text
POST /api/v1alpha1/mirrors/{name}/runs
```

Request:

```json
{
  "request_id": "01K...",
  "trigger": "manual"
}
```

The CLI generates `request_id` before sending.

Replaying the same `request_id` returns the same Run instead of creating a duplicate operation.

If the Mirror already has a non-terminal Run, the server returns `409 Conflict` with error code `mirror_busy` and the existing Run ID.

If the owner node is offline, a valid manual Run may still be created as Pending.

## 6. Run query endpoints

```text
GET /api/v1alpha1/runs
GET /api/v1alpha1/runs/{run_id}
GET /api/v1alpha1/runs/{run_id}/attempts
```

Useful filters:

```text
?mirror=ubuntu
?node=mirror01
?state=failed
?trigger=manual
?limit=100
?before=<run-ulid-or-time>
```

Cursor/keyset pagination is preferred over large OFFSET pagination for long history tables.

The Run detail response includes its Attempts.

## 7. Run cancellation

```text
POST /api/v1alpha1/runs/{run_id}/cancel
```

Request:

```json
{
  "request_id": "01K..."
}
```

Cancellation is idempotent.

Repeated cancellation of the same non-terminal Run returns the same resulting intent/state.

Cancelling a terminal Run returns its terminal state and does not mutate history.

## 8. Run logs for operators

```text
GET /api/v1alpha1/runs/{run_id}/logs
```

Query parameters:

```text
?attempt=2
?offset=0
?limit=65536
?wait_ms=20000
```

Response may use raw `text/plain` or `application/octet-stream` bytes plus headers:

```text
X-LMT-Log-Offset: 0
X-LMT-Log-Next-Offset: 65536
X-LMT-Log-Complete: false
```

If `wait_ms` is supplied and no new bytes are currently available, the server may hold the request until bytes arrive, the Attempt becomes complete, or the wait expires.

This lets:

```text
lmt run logs --follow <run>
```

work through ordinary bounded HTTP long polling without WebSockets.

## 9. Node query endpoints

```text
GET /api/v1alpha1/nodes
GET /api/v1alpha1/nodes/{name}
```

Responses expose observed state only:

- liveness;
- last seen;
- agent version/instance;
- disk capacity;
- current Runs;
- capabilities.

Local `agent.toml` remains local policy and is not treated as remotely editable server configuration.

## 10. Configuration bundle model

Configuration apply operates on an authoritative bundle, not one file at a time.

The CLI walks a directory such as:

```text
config/
└── nodes/
    ├── mirror01/mirrors/*.toml
    └── mirror02/mirrors/*.toml
```

and creates a bundle containing relative paths and file contents.

The server canonicalizes and hashes the bundle.

This makes prune/remove semantics unambiguous.

## 11. Configuration validation

```text
POST /api/v1alpha1/config/validate
```

The request contains the bundle.

The server returns:

- syntax/schema errors;
- unsafe path errors;
- duplicate names;
- unsupported placeholders;
- invalid scheduling rules;
- node ownership derived from paths;
- canonical bundle hash.

The CLI may also perform the same validation locally for fast feedback, but server validation is authoritative.

## 12. Configuration plan

```text
POST /api/v1alpha1/config/plan
```

Response conceptually:

```json
{
  "base_revision": 42,
  "bundle_hash": "sha256:...",
  "changes": [
    {
      "action": "update",
      "mirror": "ubuntu",
      "from_generation": 12,
      "to_generation": 13
    },
    {
      "action": "remove",
      "mirror": "fedora"
    },
    {
      "action": "move",
      "mirror": "pypi",
      "from_node": "mirror01",
      "to_node": "mirror02"
    }
  ]
}
```

`move` is surfaced separately because it may imply a large re-synchronization on another physical host.

## 13. Configuration apply

```text
POST /api/v1alpha1/config/apply
```

Request includes:

- exact bundle;
- expected `base_revision`;
- optional acknowledgement of high-impact changes.

The server recomputes the plan inside the apply transaction.

If the current configuration revision differs from `base_revision`, return `409 config_revision_conflict` instead of applying against stale assumptions.

The entire apply is atomic.

The response returns the new revision and the actual applied change set.

## 14. Agent authentication boundary

Agent endpoints live under:

```text
/api/v1alpha1/agent/...
```

Each request is authenticated as one Node using its bearer credential.

The authenticated Node identity is authoritative. A request body cannot impersonate another node by changing a `node` string.

## 15. Agent poll

```text
POST /api/v1alpha1/agent/poll
```

The request reports:

- agent version;
- fresh `agent_instance_id`;
- observed capacity;
- currently owned Attempt summaries;
- action acknowledgements if needed.

The server may hold the request for a bounded period when no action is ready.

Response actions initially include:

```text
start_attempt
cancel_attempt
```

Every start action carries:

- Run ID;
- Attempt number;
- spec hash;
- immutable resolved RunSpec.

For an Agent advertising `execution_identity_v1`, a start action also carries
`execution_identity: { "mirror": "..." }`. This envelope identity lets the
Agent enforce a durable Mirror fence even when a later Direct generation uses a
different target. The optional field is omitted for M3 Agents; Direct
`ProcessRunSpec` itself remains exactly M3-shaped.

## 16. Attempt events

```text
POST /api/v1alpha1/agent/attempts/{run_id}/{attempt}/events
```

Request includes:

- event sequence;
- Attempt state/snapshot;
- timestamps;
- exit/failure data when terminal;
- agent instance ID.

Duplicate/out-of-order events are acknowledged idempotently according to the state-machine rules.

The response returns the highest accepted event sequence.

## 17. Agent log upload

Control traffic is JSON, but Run log bytes should not be base64-encoded into JSON.

Use:

```text
PUT /api/v1alpha1/agent/attempts/{run_id}/{attempt}/log
Content-Type: application/octet-stream
X-LMT-Log-Offset: <n>
```

The request body contains the next bytes of the agent's combined Run log stream.

Response:

```text
204 No Content
X-LMT-Log-Next-Offset: <durably stored byte count>
```

If a chunk is retransmitted from an earlier offset, the server validates/handles it idempotently.

The exact on-disk combined stdout/stderr framing format should be specified before implementing log transport, but it must remain readable through `lmt run logs`.

## 18. Agent poll vs events

The long poll is a command channel, not the only state channel.

Attempt events/logs are posted independently so that:

- completion is not delayed by a currently open poll;
- large logs do not block command delivery;
- retry behavior remains simple.

## 19. Authentication for CLI

v0.1 can use a simple operator bearer token because the assumed deployment is a trusted management network.

The API design must not bake authorization policy into endpoint semantics, so OIDC/RBAC can be added later without breaking resources.

Read-only status endpoints may later have a separately configurable public exposure.

## 20. Metrics

Prometheus metrics are exposed separately:

```text
GET /metrics
```

The metrics endpoint is not part of the versioned JSON API.

## 21. Compatibility rules

Before stable v1:

- `v1alpha1` may change between minor pre-stable releases;
- CLI and server should normally be kept within a documented compatibility window;
- the server should reject unsupported Agent protocol versions clearly rather than accepting ambiguous behavior.

Before the first stable release, the project should define a concrete API compatibility policy.


## 22. M2 Run and Mirror fields

Run trigger becomes a typed manual/scheduled value.

Run responses additionally expose nullable scheduled_for_at, retry_due_at, and cancel_requested_at.

Mirror responses expose derived next_due_at and scheduled_due_since where applicable.

Node responses expose observed max_concurrent_runs.

## 23. M2 cancellation endpoint

~~~text
POST /api/v1alpha1/runs/{run_id}/cancel
~~~

No request ID is required because the target Run itself provides idempotent identity.

Repeated requests preserve one cancel_requested_at value.

- terminal Run: unchanged terminal response;
- safe undispatched Run or retry wait: may become Cancelled immediately;
- dispatched active work: returns non-terminal Run with cancel intent while Server delivers CancelAttempt.

## 24. M2 Agent protocol changes

Poll capacity adds max_concurrent_runs.

CancelAttempt adds spec_hash.

One action is returned per poll according to the priority defined in m2-design.md.

## 25. M2 query behavior

Run list should support trigger filtering.

Run detail shows all Attempts plus retry/cancel metadata.

Mirror detail includes scheduler-derived timing status; TOML remains the source of schedule expression itself.


## 26. M3 bounded Run list

GET /api/v1alpha1/runs supports:

~~~text
mirror
node
state
trigger
limit
before
~~~

Default limit is 50 and maximum is 500.

before is a Run-ID keyset cursor.

The API must not return complete Run history by default.

## 27. M3 log follow

Run log reads retain logical offsets and add bounded waiting:

~~~text
GET /api/v1alpha1/runs/{id}/logs
  ?attempt=N
  &offset=K
  &limit=65536
  &wait=20s
~~~

If attempt is omitted, use the latest Attempt.

Expired logs return HTTP 410 with code log_expired.

Unexpected missing files use a distinct operational error.

## 28. M3 Agent credentials

Operator-authenticated endpoints conceptually include:

~~~text
POST /api/v1alpha1/nodes/{node}/credentials
GET  /api/v1alpha1/nodes/{node}/credentials
POST /api/v1alpha1/nodes/{node}/credentials/{id}/revoke
~~~

Issue returns the raw Server-generated token once and must set Cache-Control: no-store.

List/revoke never expose the raw secret.

## 29. M3 Node binding

Node views expose sanitized binding/install diagnostics.

A conflicting Agent poll returns:

~~~text
409 agent_binding_conflict
~~~

with bound and presented installation IDs.

Binding replacement is an explicit operator endpoint and enforces the safety preconditions defined in m3-design.md.

## 30. M3 backups

Online operator endpoints conceptually support:

~~~text
POST /api/v1alpha1/backups
GET  /api/v1alpha1/backups
POST /api/v1alpha1/backups/{id}/verify
~~~

No remote API accepts an arbitrary Server filesystem path.

There is no HTTP restore endpoint.

## 31. M3 status and doctor

A sanitized read-only status projection supports lmt status and a later web status page.

Public unauthenticated access exists only when explicitly enabled by Server config.

Doctor/diagnostic responses use stable check IDs and do not mutate state.

## 32. M3 error additions

Representative codes:

~~~text
agent_binding_conflict
credential_not_found
credential_revoked
log_expired
log_missing
backup_not_configured
backup_busy
backup_invalid
state_lock_busy
~~~

API error codes are mapped to documented CLI exit categories.

## 33. Fresh-install Node enrollment

For M3 production-trial maintenance, the first operator-authenticated credential issue is also the Node bootstrap boundary:

~~~text
POST /api/v1alpha1/nodes/{node}/credentials
~~~

If `{node}` is a valid Node name but no Node record exists yet, the Server atomically creates an offline/unbound Node record and the requested credential.

This endpoint does not create Mirror ownership or mark the Node online.

The first successfully authenticated Agent poll later establishes observed state and the durable Agent installation binding.

Unauthenticated Agent self-registration remains forbidden.

## 34. M4 publication status and diagnostics

Node entries in the existing status response may include
`atomic_publication_capable` and `publication_health`. Both are omitted when no
M4 publication-health observation exists, preserving the accepted M3 response
shape for legacy Agents.

The existing metrics endpoint exposes low-cardinality publication aggregates:

- capable Agent count;
- publication commit success/failure totals;
- visibility-to-durability duration total/sample count;
- preflight rejection and GC failure totals;
- per-Node publication free bytes, GC backlog, degraded/fence/recovery state,
  and admission-block reason.

Run IDs, Attempt numbers, spec hashes, and filesystem paths are never metric
labels. The Server doctor adds stable `publication.capability` and
`publication.health` checks based on the latest authenticated Agent polls.

Filesystem and protected-spool checks that require local Agent access are
available through the offline `lmt-agent doctor` command. It acquires the normal
Agent single-instance lock and performs the real disposable filesystem probe;
it does not mutate control-plane or publication business state.
