# Abacus RFID Platform

GreyOrange Engineering Take-Home Exercise — engineering track only.

Abacus is a testable multi-tenant retail backend for onboarding stores and RFID
hardware, importing a product master, turning reader observations into floor and
backroom inventory, controlling store-scoped access, and creating explainable
replenishment work.

The repository intentionally uses one FastAPI deployment plus an independently
scalable worker. PostgreSQL is both the system of record and the durable work inbox
for the submitted slice. Kafka is the recommended evolution when measured event
volume, replay, or multiple consumers justify operating it.

> Hosted status: the Render blueprint is ready, but no hosted URL is claimed in this
> repository yet. Add the tested immutable URL and reviewer credentials only after
> deployment verification.

## Published API contract

- Product and OpenAPI title: `Abacus RFID Platform`
- Python distribution: `abacus-rfid-platform`
- Business API namespace: `/v1`; operational endpoints remain unversioned
- Application/OpenAPI info version: `0.1.0`; OpenAPI document format: `3.1.0`
- Authoritative contract: `GET /openapi.json`; interactive view: `GET /docs`

The namespace major version and application release version serve different purposes:
`/v1` preserves HTTP contract compatibility, while `0.1.0` identifies this submitted
software release. `GET /version` returns that release plus its deployed `build_sha`.

## Assignment coverage

The implementation follows the exercise in its original order:

1. **Brand/store onboarding** — active tenant creation, an idempotent 1–500 store batch,
   organization hierarchy, zones, hardware discovery, device-key rotation with plaintext
   returned once,
   and effective-dated reader assignments.
2. **Product master** — staged CSV import, checksum/idempotency, normalization,
   row-level error evidence, reconciliation preview, and atomic promotion of styles,
   SKUs, UPCs, attributes, and effective-dated EPC mappings.
3. **RFID observations** — authenticated batches, retained raw event evidence, durable
   worker jobs, duplicate/conflict handling, late-event non-regression, future-clock
   quarantine, bounded movement confirmation, inventory projections, and replay.
4. **Identity and access** — Argon2id passwords, tenant-code login, short-lived JWTs,
   corporate/manager/associate roles, store scopes, token invalidation, and audit.
5. **Replenishment** — atomic policy import and CRUD, effective dates, deterministic
   precedence, explainable evaluations, one active task per store/SKU, ownership,
   optimistic concurrency, and task lifecycle.

Sections 4 and 5 are the explicitly required build focus. Sections 1–3 are also
implemented as a coherent vertical slice so reviewers can exercise them end to end.
See [assignment coverage](docs/assignment-coverage.md), [architecture](docs/architecture.md),
and the [decision/assumption register](docs/decisions.md).

## Architecture

```mermaid
flowchart LR
    Admin[Platform integration] -->|X-Platform-Key| API[FastAPI API]
    User[Corporate / manager / associate] -->|Bearer JWT| API
    Reader[RFID reader or edge gateway] -->|batched HTTPS + device key| API
    API -->|transactional domain writes + durable jobs| PG[(PostgreSQL 17)]
    Worker[Background worker] -->|lease + SKIP LOCKED| PG
    Worker -->|catalog promotion / RFID projection / replenishment| PG
    API --> Docs[OpenAPI /docs]
```

The API and worker share domain modules but run as separate processes. Accepted work
survives restarts because the job row and source record commit together. Workers use
leases, heartbeats, `FOR UPDATE SKIP LOCKED`, retries with backoff, quarantine after
the retry limit, and compare-and-set completion. Processing is **at least once**;
idempotent state transitions provide safety. There is no exactly-once claim.

## Technology choices

| Concern | Submitted choice | Why |
|---|---|---|
| Language | Python 3.13 + FastAPI | Typed contracts, concise implementation, OpenAPI, and fast reviewability |
| Persistence | PostgreSQL 17 | Transactions, relational integrity, JSONB for optional attributes, advisory locks, exclusion constraints |
| Async work | PostgreSQL durable inbox | No external broker/credential failure mode in the reviewer-sized deployment |
| Production stream | Regional Kafka, when justified | Partitioning by tenant/store/EPC, replay, fan-out, and independent consumers |
| Identity | Local Argon2id + JWT for demo | Reproducible review; production direction is OIDC/SSO |
| Deployment | Render web + worker + managed PostgreSQL | Small operational surface and a continuously running worker |

Java/Spring Boot would also be a sound enterprise implementation. Python was chosen
because the exercise does not mandate Java, and the dominant scaling decisions here
are batching, partitioning, queue semantics, database contention, and regional
isolation—not request-handler language alone.

## Run with Docker Compose

Prerequisites: Docker with Compose v2.

1. Copy `.env.example` to `.env` and replace every `replace-with-...` value,
   especially `BOOTSTRAP_ADMIN_PASSWORD`.
2. Start PostgreSQL, run migrations, then start the API and worker:

   ```bash
   docker compose up --build -d
   ```

3. Create the first tenant administrator through the trusted deployment command:

   ```bash
   docker compose run --rm api abacus-cli bootstrap-admin
   ```

4. Open Swagger at <http://localhost:8000/docs>. Readiness is
   <http://localhost:8000/health/ready> and the deployed build identity is
   <http://localhost:8000/version>.

If `.env` does not override it, local Compose uses the platform integration key
`local-platform-key-change-before-deploy`. The setup instructions above require a
replacement, and Render generates a deployment-only secret.

## Run directly

Prerequisites: Python 3.13 and PostgreSQL 17. Copy `.env.example` to `.env`, replace
its placeholder secrets/password, and adjust `DATABASE_URL` before running:

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
$env:DATABASE_URL = "postgresql+psycopg://abacus:abacus@localhost:5432/abacus"
.\.venv\Scripts\alembic.exe upgrade head
.\.venv\Scripts\abacus-cli.exe bootstrap-admin
.\.venv\Scripts\uvicorn.exe abacus.main:app --host 0.0.0.0 --port 8000
```

Run the worker in a second terminal with the same environment:

```powershell
.\.venv\Scripts\python.exe -m abacus.worker
```

### Populated-database migration cutover

The RFID-to-task allocation migration is automatic for a clean deployment. For a
populated upgrade, first stop the old API and worker, take a backup, run the migration,
then inspect the deliberately blocked rows:

```powershell
.\.venv\Scripts\abacus-cli.exe list-reservation-cutover
.\.venv\Scripts\abacus-cli.exe reconcile-reservation-cutover `
  --task-id <uuid> --baseline <0-to-moved_quantity> `
  --reviewed-by <operator> --note "Evidence and change-ticket reference"
```

Compare each legacy task with RFID/physical evidence. The baseline is the number of
legacy moved units already reflected in RFID; zero is conservative because it keeps
uncertain units reserved rather than risking duplicate physical work. The command
locks the store/SKU and task, records reviewer/time/note, and is safe to repeat with
the same baseline. Until every row is reviewed, `/health/ready` returns 503, affected
API mutations are rejected, and the worker refuses to lease jobs. Once the list is
empty, start the new API, verify readiness, and then start the worker. The database
default is also fail-closed, so a legacy binary or direct insert cannot silently
create a task that appears reconciled.

The bootstrap command reads:

- `BOOTSTRAP_TENANT_CODE`
- `BOOTSTRAP_TENANT_NAME`
- `BOOTSTRAP_ADMIN_EMAIL`
- `BOOTSTRAP_ADMIN_DISPLAY_NAME`
- `BOOTSTRAP_ADMIN_PASSWORD` (minimum 12 characters)

It is idempotent for the configured administrator and never resets an existing
password silently.

## Test

Unit tests do not require a database. The end-to-end suite requires a disposable
PostgreSQL database whose name contains `test`; its `public` schema is reset.

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg://abacus:abacus@localhost:5432/abacus_test"
.\.venv\Scripts\python.exe -m pytest --cov=abacus --cov-report=term-missing
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m mypy src
$env:DATABASE_URL = $env:TEST_DATABASE_URL
.\.venv\Scripts\alembic.exe check
```

The active GitHub Actions workflow in `.github/workflows/ci.yml` provisions PostgreSQL
17, migrates a clean database, runs lint/format/strict mypy, the full test and coverage
gate, `alembic check`, builds this PDF, and builds the Docker image. The copy in
`ci/github-actions.yml` remains a portable template.

Build the recruiter-facing PDF from its Markdown source with:

```powershell
.\.venv\Scripts\python.exe scripts\build_engineering_response_pdf.py `
  docs\Sushant_Engineering_Response.md `
  output\pdf\Sushant_GreyOrange_Engineering_Take_Home_Answers.pdf
```

ReportLab is pinned in the `dev` dependency group so this build is reproducible in CI.

## Reviewer API path

Swagger exposes every request/response schema. The deployment command bootstraps the
Orange tenant and corporate administrator. The standard-library demo runner exercises
the frozen 100-store path and is safe to rerun with the same fixtures:

```powershell
$env:DEMO_BASE_URL = "http://localhost:8000"
$env:PLATFORM_API_KEY = "<deployment platform key>"
$env:BOOTSTRAP_ADMIN_EMAIL = "reviewer@orange.example"
$env:BOOTSTRAP_ADMIN_PASSWORD = "<deployment reviewer password>"
# Optional: known, separate credentials let reviewers log in as scoped users.
$env:DEMO_MANAGER_PASSWORD = "<manager password>"
$env:DEMO_ASSOCIATE_PASSWORD = "<associate password>"
.\.venv\Scripts\python.exe scripts\run_reviewer_demo.py
```

The runner never prints credentials. It verifies readiness, onboards 100 stores and
200 readers, creates narrowly scoped users, waits for catalog/RFID worker projections,
then evaluates and completes a replenishment task through verification. The equivalent
manual path is:

1. Log in at `POST /v1/auth/login`, then read the Orange `tenant_id` from
   `GET /v1/auth/me`. On a deliberately empty database, the alternative is to create
   the tenant at `POST /v1/platform/tenants` and then run `bootstrap-admin` with the
   same tenant code.
2. With `X-Platform-Key`, call
   `POST /v1/platform/tenants/{tenant_id}/stores:bulk-onboard` with an
   `Idempotency-Key`; include `SALES_FLOOR` and `BACKROOM` zones and readers.
   `python scripts/generate_store_batch.py --count 100` produces the assignment-sized
   deterministic request; `examples/stores.json` is the smaller hand-test fixture.
3. With `X-Platform-Key`, discover readers at
   `GET /v1/platform/tenants/{tenant_id}/devices`, then call the documented
   `credentials:rotate` endpoint and securely retain the returned device API key.
4. With the corporate JWT, use `POST /v1/users` to create store manager and associate
   users after their stores exist.
5. With `X-Platform-Key`, upload the CSV product master to
   `POST /v1/tenants/{tenant_id}/catalog/imports`; poll its status until `COMPLETED`.
6. Discover SKU UUIDs at `GET /v1/tenants/{tenant_id}/catalog/skus` with a JWT.
7. Send UUID-keyed observations to `POST /v1/device/read-batches` using the device
   credential. Inspect quarantine/progress via the platform observation endpoint.
8. Query paginated inventory at `GET /v1/tenants/{tenant_id}/inventory`.
9. With the corporate JWT and an `Idempotency-Key`, import policies; call
   `POST /v1/tenants/{tenant_id}/replenishment/evaluations` with a
   corporate/assigned-manager JWT, then
   claim the generated task, record the full movement while it is `IN_PROGRESS`,
   submit it for verification, and verify it with an assigned associate JWT.

Authentication boundaries are deliberate:

| Surface | Credential |
|---|---|
| Tenant/store/catalog integration and RFID remediation | `X-Platform-Key` |
| Reader ingestion | rotated `X-Device-Key` (`device_uuid.secret`); plaintext returned once |
| User/catalog lookup/inventory/replenishment | short-lived Bearer JWT |

## Authorization matrix

| Operation | Corporate admin | Store manager | Store associate |
|---|---:|---:|---:|
| Tenant-wide user administration/audit | Yes | No | No |
| Create/suspend associates | Yes | Assigned stores only | No |
| Read catalog | Yes | Yes | Yes |
| Read inventory | All stores | Assigned stores | Assigned stores |
| Manage policies | Yes | No | No |
| Evaluate/list replenishment | All stores | Assigned stores | Read assigned stores |
| Claim/execute tasks | Yes | Assigned stores | Assigned stores |
| Cancel/place tasks in exception | Yes | Assigned stores | No |

The tenant in a URL must match the verified token, and store-scoped principals must
pass an assigned store. Platform integration routes are intentionally a separate,
trusted administrative plane.

## RFID behavior

- A client-generated UUID is the tenant-wide event idempotency key.
- Same UUID + same payload is a duplicate; same UUID + different payload is a
  conflict. Neither creates a second effect.
- Structurally valid unknown EPCs or missing historical assignments are durably
  quarantined and can be replayed after correction.
- A bounded future clock skew is allowed; larger skew is quarantined so one bad
  reader cannot poison event-time state.
- Older reads are retained but marked `LATE_IGNORED`; they never move current state
  backward.
- Device-local sequences cannot order different readers, so ingestion is serialized per
  EPC and PostgreSQL assigns a unique monotonic acceptance sequence. The lowest
  sequence among non-quarantined observations is the deterministic tie winner.
  A later conflicting location at the same event time is quarantined as
  `AMBIGUOUS_SAME_TIMESTAMP_LOCATION`, independent of worker lease order. Repeated
  same-location evidence is a processed no-op and cannot count twice toward movement.
- `reader_sequence` is retained as device-local diagnostic evidence only; sequences
  from different readers are never used as a global ordering tie-breaker.
- A location move requires consecutive evidence within a configurable time window.
- Initial presence currently trusts one structurally valid, edge-filtered sighting;
  RSSI is retained but no universal signal threshold is assumed. Production onboarding
  should tune initial confirmation from Orange's reader/antenna measurements.
- EPC-to-SKU rebinding while an item has live state is quarantined for explicit
  reconciliation rather than silently moving counts between products.
- Observations are evidence. Receiving, POS sales, transfers, returns, shrink, book
  inventory, and variance workflows remain separate production integrations.

Inventory responses expose `projection_updated_at` (database processing time) and
`last_relevant_observation_at` (event time of the newest observation that changed or
reaffirmed that confirmed aggregate). The latter may be null for migrated historical
rows where event-time evidence cannot be reconstructed.

## Replenishment semantics

Policy winner order is deterministic:

1. effective and active,
2. store-specific before tenant-default,
3. selector specificity `SKU > STYLE > CATEGORY > SIZE`,
4. higher explicit priority,
5. reject an unresolved tie instead of guessing.

The provisional formula, because the exercise does not supply one, is:

```text
if floor >= minimum:
    recommendation = 0
else:
    recommendation = max(
        0,
        min(target - floor - open_work, backroom - reserved_for_open_work),
    )
```

Every evaluation persists its inputs, selected policy, formula, result, reason, and
inventory timestamp. One active task is allowed per tenant/store/SKU. Re-evaluation
adds only uncovered work. A task must enter `IN_PROGRESS` before movement is recorded.
Each confirmed same-store `BACKROOM -> SALES_FLOOR` EPC transition consumes at most
one task unit: the executing task first, then the oldest terminal reservation. Active
work reserves `quantity - linked transitions`; terminal work reserves
`moved_quantity - linked transitions`. Same-zone and opposite-direction reads consume
nothing. Claim ownership and an expected version prevent silent concurrent overwrites;
cancellation/exception transitions require manager-level permission.

## Hosting and immutable submission

`render.yaml` defines a paid web service, paid worker, and managed PostgreSQL 17 in
one region. The pre-deploy command applies migrations and requires the bootstrap
password secret; deploys are intentionally not automatic. The database is private to
Render services.

Recommended submission release procedure:

1. Set `BOOTSTRAP_ADMIN_PASSWORD` when applying the Blueprint.
2. Deploy one reviewed commit with web, worker, and database healthy.
3. Run the complete reviewer path and negative authorization checks against the
   public URL.
4. Record the commit SHA returned by `/version`, repository commit/tag, URL, and
   credentials in the submission.
5. Keep `autoDeployTrigger: off`; do not mutate code or schema after submission.
6. Keep paid services and database backups active through the review window.

A free-only deployment is useful for experimentation but is not recommended for an
immutable reviewer link: sleeping/cold starts, worker availability, resource limits,
and retention policies can change. Recheck [Render service types](https://render.com/docs/service-types),
[free-instance limits](https://render.com/docs/free), and the
[Blueprint specification](https://render.com/docs/blueprint-spec) immediately before
deployment.

## Scale path: 100 to 5,000 stores

The submitted topology is intentionally sized only after measuring readers/store,
filtered observations/second, burst factor, offline replay, retention, and freshness
SLO. The first steps are larger batches, edge filtering/buffering, API/worker replicas,
connection pooling, table partitioning, and archival.

At sustained multi-consumer volume, evolve to regional cells:

```mermaid
flowchart LR
    Edge[Store edge gateways] --> Kafka[(Regional Kafka)]
    Kafka --> State[Keyed EPC state processors]
    Kafka --> Audit[Raw-event archive]
    Kafka --> Consumers[Variance / analytics / alerts]
    State --> Regional[(Regional operational PostgreSQL)]
    Regional --> Control[Global control and reporting plane]
```

Kafka keys would use tenant/store/EPC according to the stream, with schema registry,
dead-letter/quarantine topics, replay tooling, lag SLOs, and regional failure
isolation. SQS is a good managed task-queue alternative, but it is less natural when
ordered partitions, long replay, and several independent stream consumers become
requirements.

## Honest boundaries and risks

- The submitted service uses polling, not PostgreSQL `LISTEN/NOTIFY`.
- Catalog, user, observation, inventory, policy and task collections use bounded
  `limit`/`offset`. Platform store/device/assignment listings are intentionally small
  submission APIs and still need pagination before large-estate operations.
- Shared-schema tenant protection is enforced in authenticated services and queries;
  the current schema does **not** yet have comprehensive composite tenant foreign
  keys or RLS. Production hardening should add both before untrusted internal writers.
- PostgreSQL exclusion constraints prevent overlapping device and EPC effective-date
  intervals, but source/business correction workflows still require operator policy.
- Policy mutations are serialized; policy change does not fan out millions of jobs
  automatically. Evaluations take a consistent tenant-policy snapshot. Operators call
  an explicit evaluation, while RFID inventory changes enqueue targeted store/SKU
  recalculations.
- The submitted single-service demo applies a bounded in-process login throttle to all
  attempts per client IP and to failed attempts per normalized tenant/account. Render
  alone sets `FORWARDED_ALLOW_IPS=*` because its load balancer is the only public
  ingress, allowing Uvicorn to derive `request.client.host` from the forwarded chain.
  Direct deployments retain Uvicorn's restrictive proxy default; they should trust
  only their own proxy addresses. Multiple API instances still need a shared limiter
  at the gateway or data store and bounded audit retention.
- The hosted slice caps catalog files and synchronous validation. Production-scale
  imports should land in object storage and use asynchronous chunked staging/COPY.
- No custom UI is included; the exercise asks for accessible REST APIs, and Swagger
  is the reviewer interface.
- No 5,000-store throughput claim is made without a supplied workload and measured
  load test.

All missing business facts and alternatives are kept explicit in
[docs/decisions.md](docs/decisions.md).
