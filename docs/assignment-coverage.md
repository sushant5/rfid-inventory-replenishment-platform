# Engineering Assignment Coverage

## Purpose and scope

This document traces the proposed submission to the **Engineering Exercise** in `Engineering Take-Home Exercise.docx`. The separate Product Manager exercise is out of scope.

The table distinguishes three levels of obligation:

- **Required - design:** the prompt asks for a design and explanation.
- **Required - build:** the prompt explicitly says to design and develop, or asks for testable hosted APIs.
- **Optional:** the prompt introduces the item with "You may also address."

The assignment does not supply workload rates, latency targets, source schemas, policy precedence, retention periods, identity-provider details, or cloud constraints. Those are not silently treated as facts. Provisional choices are recorded in `decisions.md` and remain configurable where practical.

## Requirement traceability

| ID | Assignment requirement | Obligation | Submission evidence | Verification |
|---|---|---|---|---|
| 1.1 | Onboard Orange as a brand/tenant | Required - design | Active tenant creation, idempotent bootstrap/onboarding and isolation strategy; staged tenant lifecycle is an evolution | Architecture review; tenant-isolation tests for implemented paths |
| 1.2 | Provision all 100 stores | Required - design | Atomic/idempotent bulk contract accepts up to 500 stores, so one request covers all 100 | Valid, duplicate, conflicting-idempotency and transactional rejection scenarios |
| 1.3 | Store hierarchy and configuration | Required - design | Tenant/store/zone hierarchy and configuration model | Constraint and authorization scenarios |
| 1.4 | Hardware-to-store mapping | Required - design | Effective-dated reader-to-store/zone mapping plus retained antenna-port evidence; explicit antenna topology is an open source-contract extension | Current, moved-reader and late-observation scenarios |
| 1.5 | Environment provisioning | Required - design | Shared SaaS environment model, tenant configuration, secrets and deployment boundaries | Deployment runbook and readiness checks |
| 1.6 | Operational onboarding at scale | Required - design | Validated atomic batch, idempotent retry, persisted result counts and deterministic fixture generation | Automated 100-store/200-device PostgreSQL rehearsal plus duplicate/conflicting-key tests |
| 2.1 | Ingest a large product master | Required - design | Staged import workflow supporting products, attributes, variants, sizes, colors, UPCs/SKUs and RFID mappings | Representative valid/invalid import fixtures |
| 2.2 | Validate, transform and reconcile | Required - design | Schema validation, normalization, identifier uniqueness, conflict reporting and reconciliation summary | Created/updated/unchanged/rejected counts; retry behavior |
| 2.3 | Store the product master and explain why | Required - design | Relational core, constrained identifiers and JSONB only for variable attributes; storage rationale | Schema review and integrity tests for implemented constraints |
| 3.1 | Ingest and process continuous real-time RFID observations | Required - design | Batched device endpoint, durable inbox, asynchronous worker and edge retry contract | Duplicate/conflict, late, future-skew, unmapped, move-confirmation and expired-lease reclaim scenarios |
| 3.2 | Model and store RFID data | Required - design | Immutable observations separated from derived item presence and aggregate inventory | State-transition and non-regression tests |
| 3.3 | Choose storage technologies and explain why | Required - design | PostgreSQL for the hosted slice; Kafka-based regional design for measured production demand | Decision record, load-test method and explicit scaling limits |
| 4.1 | Create users and grant platform access | **Required - build** | User lifecycle APIs, password-based demo authentication, short-lived JWTs, roles and scoped grants | Authentication, user-management and negative authorization tests |
| 4.2 | Restrict users to assigned stores and permitted operations | **Required - build** | Tenant-bound roles plus store scopes; server-derived tenant context | Cross-tenant, unassigned-store and insufficient-permission tests |
| 5.1 | Ingest replenishment policies | Required - design | Validated, idempotent policy import/API with effective dates and scope | Invalid, duplicate, overlapping and effective-date scenarios |
| 5.2 | Store and manage replenishment policies | **Required - build** | Policy CRUD/lifecycle APIs, effective intervals, revision/timestamps and deterministic precedence | CRUD, validation, interval-boundary and precedence tests |
| 5.3 | Calculate required sales-floor replenishment | **Required - build** | Explainable calculation using floor state, backroom availability and open work; owned execution and immutable terminal outcomes | Boundary, shortage, insufficient-backroom, duplicate/replacement-task and lifecycle authorization tests |
| 5.4 | Publish accessible REST APIs for testing | **Required - build** | Implemented OpenAPI/Swagger service, stable bootstrap procedure, repository instructions and Render Blueprint; public URL pending deployment approval | Clean reviewer walkthrough against the deployed immutable version |

## Candidate expectations

These expectations apply across Sections 1-5 and are required even where the assignment asks only for design.

| Expectation | Planned location/evidence |
|---|---|
| High-level architecture | `architecture.md`, including hosted and production-scale diagrams |
| Key services and components | API modules, background worker, PostgreSQL, edge contract and future Kafka processors |
| Data flow across ingestion, storage, processing and application layers | Numbered flows and Mermaid diagrams in `architecture.md` |
| Storage choices and rationale | `architecture.md` and the alternatives in `decisions.md` |
| APIs, Git repository and hosted services | OpenAPI/Swagger, repository README and Render Blueprint; URL/commit added only after deployment verification |
| Deployment considerations | Render topology, migrations, health checks, release pinning and local fallback |
| Tradeoffs, assumptions and risks | `decisions.md`; no unknown business rule is presented as an assignment fact |

## Optional considerations

The assignment says the candidate **may also address** the following. They strengthen the design but are not all commitments for the hosted implementation.

| Optional topic | Planned treatment | Implementation boundary |
|---|---|---|
| Support 100 stores today and 5,000 tomorrow | Capacity model, scale gates and regional-cell architecture | Hosted slice is not claimed to prove 5,000-store capacity |
| Detect and reconcile inventory variance | Keep observed RFID presence conceptually separate from a future book-inventory ledger and variance workflow | Design only; book inventory and formal variance reconciliation are not implemented |
| Late, duplicate or noisy RFID reads | Event idempotency, observed/ingested timestamps, non-regressing state and bounded movement confirmation | Covered by the PostgreSQL end-to-end slice |
| Near-real-time floor availability | Durable asynchronous processing, bounded polling, projection-update time, and latest relevant observation-event time | Target is a documented service objective, not a claim until measured |
| Receiving, transfers, returns and shrink | Define these as future book-ledger inputs rather than infer them from radio reads | Design boundary only; operational workflows are outside the core build |
| CI/CD, deployment and observability | Tests, migrations, health endpoints, structured logs, release identity and a deployment blueprint | Metrics/alerts are an explicit production recommendation, not submitted telemetry infrastructure |
| Hands-on coding, design and deployment choices | One cohesive vertical slice with clear module boundaries and operational documentation | Avoid microservice overhead that would dilute the required workflows |

## Implementation order used

The written submission should retain the assignment's 1-2-3-4-5 order. Development should follow dependencies:

1. Tenant, stores, zones and hardware foundations (Section 1).
2. Identity, roles and store-scoped authorization (Section 4).
3. Product master and SKU/EPC identity (Section 2).
4. RFID observations and current inventory state (Section 3).
5. Replenishment policies, calculations and tasks (Section 5).
6. End-to-end tests, deployment rehearsal and submission packaging.

## Definition of assignment-complete

The submission is ready only when:

- Every required row above points to a concrete design section, API, test or hosted behavior.
- Sections 4 and 5 have working, testable APIs rather than design prose alone.
- A reviewer can run the documented happy path and negative authorization cases without private setup knowledge.
- The hosted release and the repository tag/commit describe the same version.
- Unknown or provisional business rules are identified as assumptions, not hidden in code.
- The documentation states measured results and known limits without claiming exactly-once delivery or demonstrated 5,000-store scale.
