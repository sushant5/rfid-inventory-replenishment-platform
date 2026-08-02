# Decision and Assumption Register

## How to read this document

This register prevents unspoken assumptions. Each statement is classified as one of:

- **Assignment fact:** explicitly stated in the Engineering Exercise.
- **Decision:** the proposed technical or delivery choice.
- **Documented assumption:** a provisional business or workload rule needed to make the system demonstrable. It must be confirmed, configurable, or called out in the submission.
- **Open input:** information not present in the assignment and not safe to invent.

This register describes the submitted implementation and clearly labels production
evolution or still-open business input.

## Assignment facts

1. Orange is a new customer to be onboarded as a brand/tenant with 100 stores.
2. The design must cover tenant setup, store hierarchy/configuration, hardware-to-store mapping, environment provisioning and scalable onboarding operations.
3. Orange supplies a large product master containing products, attributes, variants, sizes, colors, UPCs/SKUs and RFID-related mappings.
4. Overhead RFID readers continuously send real-time item observations from stores.
5. Users include store associates, store managers and corporate users, and they use a management dashboard and a mobile/store app.
6. The solution must create users, grant access and restrict each user to assigned stores and permitted operations.
7. Orange has policies defining desired sales-floor quantities by product, style, category or size.
8. The solution must ingest and manage those policies, calculate replenishment quantities and expose accessible REST APIs for testing.
9. The submission must include architecture, components, data flows, storage rationale, repository and hosted APIs, deployment considerations, tradeoffs, assumptions and risks.
10. Scaling to 5,000 stores and the other topics under "You may also address" are optional design considerations, not stated acceptance thresholds for the hosted system.

## Explicit decisions

### D1. Delivery scope

**Decision:** Build one coherent vertical slice. Sections 4 and 5 receive production-quality attention because the prompt explicitly asks to design and develop them. Sections 1-3 receive complete designs plus enough working support to exercise the identity and replenishment flows end to end.

**Why:** Implementing only isolated user and policy endpoints would be difficult to test meaningfully. Building a full retail platform would exceed a take-home's reasonable scope and reduce reliability.

**Not chosen:** Five independent services, a complete UI, and full receiving/transfer/return/shrink workflows.

### D2. Language and framework

**Decision:** Python with FastAPI, Pydantic validation, SQLAlchemy, Alembic and Pytest.

**Why:** It supports concise typed APIs, automatic OpenAPI documentation, fast test development and straightforward deployment. System scale will be governed primarily by batching, persistence, partitioning and worker design rather than the API language alone.

**Alternative:** Java with Spring Boot would be a strong enterprise choice and is preferable if the job or review criteria explicitly require Java. It was not selected because the assignment itself does not mandate a language and the narrower Python implementation reduces delivery risk.

### D3. Application boundary

**Decision:** Use a modular monolith for the HTTP API plus a separately running background worker from the same repository and domain modules.

**Why:** It preserves clear boundaries and independent worker scaling without introducing distributed transactions and multiple deployments for a take-home.

**Alternative:** Microservices offer independent ownership and deployment, but those benefits do not outweigh operational complexity at this stage. An unstructured single process is also rejected because worker failures and long-running ingestion should not block request handling.

### D4. Hosted topology

**Decision:** Use a paid Render web service, a paid Render background worker and Render-managed PostgreSQL in the same region. Deploy a tested commit/image, run migrations deliberately and disable automatic deployment for the submitted release.

**Why:** The public API, continuously running worker and managed database match the design with few external dependencies. Paid instances avoid free-tier sleeping and database-expiry risks during reviewer testing.

**Alternative:** A free-only deployment is acceptable for experimentation but not recommended for an immutable take-home link. Kubernetes is unnecessary. Cloud-vendor primitives would be viable but add setup and reviewer-reproducibility work.

### D5. Multi-tenancy

**Decision:** Use one shared PostgreSQL schema with `tenant_id` on tenant-owned
records, tenant-aware service queries, scoped JWT authorization, and tenant-qualified
uniqueness constraints.

**Why:** It is operationally efficient for onboarding many stores and allows
transactional joins. The current trust boundary assumes only these services and the
worker write domain data. Comprehensive composite tenant foreign keys and PostgreSQL
RLS are production hardening work and are not claimed as implemented.

**Alternatives:** Schema-per-tenant complicates migrations and analytics. Database-per-tenant improves physical isolation but increases provisioning and operational cost. High-scale or regulated tenants can later move to dedicated regional cells/databases.

### D6. Store and hardware mapping

**Decision:** Represent tenant -> store -> zone explicitly. Store reader-device
assignments as effective-dated mappings, not only as a mutable `store_id` on a device.
Retain `antenna_port` on observations; model a separate antenna/port topology only
after Orange supplies reader capabilities and port/multiplexing semantics.

**Why:** A moved device and a late RFID observation must resolve against the mapping that was valid when the observation occurred.

**Alternative:** A direct mutable foreign key is simpler but destroys mapping history and can misattribute late data.

### D7. Product master ingestion

**Decision:** Use stage -> validate/normalize -> reconcile -> promote. Keep identifiers and relationships relational, and reserve JSONB for genuinely variable attributes. Retain import status, checksums, row-level errors and reconciliation counts.

**Why:** Direct writes can leave a partially valid catalog and make source-to-platform differences opaque. Relational constraints protect SKU/UPC/EPC identity; JSONB avoids a column for every optional attribute.

**Alternative:** A document database is flexible but weakens uniqueness and cross-entity integrity for the most important catalog identifiers.

### D8. RFID transport for the hosted slice

**Decision:** Accept authenticated, batched HTTP observations. Validate the complete
request before acceptance: a malformed envelope or structurally invalid event rejects
the batch, while a structurally valid but unknown EPC/device relationship is durably
accepted into quarantine. Persist retained raw event evidence and durable inbox jobs
in the same PostgreSQL transaction. Workers poll and claim jobs with leases,
`FOR UPDATE SKIP LOCKED`, heartbeats, and ownership-safe completion.

**Why:** This provides durable acceptance and restart recovery without an external
broker dependency. Polling adds bounded latency but keeps the correctness mechanism
simple. `LISTEN/NOTIFY` could later be added only as a wake-up hint; it is not part of
the submitted implementation.

**Alternatives:**

- **Kafka:** best fit when sustained volume, replay, multiple consumers or long outages justify it; included in the production-scale design, not required for the hosted demo.
- **SQS:** easier managed queueing and a reasonable alternative, but adds credentials/network dependency and offers weaker streaming replay/fan-out semantics than Kafka.
- **In-memory queue:** rejected because accepted data would be lost on restart.

### D9. RFID processing guarantees

**Decision:** Promise at-least-once processing with idempotent effects, never
exactly-once delivery. Store both `observed_at` and `ingested_at`; prevent older
observations from regressing current state. Reject inactive devices during
authentication. Quarantine unknown EPCs, missing effective assignments, excessive
future skew, suspended-tenant queued work and EPC rebinds requiring reconciliation.
A reused event ID with different content receives an explicit conflict disposition.

**Why:** Retries and failures make duplicates normal. Idempotency and version checks produce safe outcomes without an unverifiable exactly-once claim.

### D10. Observations versus inventory truth

**Decision:** Retain raw RFID event fields/payload as evidence while allowing status,
resolution and quarantine metadata to advance. Separate that evidence from derived
current item presence and aggregate floor/backroom availability. Treat a future
book-inventory ledger as a different source of truth; it is a design boundary, not a
submitted table or API.

**Why:** A radio observation is evidence, not automatically a sale, receipt, transfer or shrink transaction. Keeping these concepts separate enables confidence, freshness and variance reconciliation.

**Alternative:** Directly overwriting inventory from every read is simple but makes noise, absence and auditability unsafe.

### D11. Authentication and authorization

**Decision:** For the hosted demonstration, use seeded/local credentials with securely hashed passwords and short-lived JWT access tokens. Pair roles with tenant/store scopes and enforce permissions server-side on every operation. Derive tenant context from verified identity/grants, never trust a request-supplied tenant alone.

**Production direction:** Integrate an external OIDC/SSO provider and keep the same authorization model behind it.

**Alternative:** Pure role-based access without resource scope is rejected because it cannot safely model users assigned to subsets of stores. A fully generic policy language is unnecessary for this exercise.

### D12. Replenishment policies

**Decision:** Store effective-dated, revisioned rules in relational tables and snapshot
the selected inputs in each evaluation line. Resolve one winning rule deterministically
by active/effective state, store scope before tenant default, specificity (SKU before
broader selectors), explicit priority and then a validation error for an unresolved
tie. A full before/after policy history is not implemented.

**Why:** Determinism and an explanation trail matter more than building a general-purpose rule engine.

**Alternative:** Embedding executable expressions or a generic rules engine adds security, testing and governance complexity not requested by the assignment.

### D13. Replenishment task idempotency

**Decision:** Enqueue a targeted recalculation on confirmed RFID inventory changes
and expose an on-demand explanation API for policy/configuration changes. Enforce at
most one active task per tenant/store/SKU, reserve backroom stock for already-open
work, and subtract reserved work from new recommendations. Preserve the full active
reservation even after partial movement is recorded, and preserve verified work until
a later RFID aggregate transition prevents an immediate duplicate task.

**Why:** Repeated RFID events must not generate duplicate work for associates.

### D14. API style

**Decision:** Version REST endpoints under `/v1`, publish OpenAPI/Swagger, use stable
identifiers, timezone-aware ISO-8601 timestamps, structured problem responses,
bounded `limit`/`offset` on high-growth domain collections, and idempotency keys for
retriable commands/imports. Platform store/device/assignment listing pagination is
deferred and documented.

**Why:** These conventions make reviewer testing and safe client retries predictable.

### D15. Scaling strategy

**Decision:** Scale the hosted architecture vertically and with API/worker replicas only after measurement. Add edge aggregation, connection pooling, time partitioning and archival as required. For 5,000 stores, evolve to regional cells with regional Kafka, keyed stream processors, regional operational databases and a separate global reporting/control plane.

**Why:** Store count alone does not determine throughput. The transition must be based on observed event rate, batch size, retention, fan-out, replay window and latency, not an arbitrary threshold.

## Documented assumptions requiring confirmation

These rules are needed to make a demonstrable v1, but they are **not stated by the assignment**.

| ID | Provisional assumption | How the design limits risk |
|---|---|---|
| A1 | The hosted slice targets accepted observations becoming queryable within 10 seconds at p95 under the documented test load. | Return inventory `as_of`; derive lag from stored observation/job timestamps and publish measured results rather than claiming the target untested. |
| A2 | RFID devices or an edge gateway can batch observations, buffer during loss of connectivity and retry with stable event IDs. | The API is idempotent; the production design identifies edge buffering as required. |
| A3 | Source-supplied SKU, UPC and EPC mappings are authoritative unless a reconciliation conflict is detected. | Reject conflicting imports or quarantine an unsafe live EPC rebind; retain evidence instead of guessing a mapping. |
| A4 | Product imports can be incremental as well as initial full loads; absence from an incremental file does not delete data. | Require an explicit import mode and explicit delete/end-date operation. |
| A5 | A product-master batch is promoted all-or-nothing; invalid rows prevent every canonical catalog mutation. | Preserve all staged rows and errors so the source can be corrected and resubmitted safely. |
| A6 | A move between zones requires consecutive confirmation within a bounded window; a single missing or weak read does not prove absence. | Keep candidate and confirmed state internally; expose inventory `as_of`. Confirmation count/window remain configuration. |
| A7 | A policy supplies at least a minimum floor quantity and target floor quantity; maximum is optional. | Validate `0 <= minimum <= target <= maximum` when maximum exists. Model supports later rule changes. |
| A8 | When floor quantity is below minimum, the v1 recommendation is `min(target - floor - open_task_qty, backroom - reserved_backroom)`, bounded below by zero. | Return the inputs, chosen rule and formula in the explanation response. Replace once Orange confirms semantics. |
| A9 | A selector at style/category/size scope yields a target for each matched SKU, not one pool to allocate across SKUs. | Document this limitation; aggregate allocation requires an explicit allocation strategy from the customer. |
| A10 | Demo users may be pre-seeded so reviewers can test each role without an external identity provider. | Use non-production credentials, rotate/remove after review and document the production OIDC path. |
| A11 | PostgreSQL is selected for the functional hosted demonstration, not asserted sufficient for every possible 100-store workload. | Require a supplied load profile and lag measurements before setting capacity; retain explicit Kafka evolution gates. |
| A12 | The v1 replenishment formula uses confirmed RFID-derived floor/backroom presence. Book inventory and formal variance are separate future integrations. | Persist the inventory timestamp and calculation inputs; make the source strategy replaceable after customer confirmation. |

## Alternatives and rejection summary

| Concern | Selected | Not selected now | Reason |
|---|---|---|---|
| Service shape | Modular API + worker | Five microservices | Less deployment and transaction complexity |
| Hosted messaging | PostgreSQL durable inbox | Live Kafka/SQS | Fewer external failure and expiry points for reviewer testing |
| High-scale messaging | Regional Kafka | One global database queue | Replay, partitioning, consumer independence and regional isolation |
| Catalog storage | PostgreSQL + selective JSONB | Document-only database | Identifier integrity and reconciliation |
| Tenant isolation | Shared schema with layered enforcement | Schema/database per tenant | Lower provisioning and migration overhead at initial scale |
| Demo identity | Local login + JWT | Mandatory external IdP | Repeatable reviewer access; OIDC remains production direction |
| Replenishment rules | Versioned relational policies | General rules engine | Deterministic, explainable and testable v1 |
| UI | Swagger/API-first | Custom dashboard/mobile app | The Engineering prompt requires accessible REST APIs, not a frontend |
| Hosting | Paid managed services | Free sleeping/expiring services | Reduce failure risk after immutable submission |

## Open recruiter/customer inputs

These should be answered before final scope freeze. Until then, they remain visible gaps rather than hidden assumptions.

### Submission constraints

- Exact deadline and timezone.
- Exact job title/level and whether Java/Spring or another stack is preferred.
- Any instructions outside the DOCX, including email or verbal guidance.
- AI-assistance, dependency, licensing, cloud-provider and repository-visibility rules.
- Required deliverables: repository, hosted URL, design document, video, slides or API collection.
- Expected hosted uptime and whether any deployment/configuration/database maintenance is prohibited after submission.
- Hosting budget and how long the environment must remain available.

### Workload and service objectives

- Readers and antennas per store; raw and edge-filtered observations per second; batch size and payload size.
- Peak/burst multipliers, offline buffer duration and retry behavior.
- Required end-to-end freshness, availability, recovery time and recovery point.
- Raw-observation, audit, user and inventory-history retention.
- Data residency, encryption, compliance and tenant-isolation requirements.

### Source and domain semantics

- Product-master format, schema, transport, schedule, full/incremental semantics and deletion rules.
- Identifier ownership and uniqueness rules for SKU, UPC and EPC; reuse/re-tagging behavior.
- Store hierarchy, zone taxonomy, timezones and hardware installation/move workflow.
- Reader payload schema, clock quality, sequence behavior, signal fields and edge deduplication.
- What constitutes confirmed floor/backroom presence and how confidence should be calculated.
- Authoritative inventory source and integration contracts for POS, receiving, transfers, returns and shrink.
- Policy units, precedence, effective dating, overlap behavior, target allocation and exception approvals.
- Replenishment task ownership, SLA, cancellation, verification and escalation lifecycle.
- Corporate-user scope, manager multi-store behavior, permitted operations and production identity provider.
