# PostgreSQL adapter architecture

Status: draft for discussion. Package naming is settled as `resumable-store-postgres-sqlx`; API
shapes and future release ownership remain proposals. This document does not authorize publication.

The [core specification](../../resumable/docs/specification.md) defines required behavior; the
[core architecture](../../resumable/docs/architecture.md) defines execution semantics. This document
describes how a PostgreSQL implementation supplies those guarantees and integrates with Planum.

## Package names and future adapters

Use `resumable-store-<database>-<driver>` for concrete storage adapters:

- `resumable-store-postgres-sqlx`: the chosen name for this package.
- `resumable-store-sqlite-sqlx`: a future SQLite implementation using SQLx.
- `resumable-store-postgres-tokio-postgres`: an example name for a future alternative driver.

Storage is the only required adapter role in the current design. The `store` segment leaves room
for other integration roles without introducing new contracts before they are needed. Scheduling,
observability, and external-service integrations remain application concerns in the current scope.

The database identifies storage behavior, while the driver identifies connection and transaction
compatibility. Both matter to callers. The `adapter` suffix adds little information, and
`resumable-sqlx-adapter` obscures which database guarantees the implementation supports.

Keep each database implementation in its own package. A shared driver does not imply shared SQL,
locking, clock behavior, migrations, or transaction extensions. Introduce shared implementation
helpers only when concrete duplication justifies them; the core Store contract already provides
the common interface.

Naming does not promise compatibility of database files, schemas, or migration histories between
different drivers. Sharing a schema across two implementations requires an explicit compatibility
contract and verification. Independently developed adapters must not concurrently own its migrations.

## Ownership and dependencies

Dependency direction remains `server → adapter → resumable`, with the server also using core types.
The adapter depends on SQLx/PostgreSQL and implements the portable Store contract. It accepts explicit
configuration and connections; it never depends on the server or reads application environment
variables itself.

The adapter owns execution persistence, concurrency control, mutation receipts, migrations,
inspection queries, and bounded discovery of eligible work. The application owns operation dispatch,
worker lifecycle, authorization, provider clients, domain records, and presentation. Actor and entity
links belong in application records, outside generic execution metadata.

The core and adapter packages remain in the DACB workspace for now. When extracted later, move the
core and its adapters together into one repository. Publication and release ownership for that
repository remain to be decided.

## Connections and transactions

Use SQLx for this implementation and accept a caller-provided `PgPool`. The standalone Store starts
short transactions for atomic transitions. Reusing the application's pool permits one connection
budget and compatible transaction extensions; callers may provide a dedicated pool when appropriate.

SQLx is a dependency choice, not a performance guarantee. Driver replacement needs workload evidence
and must account for transaction compatibility. Keep the portable core free of SQLx types so another
driver can implement Store independently.

`PostgresStore::create_execution_in_transaction` and
`PostgresStore::complete_step_in_transaction` accept a caller-owned SQLx transaction. They never
commit, roll back, or acquire another connection. Completion locks the execution row before checking
the lease, revision, expiry, fencing generation, and attempt, and holds that lock through the caller's
commit or rollback. This serializes takeover and cancellation against the domain transaction.

After a callback commits a step completion, the engine loads the step before its ordinary completion
write. A matching completed attempt and encoded result means the adapter already committed the
checkpoint; the engine refreshes its revision and returns the callback result. If the process dies
after commit, a resumed engine reads the stored step result as usual. If commit outcome is uncertain,
the caller retries the identical completion request (including its mutation ID) in a new transaction;
the durable mutation receipt returns the recorded outcome. Keep the same request value across retries.

## Persistence model

Use fixed `resumable_` names for runtime tables. Proposed logical records are:

- Executions: immutable operation/input identity, status, revision, retry eligibility, cancellation,
  lease owner/token/claim identity, expiry, and retained fencing generation.
- Steps: exact step identity within an execution, current status/attempt/generation, initial retry
  policy, durable result or failure, next retry deadline, and latest bounded progress.
- Attempts: immutable attempt identity and starting generation, with its recorded outcome.
- Mutation receipts: request identity/fingerprint and sufficient outcome data to reconcile identical
  mutations after an uncertain commit, before evaluating current ownership.

RS-016 must turn these records into concrete columns, checks, foreign keys, uniqueness constraints,
and indexes. Enforce core value bounds in storage where possible. Preserve encoded input/result
bytes and exact step-name comparison. Receipts must not cascade away while their execution remains
recoverable; retention must follow the core's identity and recovery guarantees.

### PostgreSQL schema

Adapter migrations are tracked independently of `server/migrations/` and the application's
`_sqlx_migrations` history:

- `resumable_executions` stores the exact input/result envelopes as codec, schema version, and
  bytes; metadata; lifecycle/revision; cancellation; failure; and the current lease plus retained
  fencing generation. The optional `run_at` deadline gates worker discovery and claims. Pending-work and expired-lease partial indexes support bounded discovery
  without claiming candidates.
- `resumable_steps` uses `(execution_id, name)` uniqueness with the PostgreSQL `C` collation so
  step names compare by their exact UTF-8 bytes. It stores the first retry policy, latest state,
  result envelope, retry deadline, and the generation that began a running attempt.
- `resumable_attempts` retains attempt number, starting generation, and outcome. Its primary key
  prevents duplicate attempt numbers. A running attempt cannot restart in the same generation, but
  a recorded failure can be retried there when the execution remains leased. A trigger allows the
  step attempt counter to remain stable or increase by one only. Store transactions update that
  counter and insert the matching attempt together.
- `resumable_progress` stores only the latest lossy observation per step, separate from correctness
  state.
- `resumable_mutation_receipts` retains request fingerprints and committed outcomes for ambiguous
  mutation recovery. It cascades only when the execution itself is purged, which the core permits
  only for completed or cancelled executions.

Checks bound codec IDs, encoded values, metadata size/shape, step and operation names, progress
messages, revisions, generations, and attempt numbers. The core remains responsible for validating
the finer metadata label/key/value limits and retry-policy encoding on both writes and reads.
Lifecycle checks keep lease fields attached only to running executions, require terminal timestamps
for terminal states, and require failure classes for failed records. The JSONB `record` snapshot
matches each row's identity, status, revision, fencing generation, or attempt counter; adapter
transactions update the snapshot and its indexed/checked columns together. Foreign keys cascade
steps, attempts, progress, and receipts only during an execution purge.

Lease expiry and retry eligibility use `clock_timestamp()` inside the Store transaction that
claims or begins work. The timestamp columns use `TIMESTAMPTZ`; worker-supplied timestamps are
observational values and never authorize ownership. The expired-lease index is ordered by expiry;
the due-retry index is ordered by `next_attempt_at`. Discovery must still recheck eligibility and
ownership atomically when claiming.

The adapter migration is independent of application tables. At the time of this design, existing
backup and invitation work had separate dispatch owners. RS-020 later moved invitation delivery
fully onto resumable execution and removed its former table because there was no queued work to
preserve. Backup execution remains an application-owned integration concern. Adapter migrations and
SQLx offline metadata belong in `crates/resumable-store-postgres-sqlx/`; server queries and
migrations remain package-local to `server/`.

The initial schema uses the fixed `resumable_migrations` history name. The adapter-owned runner
serializes migration callers with a PostgreSQL advisory lock, verifies the SHA-256 checksum, and
applies the embedded SQL and history record in one transaction. Store construction does not run
migrations; the host calls `migrate` and then `validate_schema` before starting workers.

All application queries use compile-time checked SQLx macros and package-local offline metadata.
Runtime table names, including the migration history table, are fixed so queries can be checked
against the actual schema.

## Atomicity, clocks, and discovery

Each Store mutation atomically validates its preconditions and records all effects and its receipt.
Use row locking or equivalent conditional writes for revisions, lease ownership, legal transitions,
and monotonic attempt allocation. Acquire locks in a consistent order across transitions.

Use the database clock inside the transition to decide lease expiry and retry eligibility, following
the core architecture. Recheck ownership after waiting for locks. A worker's clock cannot authorize
a mutation, and an expired lease cannot be revived. The exact lease check and commit synchronization
protocol must be specified before implementing transaction extensions.

Discovery returns bounded candidates ordered deterministically by eligibility and identity. The
subsequent atomic claim establishes ownership. Index pending work, due retries, and expired leases
according to the final query predicates; finding a candidate never reserves it by itself.

## Independent migrations

The adapter owns embedded SQL files in its package's `migrations/` directory and an independent
version sequence starting at `0001`. Application migration versions may overlap because the histories
are separate. Never place adapter migrations in `server/migrations/` or its `_sqlx_migrations` table.

The default history table is `resumable_migrations`. The component name identifies ownership;
database and driver names belong in package names. Replacing an implementation does not implicitly
create a new schema history; actual reuse still requires the compatibility contract described above.

The initial release keeps this history name fixed. A runtime override would add an application
configuration surface without enabling another runtime namespace or a second adapter installation
in the same database. Renaming the history table requires a later explicit maintenance migration
that preserves its records and coordinates all migration callers.

Expose explicit migration and schema-validation entry points. Store construction does not implicitly
run DDL. The host chooses whether deployment tooling or startup invokes migration; workers begin only
after schema validation. Application migrations that reference adapter records declare the required
adapter schema version and run after those adapter migrations.

The migration implementation must serialize concurrent callers, validate applied checksums and
versions, reject incompatible/newer histories, and atomically commit each transactional migration
with its history record. A crash must leave either the previous version or a fully recorded upgrade.
Initially require transactional migrations; support for nontransactional DDL needs a separate design.

The workspace currently pins SQLx 0.8.6. Its standard Migrator does not expose a runtime migration
history-table override. The adapter therefore owns a narrow runner for the fixed history table and
embedded scripts. Runtime Store queries remain compile-time checked; dynamic SQL is isolated to
executing the embedded migration script and creating the fixed history table.

## Schema and release compatibility

Initially pair each worker release with exactly one supported adapter schema version. A worker must
reject a different installed schema version before executing work; migration bookkeeping names
never serve as compatibility signals. Coordinate upgrades so workers and schema move together.

Plan a later compatibility-window policy for additive upgrades, allowing adjacent worker/schema
versions to overlap during staged rollout. Define its version range and rollout gates before relying
on mixed-version operation.

Prefer additive migrations and staged rollout: introduce compatible storage, deploy compatible
workers, retire old workers, then remove obsolete storage in a later release. Never edit applied
migrations. Operational rollback means running a compatible binary or an explicitly designed recovery
procedure; do not assume automatic down migrations preserve execution history.

Keep `.sqlx/` package-local and regenerate it against the adapter's migrated PostgreSQL schema.
Offline compilation must cover both the server and adapter. Preparation tooling must apply adapter
migrations before queries that reference their tables are checked.

## SQLite and additional implementations

Future SQLite adapters implement the same core invariants and run the shared conformance suite.
They own separate migrations and explicitly document connection settings, write concurrency, clock
authority, claim serialization, and supported transaction extensions. PostgreSQL SQL and locking
strategies must not be copied under an assumption of equivalent behavior.

Driver-specific transaction extensions stay in their concrete package. Applications choosing a
different driver can still use the portable Store API, but shared domain transactions require
compatible connection and transaction types or a separately designed integration mechanism.

## Coexistence and verification

Existing `backup_execution_operations` keeps its history and current dispatch owner during schema
introduction. Adapter installation does not enqueue, convert, or resume its work. Invitation email
delivery is now resumable-only; RS-020 established that no prior queued work needed import.
Production backup-worker rollout must provide a recovery path for existing histories and maintain
one dispatch owner throughout cutover.

RS-017 verifies Store conformance, restart recovery, uncertain commits, concurrent claims,
stale-owner rejection, retry deadlines, and rollback for Store-owned transactions. RS-018 verifies
caller-owned transaction commit and rollback, uncertain-commit recovery, and lease-loss behavior.
Migration verification should cover concurrent callers and checksum/version incompatibilities.
The runner uses one fixed history name; custom history names are not supported.

## Decisions for review

- Decide release and publication ownership for the future repository containing the core and adapters.
- Define the later adjacent-version compatibility window and rollout gates before enabling
  mixed-version workers; the initial policy requires an exact worker/schema version match.
