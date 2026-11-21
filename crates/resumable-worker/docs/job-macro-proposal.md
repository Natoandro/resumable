# Worker job macro proposals

These examples sketch optional APIs for defining and registering jobs. The
current worker API uses `OperationKey::new`, `OperationRegistry::register`, and
`OperationRegistry::register_typed`.
The examples assume that the registry and handler functions are in scope.

## Register one job

Proposed:

```rust
register_job!(registry, ("billing.send_invoice", 1) => send_invoice).await?;
```

Current:

```rust
let key = OperationKey::new("billing.send_invoice", 1)?;
registry.register(key, send_invoice).await?;
```

The macro saves one line. The existing API is a reasonable default for a
single registration.

## Register several jobs

Proposed:

```rust
register_jobs!(registry;
    ("billing.send_invoice", 1) => send_invoice,
    ("billing.reconcile", 2) => reconcile,
).await?;
```

Current:

```rust
let send_invoice_key = OperationKey::new("billing.send_invoice", 1)?;
registry.register(send_invoice_key, send_invoice).await?;

let reconcile_key = OperationKey::new("billing.reconcile", 2)?;
registry.register(reconcile_key, reconcile).await?;
```

A `macro_rules!` macro could expand into sequential registrations, returning
the first error. As with the explicit calls, a successful earlier registration
would remain in the registry if a later one fails. Each job's persisted name
and version stay visible in the worker's registration list.

## Declare a typed job once

Proposed function attribute and explicit registration:

```rust
#[job(name = "billing.send_invoice", version = 1)]
async fn send_invoice(
    input: InvoiceInput,
    app: AppContext,
    execution: JobExecutionContext,
) -> Result<Receipt, OperationError> {
    // Job body
}

// During worker setup:
send_invoice_job::register(&registry).await?;
```

Without the attribute, register the same handler with the typed helper:

```rust
let key = OperationKey::new("billing.send_invoice", 1)?;
registry.register_typed(key, send_invoice).await?;
```

The attribute would preserve the ordinary function and generate a companion
`send_invoice_job` module. Its `register` function would perform the same key
construction and typed registration shown above. The typed helper would adapt
the worker's JSON input and output to the handler's Rust types. The companion
module could also expose a typed job descriptor for enqueueing with the same
operation key and input type. The enqueue API remains open.

### Register by job name

The default generated module would be `send_invoice_job`. A declarative macro
can accept that descriptor directly:

```rust
register_job!(registry; send_invoice_job).await?;
```

This expands to `send_invoice_job::register(&registry).await?`. The same form
could support `register_jobs!(registry; send_invoice_job, reconcile_job).await?`.

Using the handler name instead, as in
`register_job!(registry; send_invoice).await?`, would require a way to derive
`send_invoice_job` from `send_invoice`. A procedural macro could generate that
identifier. Another option would be to generate a module named `send_invoice`
alongside the function. Rust puts the module and function in separate namespaces,
but the module could collide with an existing type or module.

The `_job` suffix makes the generated name predictable, and that name can
still be taken. If that comes up, the attribute could accept an explicit name,
such as `module = invoice_registration`. Passing the descriptor directly also
works with that override.

Keep the operation name and version explicit. They identify stored executions,
so a function rename should leave the persisted name intact. Use a new version
when the handler can no longer safely interpret data from the older version.

Use the typed helper when handlers need JSON adaptation. Try
`register_jobs!` if setup still repeats the same registration pattern. The
`#[job]` attribute has the most value when registration and enqueueing both
need the same typed descriptor.

See the [standalone worker](../examples/standalone.rs) for current registration
and dispatch, and the [step macro proposal](../../resumable/docs/step-macro-proposal.md)
for workflow body syntax.
