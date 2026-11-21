# Durable step macro proposals

These sketches explore syntax for writing durable steps. The current API is
`ExecutionContext::step` and `ExecutionContext::step_with`.

## Forward an existing closure

Proposed:

```rust
let doubled = step!(ctx, "double", |_| async {
    Ok::<_, std::convert::Infallible>(input * 2)
}).await?;
let result = step!(ctx, "increment", |_| async {
    Ok::<_, std::convert::Infallible>(doubled + 1)
}).await?;
```

Current:

```rust
let doubled = ctx.step("double", |_| async {
    Ok::<_, std::convert::Infallible>(input * 2)
}).await?;
let result = ctx.step("increment", |_| async {
    Ok::<_, std::convert::Infallible>(doubled + 1)
}).await?;
```

The forwarding form adds little convenience because callers still write the
closure and async block.

## Wrap a step body

Proposed:

```rust
let doubled = step!(ctx, "double", {
    Ok::<_, std::convert::Infallible>(input * 2)
}).await?;
```

Current:

```rust
let doubled = ctx.step("double", |_| async {
    Ok::<_, std::convert::Infallible>(input * 2)
}).await?;
```

A `macro_rules!` implementation could supply the reusable closure and async
block. The caller can use `.await` inside the body for asynchronous work and
`.await?` after the macro to get the durable result.

The current API expects a reusable `Fn(StepContext) -> Future`. In the arithmetic
example, `async { ... }` can borrow `input` from the surrounding workflow. When
the future uses the closure's own `step` argument, `async move { ... }` lets the
future own that argument after the closure returns. For a captured non-`Copy`
value that each attempt needs to own, clone it inside the closure before
building the future. The explicit API can express that setup.

The persisted name and the await remain visible at the call site. An
effectful step could bind its `StepContext` with
`step!(ctx, "charge-card", |step| { /* body */ })` to access the idempotency key.

### Reuse existing retry options

Proposed:

```rust
let receipt = step!(ctx, "upload", options = upload_retry, |step| {
    upload_snapshot(step).await
}).await?;
```

Current:

```rust
let receipt = ctx.step_with("upload", upload_retry, |step| async move {
    upload_snapshot(step).await
}).await?;
```

Here `upload_retry` is an ordinary `StepOptions` value that can be shared across
workflows. The macro forwards it to the existing API. If
`upload_snapshot` already takes a `StepContext` and returns the expected future,
the current API can take the function directly:

```rust
let receipt = ctx.step_with("upload", upload_retry, upload_snapshot).await?;
```

## Mark a step with an attribute

The original idea puts the step name above a local binding:

```rust
#[step(double)]
let doubled = async move { Ok::<_, std::convert::Infallible>(input * 2) }.await;
```

If an outer macro owns this syntax, it could accept a plain block and generate
the closure, async block, and durable call itself. The preferred `step!` form
above supplies the same wrapper at the call site with less rewriting.

The intended equivalent is the current `ctx.step("double", …).await?` call.
Stable Rust supports procedural attribute macros on items, which rules out
applying a standalone custom attribute macro to this `let` statement. An outer
`#[workflow]` function attribute could consume the inner marker and rewrite the
binding before the function body is compiled. That would require rewriting the
workflow body and deciding how the attribute identifies its execution context.
[Rust Reference](https://doc.rust-lang.org/reference/procedural-macros.html#attribute-macros)

Keep step names explicit and stable across deployments. A completed step can
return its stored result on resume, so changing its persisted name can change
which work runs. Keep the engine's existing step and retry behavior in every
form.

Try the block form first on [backup_snapshot](../examples/backup_snapshot.rs).
The [PostgreSQL walkthrough](postgres-worker.md) shows the current two-step
syntax and demonstrates result reuse after a worker restart.
