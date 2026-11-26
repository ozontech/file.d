# Adding transform functions

Built-in functions share the registry, parameter validation, and ordinary
`core.CallExpr` nodes. Register each function once in `registry.go`; adding a
function must not require a name-specific parser or compiler branch.

## Choose the execution contract

| Interface | Implementation | Examples |
| --- | --- | --- |
| `Function` | Receives evaluated values; uses the existing `CallPlan` | `trim`, `lookup`, `to_string` |
| `SpecializedFunction` | Prepares a dedicated `core.CallExecutor` from validated arguments | `exists`, `rename` |

Both interfaces supply `Name()` and `Params()`. A registered implementation
provides exactly one of them. Ordinary functions keep `Call`, `CallIndexed`,
and optional `CallIndexedWithState`; their argument storage is unchanged.

Use a specialized executor when an operation needs expressions such as event
paths. It implements `EvalCall`, without becoming a new AST node. For example,
`existsCall` resolves its one path and calls the current target's `Exists`;
`renameCall` resolves two paths and a bool and calls `Rename`. They use local
variables, with no mixed argument array or additional call-state storage.

## Preparation and execution rules

- `Parameter.Mode` defaults to `ValueArgument`. `PathArgument` requires a
  `PathExpr`; `EventFieldOnly` also rejects root and metadata paths. These are
  preparation-time constraints. Path parameters are required positionals with
  no defaults or value kinds.
- A nil parameter default declares a required positional argument. A non-nil
  default declares an optional, keyword-only argument. Declare positionals first.
- The compiler validates the call and its children before invoking `PrepareCall`.
  The executor binds expressions and handles runtime value checks and defaults
  according to its descriptor. `core.PathExpr.ResolvePath` evaluates indices
  without reading the value at the resulting path.
- Pure constants can be captured during preparation. `rename` stores a folded
  bool `override` directly, avoiding its repeated evaluation on each event.
- Preserve source-order argument evaluation, including named arguments before
  or between positional arguments. Capture values before later expressions can
  mutate their JSON nodes. `rename` only needs a bool or an invalid kind, so it
  snapshots that information without materializing a collection.
- An argument evaluation error stops the call immediately. Value-kind checks
  happen after argument evaluation; a later evaluation error takes precedence.
  Keep source positions and wrapped errors, including `core.AbortError`.
- Published executors and expressions are immutable and shared between workers.
  Never retain a target, context, event value, or per-call path in the executor.
  Obtain the target from each execution context. Writes use `Target` and its
  ownership/copying rules. New target capabilities need matching `RootTarget`
  and `MapTarget` implementations.

Unprepared specialized calls use `runtime.Context.PlanCall`: it validates against
that context's registry and creates an ephemeral executor without modifying the
shared AST. Ordinary unprepared calls continue through `EvalContext.CallFunc`.
Custom contexts supporting unprepared specialized calls implement
`core.CallPlanner`. `CompiledFunc.Call` and `Resolve` reject specialized functions;
Go callers can use a prepared expression or the target operation directly.

## Tests

Add coverage to `TestRegisteredFunctionsPrepareCalls` or
`TestRegisteredSpecializedFunctionsPrepareCalls`; together they cover the entire
registry. Check results, defaults, invalid arguments, evaluation order and nested
calls. Event operations also need both targets, prepared/unprepared execution,
mutation failures, retained results and parallel execution of a shared program.
Compare relevant `Plugin.Do` benchmarks before accepting a specialization.
