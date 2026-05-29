# Rivus

**Rivus** is a flow-oriented, DAG-native, continue-first, observable-first
stream runtime — an attempt to take PowerShell's "everything is a pipeline"
philosophy and rebuild it on a chunk-based, columnar, query-planned execution
graph with Rust/C-class performance ambitions.

> Rivus is not a shell, not a query engine, and not a language — it is all
> three, unified. Source ⇄ DAG IR ⇄ optimized IR ⇄ source is reversible;
> execution is observable; errors continue rather than crash.

## Core principles ("physical laws")

1. **Everything is Flow** — function / filter / scriptblock unified as Scope + Flow
2. **Continue First** — errors are events on a side-channel stream, not stack unwinds
3. **DAG Native** — even a linear pipe is a degenerate DAG
4. **Observable First** — telemetry is core, the runtime is always visualizable
5. **IR Reversible** — the graph regenerates readable source
6. **Chunk Native** — columnar chunks, not items, are the unit of execution
7. **Execution-aware typing** — a type selects an *execution lane*, not a memory layout
8. **Text is stream** — strings are decode-continuations, malformed input does not stop the flow

## The Unified Flow Syntax

```rivus
# Tee one source into two filtered flows, then merge them.
Users:
    open examples/users.csv
    -> Adults: |? age >= 20 ;
    -> Minors: |? age <  20 ;
;

Merged:
    Adults + Minors
;
```

| operator | meaning |
|---|---|
| `\|?` filter · `\|>` map/project · `\|#` group | transforms |
| `->` branch (tee) · `+` merge · `&` join | DAG fan-out / fan-in |
| `on error ... : transition degraded ;` | continue-first lifecycle hook |

## Quick start

```sh
cargo test                                          # 11 tests
cargo run -p rivus-cli -- run     examples/branch.riv
cargo run -p rivus-cli -- run     examples/recover.riv   # escalates to mode: degraded
cargo run -p rivus-cli -- explain examples/branch.riv    # DAG IR + regenerated source
```

`rivus run` prints the live execution graph, the error stream, and captured outputs:

```
▒ execution graph   final mode: normal
  Users                    open        0→8     ██████████████ done
    └─ Minors              filter      8→4     ███████░░░░░░░ done
    └─ Adults              filter      8→4     ███████░░░░░░░ done
      └─ Merged            merge       8→8     ██████████████ done
```

## Workspace layout

```
crates/
  rivus-core      Chunk / Column / Schema / Value / Mode / ErrorEvent
  rivus-ir        PlanGraph (DAG) / Op / Expr / to_source()  (reversible)
  rivus-parser    Unified Flow Syntax → DAG IR
  rivus-runtime   single-thread chunk execution engine / operators / telemetry
  rivus-cli       `rivus run | explain | check`  (ASCII visualization)
examples/         *.riv programs + users.csv
docs/design/      full 17-section design (architecture → distributed)
```

## Design

The complete design — architecture, execution model, chunk model, IR,
scheduler, type system, memory model, optimization, JIT, syntax, runtime API,
plugin ABI, error model, observability, benchmarks, MVP scope, and the future
distributed architecture — lives in [`docs/design/`](docs/design/README.md),
staged as **MVP → optimization → JIT → distributed**.

## Status

MVP (Phase 0) is implemented and runnable: the headline goal — *a working DAG
flow and its visualization* — is met. See
[`docs/design/16-mvp-scope.md`](docs/design/16-mvp-scope.md) for exactly what
is implemented vs. designed-but-pending.

## License

Apache-2.0 (see [LICENSE](LICENSE)).
