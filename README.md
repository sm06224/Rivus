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
| `\|> name (age * 12) as months` | computed columns (`+ - * / %`, `as` alias) |
| `take N` (`limit`/`head`) | cap the stream at N rows |
| `sort KEY [asc\|desc]` | stable order by one key (blocking) |
| `distinct [KEY ...]` | drop duplicate rows (whole-row or by key) |
| `->` branch (tee) · `+` merge · `&` join | DAG fan-out / fan-in |
| `on error ... : transition degraded ;` | continue-first lifecycle hook |

## Installation

Rivus ships **pre-built binaries for macOS (Apple Silicon) and Windows 11+
(x64)**. Every other platform — Linux, Intel Mac, ARM Windows — builds from
source, which is a single command because the runtime has **zero third-party
dependencies**.

Each platform offers two flavors so you can trade compatibility for speed:

| flavor | meaning | pick it when |
|---|---|---|
| **portable** (multi-arch) | baseline codegen that runs on the widest range of CPUs in that family | you want it to *just run* anywhere (Intel included) |
| **tuned** (CPU-specialized) | `-C target-cpu=…` build, faster but needs that CPU generation or newer | you know your CPU and want the extra speed |

### Available downloads

| OS | flavor | asset | runs on |
|---|---|---|---|
| macOS | portable | `rivus-<version>-macos-arm64.tar.gz` | any Apple Silicon (M1 and later) |
| macOS | tuned | `rivus-<version>-macos-arm64-appleM-tuned.tar.gz` | all Apple Silicon (M-series tuned) |
| Windows 11+ | portable | `rivus-<version>-windows-x64-portable.zip` | any x64 — **Intel** and older AMD |
| Windows 11+ | AMD tuned | `rivus-<version>-windows-x64-amd-zen4.zip` | AMD Zen 4 (Ryzen 7000 / EPYC 9004) or newer |
| Windows 11+ | AMD tuned | `rivus-<version>-windows-x64-amd-zen3.zip` | AMD Zen 3 (Ryzen 5000 / EPYC 7003) or newer |

> On an **Intel** PC, use the Windows **portable** zip — the AMD-tuned builds
> will fault with an illegal-instruction error. On any Mac, you need **Apple
> Silicon**; Intel Macs are not supported (build from source instead).

### Option A — download a pre-built binary

1. Grab the asset for your OS/CPU from the
   [**Releases**](https://github.com/sm06224/rivus/releases) page (see the table
   above), plus its matching `.sha256`.
2. Verify the checksum (optional but recommended):
   - macOS: `shasum -a 256 -c rivus-<version>-macos-arm64.tar.gz.sha256`
   - Windows (PowerShell): `Get-FileHash .\rivus-<version>-windows-x64-portable.zip -Algorithm SHA256`
3. Extract and put `rivus` on your `PATH`:

   **macOS** (Terminal):
   ```sh
   tar -xzf rivus-<version>-macos-arm64.tar.gz
   cd rivus-<version>-macos-arm64
   # macOS quarantines downloads; clear it so Gatekeeper lets it run:
   xattr -d com.apple.quarantine ./rivus 2>/dev/null || true
   sudo mv ./rivus /usr/local/bin/        # or any dir on your PATH
   rivus --help
   ```

   **Windows** (PowerShell):
   ```powershell
   Expand-Archive .\rivus-<version>-windows-x64-portable.zip -DestinationPath .\rivus
   # Add the folder to PATH for this session (use the GUI for a permanent one):
   $env:Path = "$PWD\rivus;$env:Path"
   rivus --help
   ```

### Option B — build from source (any platform)

You need a Rust toolchain (`rustup` from <https://rustup.rs>). Then:

```sh
git clone https://github.com/sm06224/rivus
cd rivus
cargo build --release                  # binary at target/release/rivus
./target/release/rivus run examples/branch.riv
```

Or install it straight onto your `PATH` with Cargo:

```sh
cargo install --path crates/rivus-cli  # provides the `rivus` command
```

> Building macOS/Windows x64 packages yourself? `dist/build.sh` produces the
> same archive layout as the official releases — see [`dist/`](dist/README.md).

## Quick start

```sh
cargo test                                          # 11 tests
cargo run -p rivus-cli -- run     examples/branch.riv
cargo run -p rivus-cli -- run     examples/recover.riv   # escalates to mode: degraded
cargo run -p rivus-cli -- explain examples/branch.riv    # DAG IR + regenerated source
```

> Already installed via Option A/B? Drop the `cargo run -p rivus-cli --` and
> just call `rivus run examples/branch.riv`.

### Run a flow without a file

A program can be passed inline with `-c`, or piped on stdin (heredoc) — handy
for one-liners and for embedding Rivus in another shell:

```sh
# inline string
rivus run -c 'U: open users.csv |? age >= 20 |> name age save stdout as csv ;'

# heredoc on stdin (the `-` reads the program from stdin)
rivus run - <<'RIV'
Adults:
    open users.csv
    |? age >= 20
    |> name age
    save stdout as jsonl
;
RIV
```

The execution-graph visualization is written to **stderr**, so a `save stdout`
sink leaves **stdout** as clean data you can pipe onward
(`rivus run -c '…' | jq .`). The same `-c` / `-` input works for `explain` and
`check` too.

### Large files stream (bounded memory)

CSV sources and sinks are **streaming**: a file is read in chunks and written as
results flow, so memory stays flat regardless of size. A **1.1 GB** CSV through
`open |? … |> … save out.csv` runs in **~10 MiB** of resident memory (it does
*not* load the file). On an interactive terminal a live progress line shows on
stderr:

```
  … 47,534,080 rows  15.3s  3,114,112 rows/s
  ✓ 48,000,000 rows in 15.4s  (3,123,164 rows/s)
```

(stdin is the exception — it can't be re-read, so a piped-in CSV is buffered.)

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
  rivus-optimizer semantics-preserving DAG transformations (IR-in / IR-out)
  rivus-runtime   single-thread chunk execution engine / operators / telemetry
  rivus-cli       `rivus run | explain | check`  (ASCII visualization)
examples/         *.riv programs + users.csv
docs/GUIDE.md     syntax & usage reference (start here)
docs/design/      full design (architecture → distributed)
```

## Guide

[**`docs/GUIDE.md`**](docs/GUIDE.md) is the full syntax & usage reference: every
source, transform and sink, the expression language, computed columns, a
one-liner cookbook (`-c` / stdin / pipes), and the complete grammar.

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

Licensed under the **Apache License 2.0** (see [LICENSE](LICENSE) and
[NOTICE](NOTICE)).

Use it freely — commercial use, modification, and redistribution are all
permitted, and the license includes an explicit patent grant. It is provided
**as-is, without warranty or liability** (LICENSE §7–8). Copyright is held by
the human author (sm06224); AI tooling assisted development and is credited in
NOTICE for transparency, not as a copyright holder.
