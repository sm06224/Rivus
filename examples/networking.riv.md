---
title: Networking & distributed transport — literate test cases
chunk_size: 4096
needs:
  - net:http:loopback        # (C) capability: reach an http:// loopback source (§0.15)
  - net:tcp:loopback         # (C) capability: dial / subscribe a tcp:// loopback feed
  - net:peer:loopback        # (C) capability: ship an IR to a loopback worker
---

# Networking & distributed transport — literate test cases (§33 / §34)

This `.riv.md` is **living test documentation** for the transport layer. Each
`flow` cell is a real, parseable Rivus flow; the prose states the **scenario**,
the **expected result**, the **capability boundary**, and the **automated test**
that pins it (so a reader can map a case to its `cargo test`). Parsing and
`rivus explain` work in any build; *running* the networked cells needs
`--features net` (or `--features quic`), and a loopback server — the runnable
end-to-end harness is [`examples/networking-demo.sh`](networking-demo.sh).

> Conventions: ports below are placeholders (`:PORT`); the demo/tests bind an
> ephemeral loopback port and substitute it. Capability is **loopback-by-default**
> — a non-loopback host/peer needs an explicit allowlist (shown per case).

---

## Tier C — loopback-exception client fetch (§28.12.5-1)

### Case C1 — `open "http://…"` bounded GET over CSV

**Scenario.** Fetch a remote CSV and wrangle it exactly like a local file — only
the *transport* changes (§28.2 orthogonality). The optimizer still pushes the
filter into the reader.

**Expected.** Two adult rows in country `JP` (`alice,30` and `carol,42`); `bob`
(17) is filtered out. **Chunk-size independent** (§0.5).

**Capability.** `127.0.0.1` is loopback → always allowed. A remote host would
need `RIVUS_CAP_NET_HOSTS=host[:port]`, else a fatal capability denial that names
only the target.

**Pinned by.** `tests/net.rs::http_get_content_length`,
`http_get_chunk_size_independent`.

```flow
Adults:
    open "http://127.0.0.1:PORT/data.csv"
    |? age >= 18, country == "JP"
    |> name age country
;
```

### Case C2 — `open "http://…"` over JSON, chunked + redirects

**Scenario.** The same GET against a JSON body framed with
`Transfer-Encoding: chunked` (and following up to 5 `3xx` redirects). JSON
decodes single-pass (a network body can't be re-read for two-pass inference).

**Expected.** All rows decode across chunk boundaries; result identical to a
local read of the same document. `https://` is **refused** with guidance (TLS is
out of scope, §28.12.5-5).

**Pinned by.** `tests/net.rs::http_get_jsonl`, `http_get_chunked`.

```flow
People:
    open "http://127.0.0.1:PORT/data.jsonl" as json
    |? age >= 18
    |> name age
;
```

### Case C3 — `subscribe "tcp://…"` unbounded client feed

**Scenario.** Dial a TCP producer (client-side; no listener is bound) and stream
newline-delimited records. This is an **unbounded** source (§0.14): the optimizer
and the parallel executor leave it alone; it ends on peer close or `take N`.

**Expected.** Pass-through (filter/project/take) flows live; rows arrive as the
feed sends them. Lossless backpressure (a slow consumer fills the TCP window).

**Capability.** loopback default; `RIVUS_CAP_NET_HOSTS` for a remote feed.

**Pinned by.** `tests/net.rs::subscribe_tcp_stream_until_peer_close`,
`subscribe_jsonl_stream`.

```flow
Live:
    subscribe "tcp://127.0.0.1:PORT"
    |? age >= 18
    |> name age
    take 100
;
```

### Case C4 — unbounded + whole-stream aggregate is refused (never-silent)

**Scenario.** A `|#` group-by needs the whole stream, which an unbounded source
never ends — so the plan is refused **before running** (a window is a later
slice). `take N` is *not* offered as the fix (it bounds the count, not which
rows arrive — arrival order is environmental).

**Expected.** `rivus run` returns a build error mentioning the unbounded source;
parsing / `rivus explain` still succeed (always-std).

**Pinned by.** `tests/net.rs::subscribe_blocking_op_refused`.

```flow
Counts:
    subscribe "tcp://127.0.0.1:PORT"
    |# country count:name
;
```

---

## Tier A — protected-channel distributed execution (§33 / §17, the headline)

The **IR is the deployment artifact** (§28.12.5-4): the coordinator ships a flow
to a worker, which runs it on the same chunk engine and streams the result back —
**byte-identical to a local run** (interpret == distribute, §0.5).

```sh
# Worker (binds the trusted WireGuard interface or loopback; accepts allowlisted peers):
rivus serve --bind 127.0.0.1:PORT            # std (kernel-WireGuard-bound) primary
rivus serve --bind 127.0.0.1:PORT --quic     # QUIC alternative (§28.12.5-3)

# Coordinator (ships the IR, prints the streamed result on stdout, events on stderr):
rivus run examples/networking.riv.md --on rivus://127.0.0.1:PORT
rivus run examples/networking.riv.md --on quic://127.0.0.1:PORT
```

### Case A1 — round-trip is byte-identical (`distribute == interpret`)

**Scenario.** Ship the flow below to a worker; compare the streamed result to a
local `rivus run` of the same flow.

**Expected.** Identical bytes. Crypto/confidentiality is delegated to the kernel
WireGuard interface (§28.12.5-2) — Rivus embeds none; it enforces only the
binding + peer allowlist boundary.

**Capability.** `rivus serve` binds loopback by default; a real deployment sets
`RIVUS_CAP_NET_IFACE=<wg-ip>` (refuse any other bind → no raw listener) and
`RIVUS_CAP_NET_PEERS=<peer-ips>`.

**Pinned by.** `tests/net.rs::distributed_exec_round_trips_byte_identical`,
`distributed_no_raw_public_listener`, `distributed_peer_allowlist_denies_remote`;
QUIC: `tests/quic.rs::quic_protected_channel_round_trip_and_pinning`.

```flow
Adults:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 18, country == "JP"
    |> name age
;
```

### Case A2 — event-centric observability over the telemetry channel (§34)

**Scenario.** The same job, observed. Every frame carries a logical **channel**
(Control / Data / Telemetry) on one connection — the QUIC stream-separation
lesson without N sockets. The worker *narrates* the job on the telemetry channel
instead of the client packet-sniffing.

**Expected.** On stderr (telemetry channel): `flow.started job_bytes=…`,
`flow.completed result_bytes=… ms=…`, `transfer.done frames=… bytes=…`. On stdout
(data channel): the clean result. The two never interleave.

**Pinned by.** `tests/net.rs::distributed_emits_telemetry_events`.

```flow
ByCountry:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 20
    |# country
;
```

### Case A3 — worker error propagates (never-silent)

**Scenario.** Ship a flow the worker cannot parse/run. The worker returns an
error on the control channel; the coordinator surfaces it as a failure (never a
silent empty result).

**Expected.** `rivus run --on …` exits non-zero with the worker's message.

**Pinned by.** `tests/net.rs::distributed_worker_error_propagates`.

```text
this is deliberately not a valid Rivus flow
```

### Case A4 — host Transport Service over a Unix-domain socket (§34.4, pre-impl)

**Scenario.** Co-located Rivus processes share one comms front instead of each
owning a network stack (the PMCN "consolidate responsibility" idea). The worker
fronts a UDS; the coordinator ships the IR over `uds://`. The **same
channel-tagged frames** as the TCP path run over the socket — proving the
protocol is transport-agnostic (§34.1).

**Expected.** Byte-identical result to A1; telemetry events on the telemetry
channel. UDS is local + filesystem-permission-gated, so no IP allowlist applies —
the capability boundary is the socket file's path/permissions.

**Pinned by.** `tests/net.rs::distributed_uds_transport_service_round_trips`.

```sh
rivus serve --uds /run/rivus.sock
rivus run examples/networking.riv.md --on uds:///run/rivus.sock
```

The shipped flow is the same A1 flow.

---

## Tier B — QUIC alternative identity & pinning (§28.12.5-3/4)

### Case B1 — static-public-key identity + fingerprint pinning

**Scenario.** Over QUIC, each side mints a self-signed cert; the identity is its
DER's SHA-256 **fingerprint**. The allowlist pins allowed peer fingerprints
(`RIVUS_CAP_NET_PEER_KEYS`). TLS accepts any cert; the *application* enforces the
pin after the handshake — a boundary, not a secret. Private keys never leave the
process and never touch the IR / telemetry.

**Expected.** A client that pins the wrong worker key is **refused** at the
application layer (message: "not in the pinned allowlist"); the matching key (or
dev accept-any) succeeds and the round-trip is byte-identical.

**Pinned by.** `tests/quic.rs::quic_protected_channel_round_trip_and_pinning`,
`distributed_quic::tests::{pin_rules, fingerprint_is_stable_hex_sha256}`.

This case is exercised through the same flow as A1, over `quic://`:

```flow
Adults:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 18
    |> name age
;
```

### Case B2 — QUIC session reuse: one handshake, many jobs (§34.4 s2', #176)

**Scenario.** A `QuicSession` performs the TLS 1.3 handshake + static-key pin
**once** on `connect`, then runs *many* jobs, each on a fresh QUIC **bidi
stream** (native multiplexing — no cross-job head-of-line blocking). This is the
QUIC counterpart of the std `Session`, and the lever for QUIC's
handshake-dominated per-call cost.

**Expected.** Every job is byte-identical to a local run (the three flows below,
run over one connection, each match `rivus run` of the same flow). **Measured
4.3× faster** than a fresh connection per job (per-call 7.9 → reused 1.8 ms/job);
the cost to budget is the handshake, paid once.

**Pinned by.** `tests/quic.rs::quic_protected_channel_round_trip_and_pinning`
case (c); benched by `transport_bench::bench_quic_distributed_latency`.

```flow
R:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 18
    |> name
;
```

```flow
R:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 40
    |> name
;
```

```flow
R:
    open examples/users.csv (name:str age:i64 country:str)
    |> name
;
```

---

## Tier D — transport CPU budget (§34.3, feature `cpubudget`, #174)

### Case D1 — core affinity is placement, not data (byte-identity holds)

**Scenario.** The §34.0 thesis: on a node where Rivus SIMD saturates the CPU, the
transport's crypto SIMD competes for the same cores. With `cpubudget` the
transport/crypto+I/O threads are pinned to a bounded core set
(`RIVUS_NET_TRANSPORT_CORES=0`) so they can't steal data-plane cycles. Affinity
is an **ops knob, not data** (§0.14) — like the `watch` queue budget.

**Expected.** Running the flow below with the transport pinned produces the
**exact same bytes** as running it unpinned (affinity changes *where* work runs,
never *what* it computes). Separately measured: **1.6–1.7× more data-plane
throughput** under transport-crypto contention on a 4-core box when the transport
is confined off the data cores.

**Capability / availability.** Linux-only syscall path (`sched_setaffinity`)
behind the off-by-default `cpubudget` feature; a no-op `Unsupported` elsewhere.
The pinning is best-effort — a failed pin degrades to "scheduler decides", never
fatal, and never changes the result.

**Pinned by.** `cpu_budget::tests::affinity_does_not_change_output` (byte-identity);
benched by `transport_bench::bench_cpubudget_affinity_protects_data_plane`.

```flow
Adults:
    open examples/users.csv (name:str age:i64 country:str)
    |? age >= 18
    |> name age
;
```

This case is the A1 flow run with `RIVUS_NET_TRANSPORT_CORES` set on the worker:

```sh
RIVUS_NET_TRANSPORT_CORES=0 rivus serve --bind 127.0.0.1:PORT   # cpubudget build
rivus run examples/networking.riv.md --on rivus://127.0.0.1:PORT
# → byte-identical to `rivus run` with no budget set.
```
