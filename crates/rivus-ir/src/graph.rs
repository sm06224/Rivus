//! The DAG IR.
//!
//! Rivus is DAG-native (Master principle #3): even a "linear" pipeline is a
//! degenerate DAG. Nodes are flow scopes / transforms / events; edges are
//! streams (or error side-channels). The graph is the single source of truth
//! that the optimizer rewrites and that [`PlanGraph::to_source`] regenerates
//! back into readable Rivus source (Master principle #5: IR reversibility).

use crate::expr::{Access, CmpOp, Expr};
use rivus_core::{DataType, Mode, Severity, Value};
use std::collections::HashMap;
use std::fmt::Write as _;

pub type NodeId = usize;

/// Provenance of a node spliced by `| name k=v …` (§25.3/§25.4): the apply
/// `(site_id, flow_name, value-hole bindings)`. Nodes from the same apply share
/// the `site_id` so `to_source` collapses them back to one `| name k=v …`.
pub type ApplySite = (u32, String, Vec<(String, Value)>);

/// Byte order for binary records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endian {
    Little,
    Big,
}

/// Which rows a join keeps. `Inner` emits only matched pairs; `Left` keeps
/// every left row, padding the right columns with defaults when unmatched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinKind {
    /// The `&`-operator spelling used in source (`&`, `&left`, `&right`, `&full`).
    pub fn amp(&self) -> &'static str {
        match self {
            JoinKind::Inner => "&",
            JoinKind::Left => "&left",
            JoinKind::Right => "&right",
            JoinKind::Full => "&full",
        }
    }
    /// Keep left rows that matched nothing (left / full outer).
    pub fn keeps_left(&self) -> bool {
        matches!(self, JoinKind::Left | JoinKind::Full)
    }
    /// Keep right rows that matched nothing (right / full outer).
    pub fn keeps_right(&self) -> bool {
        matches!(self, JoinKind::Right | JoinKind::Full)
    }
}

/// How `fill col …` replaces a column's missing (empty) cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillMethod {
    /// Substitute a constant value (the column becomes text).
    Value(String),
    /// Forward-fill: carry the last non-empty value forward over blanks.
    Ffill,
    /// Backward-fill: carry the next non-empty value backward over blanks.
    Bfill,
    /// Fill blanks with the mean of the column's non-empty numeric cells.
    /// Buffers the whole stream (a pipeline-breaker like `sort`).
    Mean,
    /// Fill blanks with the median (p50, linear-interpolated) of the column's
    /// non-empty numeric cells. Buffers the whole stream (pipeline-breaker).
    Median,
}

/// What a `|!` validator does with a row that fails its contract (#83, §24.2).
/// Every disposition surfaces the failure on the error stream (never silent);
/// they differ only in what happens to the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Keep the row, surface the violation (`Recoverable`).
    Warn,
    /// Drop the row, surface the violation (`Recoverable`).
    Reject,
    /// Halt the run (`Fatal`, mode = Halted) on the first violation (strict).
    Halt,
}

impl Disposition {
    pub fn parse(s: &str) -> Option<Disposition> {
        Some(match s {
            "warn" => Disposition::Warn,
            "reject" => Disposition::Reject,
            "halt" => Disposition::Halt,
            _ => return None,
        })
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Disposition::Warn => "warn",
            Disposition::Reject => "reject",
            Disposition::Halt => "halt",
        }
    }
}

/// Aggregate functions for `|# key agg:col` (count is always emitted implicitly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Sum,
    Avg,
    Min,
    Max,
    /// Sample standard deviation (ddof=1; `0` for fewer than two values).
    Std,
    /// `count:col` — the number of **non-null** values of a column (SQL
    /// `COUNT(col)`), as opposed to the always-emitted implicit `count` which is
    /// `COUNT(*)` = the group's row count (design 26 §26.2d).
    Count,
    /// Number of distinct **non-null** values (`nunique` is an accepted alias).
    CountDistinct,
    /// First **non-null** value seen in the group (source order).
    First,
    /// Last **non-null** value seen in the group (source order).
    Last,
    /// Percentile of the numeric values in the group (linear interpolation,
    /// like numpy/pandas default). The `u8` is the percentile in 0..=100;
    /// `median` is p50. These buffer every numeric value per group, so — like
    /// `sort`/`join` — they are pipeline-breakers bounded by group cardinality.
    Pct(u8),
}

impl AggFunc {
    pub fn parse(s: &str) -> Option<AggFunc> {
        Some(match s {
            "sum" => AggFunc::Sum,
            "avg" => AggFunc::Avg,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            "std" => AggFunc::Std,
            "count" => AggFunc::Count,
            "count_distinct" | "nunique" => AggFunc::CountDistinct,
            "first" => AggFunc::First,
            "last" => AggFunc::Last,
            "median" => AggFunc::Pct(50),
            // `pN` / `pNN` percentile, N in 0..=100 (e.g. `p50`, `p90`, `p99`).
            other => {
                let n = other.strip_prefix('p')?;
                let pct: u8 = n.parse().ok()?;
                if pct > 100 {
                    return None;
                }
                AggFunc::Pct(pct)
            }
        })
    }

    /// A heap-allocated label (most variants are static; `Pct` is `pNN`, and
    /// p50 renders as `median` to round-trip the `median` alias).
    pub fn label(&self) -> String {
        match self {
            AggFunc::Pct(50) => "median".to_string(),
            AggFunc::Pct(n) => format!("p{n}"),
            other => other.as_str().to_string(),
        }
    }

    /// Static name for the non-percentile variants (used in column headers and
    /// `to_source`). Percentiles have no static name — use [`AggFunc::label`].
    pub fn as_str(&self) -> &'static str {
        match self {
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Std => "std",
            AggFunc::Count => "count",
            AggFunc::CountDistinct => "count_distinct",
            AggFunc::First => "first",
            AggFunc::Last => "last",
            AggFunc::Pct(_) => "pct",
        }
    }
}

/// A fixed-width field type for binary (C-struct-dump) records. Integer widths
/// all ride the `i64` execution lane; floats ride `f64`; `bool` is one byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinType {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Bool,
}

impl BinType {
    pub fn parse(s: &str) -> Option<BinType> {
        Some(match s {
            "i8" => BinType::I8,
            "i16" => BinType::I16,
            "i32" => BinType::I32,
            "i64" => BinType::I64,
            "u8" => BinType::U8,
            "u16" => BinType::U16,
            "u32" => BinType::U32,
            "u64" => BinType::U64,
            "f32" => BinType::F32,
            "f64" => BinType::F64,
            "bool" => BinType::Bool,
            _ => return None,
        })
    }

    /// Width in bytes (packed; no padding — the layout is explicit).
    pub fn size(&self) -> usize {
        match self {
            BinType::I8 | BinType::U8 | BinType::Bool => 1,
            BinType::I16 | BinType::U16 => 2,
            BinType::I32 | BinType::U32 | BinType::F32 => 4,
            BinType::I64 | BinType::U64 | BinType::F64 => 8,
        }
    }

    /// Natural alignment in bytes (for C `repr(C)` layout). For these
    /// primitives alignment equals size.
    pub fn align(&self) -> usize {
        self.size()
    }

    /// Which columnar execution lane this decodes into.
    pub fn lane(&self) -> DataType {
        match self {
            BinType::Bool => DataType::Bool,
            BinType::F32 | BinType::F64 => DataType::F64,
            _ => DataType::I64,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BinType::I8 => "i8",
            BinType::I16 => "i16",
            BinType::I32 => "i32",
            BinType::I64 => "i64",
            BinType::U8 => "u8",
            BinType::U16 => "u16",
            BinType::U32 => "u32",
            BinType::U64 => "u64",
            BinType::F32 => "f32",
            BinType::F64 => "f64",
            BinType::Bool => "bool",
        }
    }
}

/// Source provenance mode (design §28.6): does a source attach its origin
/// [`rivus_core::Resource`] to each chunk it produces?
///
/// `Off` by default (zero overhead). `Source` (`with source`) rides the handle
/// on chunk metadata, reachable via the `source.uri` accessor. `Filename`
/// (`with filename`) is the sugar alias that additionally materializes a
/// `filename` column (= `source.uri`). Only the uri is the in-contract identity
/// (§00 0.14), so provenance stays byte-identical across serial/parallel reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Provenance {
    #[default]
    Off,
    Source,
    Filename,
}

impl Provenance {
    /// The trailing source modifier this renders to (`to_source`), or `""`.
    pub fn modifier(self) -> &'static str {
        match self {
            Provenance::Off => "",
            Provenance::Source => " with source",
            Provenance::Filename => " with filename",
        }
    }

    /// The origin handle a source attaches to each chunk under this mode, or
    /// `None` when provenance is off (`Off` → zero overhead). Both `source` and
    /// `filename` ride the handle (`filename` additionally materializes a column,
    /// slice 2-②b). Only the uri is in-contract (§00 0.14), and every reader —
    /// the serial path and each byte-range parallel worker — derives the same
    /// handle from the same path, so provenance is byte-identical (serial ==
    /// parallel, partition-independent).
    pub fn source(self, path: &str) -> Option<rivus_core::Resource> {
        match self {
            Provenance::Off => None,
            Provenance::Source | Provenance::Filename => Some(rivus_core::Resource::new(path)),
        }
    }

    /// Does this mode materialize a `filename` column (= `source.uri`) at the end
    /// of each chunk? Only `with filename` does (slice 2-②b); `with source`
    /// rides the handle on metadata only. The column is suffixed `filename_r` on
    /// collision with an existing column (§27.1, the join rule).
    pub fn materializes_filename(self) -> bool {
        matches!(self, Provenance::Filename)
    }
}

/// **Discovery** (design §28.2): which resource(s) a source reads. The v1 form
/// is a single fixed path (`open PATH` / `readbin PATH`); slice 3 adds `ls` /
/// `glob` / recursive discovery as further variants. Keeping it a layer (not a
/// bare `path: String` on the source) is what lets discovery become a flow
/// without re-shaping `Op`.
#[derive(Debug, Clone)]
pub enum Discovery {
    /// A single fixed resource path — the v1 `open` / `readbin` source.
    Fixed(String),
    /// A glob pattern enumerated into a **stream of resources** (`ls "logs/**/*.csv"`,
    /// design §28.3): `**` recurses, `*`/`?`/`[...]` match within a path segment.
    /// std-only (no deps); resources are emitted in deterministic uri-ascending
    /// order. Paired with `Codec::Discover` it lists files as a `Resource` table.
    Glob(String),
}

impl Discovery {
    /// The discovery's path/pattern string: the fixed path (`Fixed`) or the glob
    /// pattern (`Glob`). Used for `to_source` and the parallel-read size gate
    /// (`Glob` never plans a byte-range read — its codec is `Discover`).
    pub fn path(&self) -> &str {
        match self {
            Discovery::Fixed(p) | Discovery::Glob(p) => p,
        }
    }
}

/// **Transport** (design §28.2): how a resource's bytes are obtained. Today this
/// is always the local family, selected at read time from the path scheme (a
/// plain file / stdin `-` / a compressed `.gz`/`.zst` stream), so the single
/// `Local` variant reserves the orthogonal slot; slice 5 adds feature-gated
/// `Http` / `Socket` without re-shaping `Op`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Transport {
    #[default]
    Local,
}

/// **Codec** (design §28.2): how bytes decode into chunks — the format, plus its
/// format-specific configuration. The optimizer's reader pushdowns
/// (`projection` / `prefilter` / `str_prefilter`) ride on the CSV codec because
/// only the CSV reader implements them.
#[derive(Debug, Clone)]
pub enum Codec {
    /// Delimited text (CSV/TSV). `delim` is the field byte (`b','` / `b'\t'`).
    /// `header` / `declared` / `dt_formats` are read config; `projection` /
    /// `prefilter` / `str_prefilter` are optimizer-set reader pushdowns (see the
    /// `Op::Source` doc and the optimizer module).
    Csv {
        header: bool,
        declared: Option<Vec<(String, Option<DataType>)>>,
        dt_formats: Vec<(String, String)>,
        delim: u8,
        projection: Option<Vec<String>>,
        prefilter: Vec<(String, CmpOp, f64)>,
        str_prefilter: Vec<String>,
    },
    /// JSON Lines (one flat JSON object per line) or a top-level JSON array.
    Jsonl,
    /// Fixed-width binary records (a C-struct dump). `endian` selects byte order;
    /// `c_align` true uses C `repr(C)` natural-alignment padding, false packs.
    Binary {
        fields: Vec<(String, BinType)>,
        endian: Endian,
        c_align: bool,
    },
    /// **Discovery codec** (design §28.3): no bytes are decoded — the discovered
    /// resources are emitted directly as ordinary file columns (`path`/`name`/
    /// `size`/`mtime`). This is the `ls` source (`Discovery::Glob` +
    /// `Codec::Discover`). `name_prefilter` carries required filename substrings
    /// pushed down by the optimizer (`discovery_prefilter`): the enumeration walk
    /// skips any entry whose name lacks one **before statting it** (a superset
    /// pre-scan — the downstream filter stays authoritative, so results are
    /// unchanged). Slice 3c's `read` consumes the resource stream to decode.
    Discover { name_prefilter: Vec<String> },
}

impl Codec {
    /// A CSV/TSV codec with default read config and no optimizer pushdowns set
    /// (the parser's fresh source; `delim` picks CSV `b','` vs TSV `b'\t'`).
    pub fn csv(delim: u8) -> Codec {
        Codec::Csv {
            header: true,
            declared: None,
            dt_formats: Vec::new(),
            delim,
            projection: None,
            prefilter: Vec::new(),
            str_prefilter: Vec::new(),
        }
    }

    /// A fresh discovery codec (`ls`); no name pre-filter pushed yet.
    pub fn discover() -> Codec {
        Codec::Discover {
            name_prefilter: Vec::new(),
        }
    }
}

/// Format selector for the multi-file `read` (design §28.3, slice 3c). `None`
/// (no `as FMT`) infers per file from its extension; otherwise every file is
/// forced to this format. `Tsv` is CSV with a tab delimiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFmt {
    Csv,
    Tsv,
    Jsonl,
}

impl ReadFmt {
    /// The `as …` word for `to_source` (and the `read as …` parser).
    pub fn as_str(self) -> &'static str {
        match self {
            ReadFmt::Csv => "csv",
            ReadFmt::Tsv => "tsv",
            ReadFmt::Jsonl => "jsonl",
        }
    }
}

/// A flow operator. One enum spanning sources, transforms, fan-out/in and
/// sinks — because in Rivus they are all just nodes in the same graph.
#[derive(Debug, Clone)]
pub enum Op {
    /// A **source** (design §28.2/§28.8): read a resource (`discovery`) over a
    /// `transport`, decode it with a `codec`, optionally attaching `provenance`.
    /// One composable node replacing the former format-specific `OpenCsv` /
    /// `OpenJsonl` / `OpenBinary`, so discovery (slice 3) and routing (slice 4)
    /// attach here without re-stratifying I/O by format. The v1 surface forms —
    /// `open PATH [as FMT] (schema) [with …]`, `readcsv`/`readjson`/`readbin` —
    /// desugar to this (`Discovery::Fixed` + `Transport::Local` + the matching
    /// `Codec`), and `to_source` restores the original surface form (reversible).
    ///
    /// Reader pushdowns set by the optimizer ride on `Codec::Csv`: `projection`
    /// (`project_pushdown`) restricts which columns the reader builds; `prefilter`
    /// (`filter_pushdown`) lets it skip *building* rows whose numeric conjunction
    /// is definitely false; `str_prefilter` carries required literal substrings
    /// (a ripgrep-style raw-line pre-scan). All are conservative supersets — the
    /// downstream `FilterProject` stays authoritative — so results are unchanged.
    Source {
        discovery: Discovery,
        transport: Transport,
        codec: Codec,
        provenance: Provenance,
    },
    /// `read [as FMT] [with source|filename]` — discovery-as-flow's reader stage
    /// (design §28.3, slice 3c): consume a **`Resource` column** from upstream
    /// (default `path`, else the first `Resource`-typed column) and open+decode
    /// every handle, concatenating the files **by name** (union-by-name) in
    /// deterministic uri order. `fmt` forces a format for every file; `None`
    /// infers per file from its extension. `provenance` attaches each file's
    /// handle to its rows (so `source.uri` / `filename` work per row). The
    /// supplier is source-agnostic: `ls`, a manifest (`resource(col)`), or a
    /// computed path all feed it (§28.3).
    Read {
        fmt: Option<ReadFmt>,
        provenance: Provenance,
    },
    /// `stream X` — replay of a named flow (and, internally, a reference edge).
    StreamRef { name: String },
    /// `|? <pred>`
    Filter { pred: Expr },
    /// `|! <pred> warn|reject|halt` — declare a row contract: a row where `pred`
    /// is false is non-conforming and disposed of per `disposition`, always
    /// surfaced on the error stream (never silent). Stateless (row-wise). #83 §24.
    Validate {
        pred: Expr,
        disposition: Disposition,
    },
    /// `|> field [field ...]` — pure column selection.
    Project { fields: Vec<String> },
    /// `|> field (expr) as alias ...` — projection with computed columns. Each
    /// item is `(expr, output_name)`; a bare field is `(Field, name)`. Emitted
    /// only when at least one item is computed (pure selection stays `Project`),
    /// so existing fusion/pushdown are unaffected. Stateless (row-wise).
    ProjectExpr { items: Vec<(Expr, String)> },
    /// `take N` / `limit N` / `head N` — pass through at most `N` rows of the
    /// stream flowing through this node, then drop the rest. Stateful (a global
    /// running count), so it is a pipeline-breaker for the parallel executor.
    Take { n: usize },
    /// `sort KEY [asc|desc]` — order the whole stream by one key column. A
    /// blocking operator (buffers every row, emits on finish); the sort is
    /// stable, so equal keys keep source order and the result is chunk-size
    /// independent. Pipeline-breaker for the parallel executor.
    /// `sort KEY [asc|desc] [KEY [asc|desc] ...]` — order the whole stream by
    /// one or more keys, each with its own direction (default ascending).
    /// Blocking (buffers all rows) → serial path.
    Sort { keys: Vec<(String, bool)> },
    /// `distinct [KEY ...]` — drop duplicate rows, keeping the first occurrence.
    /// With no keys, the whole row is the dedup key; otherwise only the named
    /// columns. Streaming (emits as it goes) but stateful (a global seen-set),
    /// so it runs on the serial path. Output order = first-occurrence order.
    Distinct { keys: Vec<String> },
    /// `describe` — replace the stream with a one-row-per-column summary
    /// (column, type, count, min, max, mean). A streaming, single-pass
    /// accumulator that emits on finish; stateful → serial path.
    Describe,
    /// `dropna [col ...]` — drop rows with a missing (empty) value in any of the
    /// named columns (or any column when none named). Streaming, stateless.
    DropNa { cols: Vec<String> },
    /// `fill col VALUE|ffill|bfill` — replace missing (empty) cells of `col`.
    /// `VALUE` substitutes a constant (the column becomes text); `ffill` carries
    /// the last non-empty value forward, `bfill` the next non-empty value back.
    /// A constant fill is streaming/stateless; `ffill`/`bfill` are stateful
    /// (they carry state across rows and chunks) → serial path.
    Fill { col: String, method: FillMethod },
    /// `rename OLD NEW [OLD NEW ...]` — rename columns in place, preserving
    /// position, type and values. Unknown `OLD` names are skipped with a warning.
    /// Streaming, stateless.
    Rename { pairs: Vec<(String, String)> },
    /// `drop COL [COL ...]` — remove the named columns, keeping the rest in
    /// order. Unknown names are ignored. Streaming, stateless. (Sugar over
    /// projection, but resolved against the live schema since `drop` names the
    /// columns to remove rather than the ones to keep.)
    Drop { cols: Vec<String> },
    /// `cast COL:type [COL:type ...]` — change the type of named columns in
    /// place (position and name kept; values re-coerced via the cast lane).
    /// Sugar for a computed `(col:type) as col` projection that keeps the rest.
    /// Unknown names are skipped with a warning. Streaming, stateless.
    Cast { casts: Vec<(String, DataType)> },
    /// `reorder COL [COL ...]` — move the named columns to the front in the
    /// given order; all other columns follow in their original order. Unknown
    /// names are ignored. Streaming, stateless, type/value preserving.
    Reorder { cols: Vec<String> },
    /// `|# key [key ...] [agg:col ...]` — group by one or more keys. Always
    /// emits a `count`; each `(func, col)` adds an aggregate column (e.g.
    /// `sum:score`, `avg:age`). Each key becomes a column in the output.
    GroupBy {
        keys: Vec<String>,
        aggs: Vec<(AggFunc, String)>,
    },
    /// Fused linear chain of filters and an optional trailing projection,
    /// produced by the optimizer (`fuse_linear`). All `preds` must pass (AND);
    /// when `fields` is `Some`, only those columns are materialized — gathering
    /// the projected columns once instead of filter-then-project's two passes.
    FilterProject {
        preds: Vec<Expr>,
        fields: Option<Vec<String>>,
    },
    /// `->` fan-out (tee): forwards each chunk to every outgoing edge.
    Branch,
    /// `+` merge: union of all incoming streams.
    Merge,
    /// `&` synchronized join on one or more key pairs. `kind` selects inner
    /// (`&`) vs left/right/full outer. `left_keys[i]` joins `right_keys[i]`; the
    /// two vectors have equal length (≥1). An outer join keeps unmatched rows on
    /// the kept side, filling the other side's columns with type defaults.
    Join {
        left_keys: Vec<String>,
        right_keys: Vec<String>,
        kind: JoinKind,
    },
    /// `print` / default leaf sink.
    SinkPrint,
    /// `save path.csv` — `delim` selects the field separator (`b','` for CSV,
    /// `b'\t'` for a `.tsv`/`.tab` path or `save out.x as tsv`).
    SinkCsv { path: String, delim: u8 },
    /// `save path.jsonl` — write JSON Lines (one object per row).
    SinkJsonl { path: String },
    /// `save path.json` — write a single JSON array (`[{…},{…}]`). Unlike
    /// `SinkJsonl` (one object per line, streaming), this brackets the whole
    /// result; still written incrementally (open bracket, comma-separated rows,
    /// close bracket) so it stays bounded-memory.
    SinkJson { path: String },
}

/// The default CSV field delimiter.
pub const COMMA: u8 = b',';

/// Render a join's `on` clause faithfully for `to_source`: one token per key
/// pair, `lk` when the two names are equal else `lk:rk`, space-separated. So
/// `on id`, `on uid:oid`, and `on a b c` all round-trip, as does a mixed
/// `on a x:y`.
pub fn join_on_clause(left_keys: &[String], right_keys: &[String]) -> String {
    let parts: Vec<String> = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| {
            if l == r {
                l.clone()
            } else {
                format!("{l}:{r}")
            }
        })
        .collect();
    format!("on {}", parts.join(" "))
}

/// Pick the field delimiter for a path by extension: `.tsv`/`.tab` use a tab,
/// everything else (including `.csv`) a comma. Keeps TSV a std-only, zero-config
/// feature — `open f.tsv` and `save out.tsv` just work.
pub fn delim_for_path(path: &str) -> u8 {
    let mut lower = path.to_ascii_lowercase();
    // A compression suffix doesn't change the field delimiter: `.tsv.gz` is
    // still tab-delimited. Strip it before checking the data extension.
    for suf in [".gz", ".zst", ".zstd"] {
        if let Some(stripped) = lower.strip_suffix(suf) {
            lower = stripped.to_string();
            break;
        }
    }
    if lower.ends_with(".tsv") || lower.ends_with(".tab") {
        b'\t'
    } else {
        COMMA
    }
}

/// Render the `as …` modifier needed so `path` re-parses with `delim`, for
/// `to_source` reversibility. Returns `None` when the path extension already
/// implies `delim` (e.g. `.tsv` → tab, `.csv` → comma) so the rendered source
/// stays clean; otherwise the explicit `as tsv` / `as csv` (or `delim "…"`).
pub fn delim_modifier_for(path: &str, delim: u8) -> Option<String> {
    if delim == delim_for_path(path) {
        return None;
    }
    Some(match delim {
        COMMA => "as csv".to_string(),
        b'\t' => "as tsv".to_string(),
        other => format!("delim \"{}\"", escape_delim(other)),
    })
}

/// Render a delimiter byte for display inside a quoted `delim "…"` modifier.
fn escape_delim(b: u8) -> String {
    match b {
        b'\t' => "\\t".to_string(),
        b'\n' => "\\n".to_string(),
        b'\r' => "\\r".to_string(),
        0x20..=0x7e => (b as char).to_string(),
        other => format!("\\x{other:02x}"),
    }
}

impl Op {
    /// Return a copy of this op with every `$x` value hole in its expressions
    /// bound from `bindings` (§25.3). Only ops that carry expressions are
    /// affected; the binding is structural (hole → value literal in the IR),
    /// never textual, so it cannot inject flow structure.
    pub fn bind_holes(&self, bindings: &HashMap<String, Value>) -> Op {
        match self {
            Op::Filter { pred } => Op::Filter {
                pred: pred.bind_holes(bindings),
            },
            Op::Validate { pred, disposition } => Op::Validate {
                pred: pred.bind_holes(bindings),
                disposition: *disposition,
            },
            Op::ProjectExpr { items } => Op::ProjectExpr {
                items: items
                    .iter()
                    .map(|(e, a)| (e.bind_holes(bindings), a.clone()))
                    .collect(),
            },
            Op::FilterProject { preds, fields } => Op::FilterProject {
                preds: preds.iter().map(|p| p.bind_holes(bindings)).collect(),
                fields: fields.clone(),
            },
            other => other.clone(),
        }
    }

    /// Collect the names of every `$x` value hole in this op's expressions.
    pub fn collect_holes(&self, out: &mut Vec<String>) {
        match self {
            Op::Filter { pred } | Op::Validate { pred, .. } => pred.collect_holes(out),
            Op::ProjectExpr { items } => items.iter().for_each(|(e, _)| e.collect_holes(out)),
            Op::FilterProject { preds, .. } => preds.iter().for_each(|p| p.collect_holes(out)),
            _ => {}
        }
    }

    /// Is this a sink (leaf writer)? Used so `| name` reuse splices only a
    /// flow's *transforms* and never drags its sink along (§25.4).
    pub fn is_sink(&self) -> bool {
        matches!(
            self,
            Op::SinkPrint | Op::SinkCsv { .. } | Op::SinkJsonl { .. } | Op::SinkJson { .. }
        )
    }

    /// A v1 single-file source: `Discovery::Fixed(path)` + `Transport::Local` +
    /// `codec`, provenance off. The parser layers provenance / read config /
    /// optimizer pushdowns on afterward.
    pub fn source(path: impl Into<String>, codec: Codec) -> Op {
        Op::Source {
            discovery: Discovery::Fixed(path.into()),
            transport: Transport::Local,
            codec,
            provenance: Provenance::Off,
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            // `readbin` for the binary codec, `open` for csv/jsonl (matching the
            // surface verb each desugars from).
            Op::Source { codec, .. } => match codec {
                Codec::Binary { .. } => "readbin",
                Codec::Discover { .. } => "ls",
                Codec::Csv { .. } | Codec::Jsonl => "open",
            },
            Op::Read { .. } => "read",
            Op::StreamRef { .. } => "stream",
            Op::Filter { .. } => "filter",
            Op::Validate { .. } => "validate",
            Op::Project { .. } => "project",
            Op::ProjectExpr { .. } => "project",
            Op::Take { .. } => "take",
            Op::Sort { .. } => "sort",
            Op::Distinct { .. } => "distinct",
            Op::Describe => "describe",
            Op::DropNa { .. } => "dropna",
            Op::Fill { .. } => "fill",
            Op::Rename { .. } => "rename",
            Op::Drop { .. } => "drop",
            Op::Cast { .. } => "cast",
            Op::Reorder { .. } => "reorder",
            Op::FilterProject { .. } => "fused",
            Op::GroupBy { .. } => "group",
            Op::Branch => "branch",
            Op::Merge => "merge",
            Op::Join { .. } => "join",
            Op::SinkPrint => "print",
            Op::SinkCsv { .. } => "save",
            Op::SinkJsonl { .. } => "save",
            Op::SinkJson { .. } => "save",
        }
    }

    /// Does this op **buffer** its input and emit mainly on `finish` (so it can
    /// look "stuck" mid-run while it accumulates rows)? The live dashboard uses
    /// this to show a "buffering N rows" working state instead of a stalled-
    /// looking `0` for a blocking operator (UX-J). The display only activates
    /// when the op is actually accumulating (`rows_in > rows_out && !finished`),
    /// so a streaming op (e.g. `distinct` first-occurrence) never false-shows it.
    pub fn is_blocking(&self) -> bool {
        matches!(
            self,
            Op::Sort { .. }
                | Op::GroupBy { .. }
                | Op::Distinct { .. }
                | Op::Describe
                | Op::Join { .. }
                | Op::Fill { .. }
        )
    }

    /// Render this op as the pipeline fragment that produced it.
    pub fn to_src_line(&self) -> String {
        match self {
            // A source renders by codec back to its v1 surface form (reversible),
            // using the discovery path and the top-level provenance modifier. The
            // `transport` layer has no surface syntax today (always Local).
            Op::Source {
                discovery,
                codec,
                provenance,
                ..
            } => {
                let path = discovery.path();
                match codec {
                    Codec::Csv {
                        header,
                        declared,
                        dt_formats,
                        delim,
                        projection,
                        prefilter,
                        str_prefilter,
                    } => {
                        let mut s = format!("open {path}");
                        if !header {
                            s.push_str(" noheader");
                        }
                        if let Some(m) = delim_modifier_for(path, *delim) {
                            s.push(' ');
                            s.push_str(&m);
                        }
                        if let Some(cols) = declared {
                            let parts: Vec<String> = cols
                                .iter()
                                .map(|(n, t)| match t {
                                    // Datetime renders in its annotation form
                                    // (`datetime` or `datetime("fmt")`), not the
                                    // DataType Display, so an explicit parse format
                                    // round-trips.
                                    Some(DataType::DateTime { .. }) => {
                                        match dt_formats.iter().find(|(c, _)| c == n) {
                                            Some((_, fmt)) => format!("{n}:datetime({fmt:?})"),
                                            None => format!("{n}:datetime"),
                                        }
                                    }
                                    Some(t) => format!("{n}:{t}"),
                                    None => n.clone(),
                                })
                                .collect();
                            s.push_str(&format!(" ({})", parts.join(" ")));
                        }
                        s.push_str(provenance.modifier());
                        if let Some(cols) = projection {
                            s.push_str(&format!("  # read-only: {}", cols.join(",")));
                        }
                        if !prefilter.is_empty() {
                            let preds: Vec<String> = prefilter
                                .iter()
                                .map(|(c, op, v)| format!("{c}{}{v}", op.as_str()))
                                .collect();
                            s.push_str(&format!("  # pre-filter: {}", preds.join(" and ")));
                        }
                        if !str_prefilter.is_empty() {
                            s.push_str(&format!("  # str-prefilter: {:?}", str_prefilter));
                        }
                        s
                    }
                    Codec::Binary {
                        fields,
                        endian,
                        c_align,
                    } => {
                        let cols: Vec<String> = fields
                            .iter()
                            .map(|(n, t)| format!("{n}:{}", t.as_str()))
                            .collect();
                        let mut mods = String::new();
                        if *endian == Endian::Big {
                            mods.push_str("be ");
                        }
                        if *c_align {
                            mods.push_str("aligned ");
                        }
                        format!(
                            "readbin {path} {mods}({}){}",
                            cols.join(" "),
                            provenance.modifier()
                        )
                    }
                    Codec::Jsonl => format!("open {path}{}", provenance.modifier()),
                    // `ls "glob"` — the path is the glob pattern; quote it so it
                    // re-lexes as one string token (reversible). Provenance is not
                    // applicable to a discovery source (it emits handles already).
                    // A pushed name pre-filter is an inert `#` comment (like CSV).
                    Codec::Discover { name_prefilter } => {
                        let mut s = format!("ls {path:?}");
                        if !name_prefilter.is_empty() {
                            s.push_str(&format!("  # name-prefilter: {name_prefilter:?}"));
                        }
                        s
                    }
                }
            }
            Op::Read { fmt, provenance } => {
                let mut s = "read".to_string();
                if let Some(f) = fmt {
                    s.push_str(&format!(" as {}", f.as_str()));
                }
                s.push_str(provenance.modifier());
                s
            }
            Op::StreamRef { name } => format!("stream {name}"),
            Op::Filter { pred } => format!("|? {pred}"),
            Op::Validate { pred, disposition } => format!("|! {pred} {}", disposition.as_str()),
            Op::Project { fields } => format!("|> {}", fields.join(" ")),
            Op::ProjectExpr { items } => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|(e, alias)| match e {
                        Expr::Field {
                            name,
                            access: Access::Fast,
                        } if name == alias => name.clone(),
                        // The parser's computed-column rule is `(expr) as alias`,
                        // so a computed item must render parenthesized to
                        // re-parse. `Arith` already self-parenthesizes; wrap
                        // anything that doesn't start with `(` (e.g. `case`,
                        // field renames, functions).
                        _ => {
                            let s = e.to_string();
                            if s.starts_with('(') {
                                format!("{s} as {alias}")
                            } else {
                                format!("({s}) as {alias}")
                            }
                        }
                    })
                    .collect();
                format!("|> {}", parts.join(" "))
            }
            Op::Take { n } => format!("take {n}"),
            Op::Sort { keys } => {
                let parts: Vec<String> = keys
                    .iter()
                    .map(|(k, desc)| {
                        if *desc {
                            format!("{k} desc")
                        } else {
                            k.clone()
                        }
                    })
                    .collect();
                format!("sort {}", parts.join(" "))
            }
            Op::Distinct { keys } => {
                if keys.is_empty() {
                    "distinct".to_string()
                } else {
                    format!("distinct {}", keys.join(" "))
                }
            }
            Op::Describe => "describe".to_string(),
            Op::DropNa { cols } => {
                if cols.is_empty() {
                    "dropna".to_string()
                } else {
                    format!("dropna {}", cols.join(" "))
                }
            }
            Op::Fill { col, method } => match method {
                FillMethod::Value(v) => format!("fill {col} \"{v}\""),
                FillMethod::Ffill => format!("fill {col} ffill"),
                FillMethod::Bfill => format!("fill {col} bfill"),
                FillMethod::Mean => format!("fill {col} mean"),
                FillMethod::Median => format!("fill {col} median"),
            },
            Op::Rename { pairs } => {
                let parts: Vec<String> = pairs.iter().map(|(f, t)| format!("{f} {t}")).collect();
                format!("rename {}", parts.join(" "))
            }
            Op::Drop { cols } => format!("drop {}", cols.join(" ")),
            Op::Cast { casts } => {
                let parts: Vec<String> = casts.iter().map(|(c, t)| format!("{c}:{t}")).collect();
                format!("cast {}", parts.join(" "))
            }
            Op::Reorder { cols } => format!("reorder {}", cols.join(" ")),
            Op::FilterProject { preds, fields } => {
                let mut s: String = preds.iter().map(|p| format!("|? {p} ")).collect();
                if let Some(f) = fields {
                    s.push_str(&format!("|> {}", f.join(" ")));
                }
                s.trim_end().to_string()
            }
            Op::GroupBy { keys, aggs } => {
                let mut s = format!("|# {}", keys.join(" "));
                for (f, c) in aggs {
                    s.push_str(&format!(" {}:{c}", f.label()));
                }
                s
            }
            Op::Branch => "-> branch".to_string(),
            Op::Merge => "+ merge".to_string(),
            Op::Join {
                left_keys,
                right_keys,
                kind,
            } => format!("{} {}", kind.amp(), join_on_clause(left_keys, right_keys)),
            Op::SinkPrint => "print".to_string(),
            Op::SinkCsv { path, delim } => match delim_modifier_for(path, *delim) {
                Some(m) => format!("save {path} {m}"),
                None => format!("save {path}"),
            },
            Op::SinkJsonl { path } => {
                // `.jsonl`/`.ndjson` paths imply jsonl; otherwise be explicit.
                let lower = path.to_ascii_lowercase();
                if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
                    format!("save {path}")
                } else {
                    format!("save {path} as jsonl")
                }
            }
            Op::SinkJson { path } => {
                // A `.json` path implies a JSON array; otherwise be explicit.
                if path.to_ascii_lowercase().ends_with(".json") {
                    format!("save {path}")
                } else {
                    format!("save {path} as json")
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    /// Normal data flow.
    Stream,
    /// Error side-channel (continue-first error stream).
    Error,
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    pub kind: EdgeKind,
}

/// Lifecycle events (Observability spec §10). Hooks are themselves scopes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookEvent {
    Begin,
    Process,
    End,
    First,
    Last,
    ChunkBegin,
    ChunkEnd,
    Error,
    Recovery,
    ModeChange,
    Retry,
    Timeout,
}

impl HookEvent {
    pub fn parse(s: &str) -> Option<HookEvent> {
        Some(match s {
            "begin" => HookEvent::Begin,
            "process" => HookEvent::Process,
            "end" => HookEvent::End,
            "first" => HookEvent::First,
            "last" => HookEvent::Last,
            "chunk_begin" => HookEvent::ChunkBegin,
            "chunk_end" => HookEvent::ChunkEnd,
            "error" => HookEvent::Error,
            "recovery" => HookEvent::Recovery,
            "mode_change" => HookEvent::ModeChange,
            "retry" => HookEvent::Retry,
            "timeout" => HookEvent::Timeout,
            _ => return None,
        })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            HookEvent::Begin => "begin",
            HookEvent::Process => "process",
            HookEvent::End => "end",
            HookEvent::First => "first",
            HookEvent::Last => "last",
            HookEvent::ChunkBegin => "chunk_begin",
            HookEvent::ChunkEnd => "chunk_end",
            HookEvent::Error => "error",
            HookEvent::Recovery => "recovery",
            HookEvent::ModeChange => "mode_change",
            HookEvent::Retry => "retry",
            HookEvent::Timeout => "timeout",
        }
    }
}

/// What a hook does when it fires (MVP subset).
#[derive(Debug, Clone)]
pub enum HookAction {
    /// Route matching items/chunks to a named flow (e.g. `on error: Errors`).
    Route(String),
    /// Escalate the runtime mode (`transition degraded`).
    Transition(Mode),
    /// Emit a log line.
    Log(String),
}

#[derive(Debug, Clone)]
pub struct Hook {
    pub event: HookEvent,
    /// Optional guard: `on error severity >= warning:`
    pub min_severity: Option<Severity>,
    pub action: HookAction,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    /// Scope label, if this node is the visible output of a named scope.
    pub label: Option<String>,
    pub op: Op,
    pub hooks: Vec<Hook>,
    /// Comment trivia that appeared immediately before this node's statement in
    /// the source (already canonicalized to `# …` / `#{ … }#`). Inert — it has
    /// no execution meaning — but preserved through the IR so `to_source` /
    /// `rivus fmt` round-trip the author's notes (§25.7). Empty for most nodes.
    pub leading_comments: Vec<String>,
    /// Provenance for the named-flow reuse form `| name` (§25.4): when this node
    /// was spliced in by *applying* a named flow's transforms, it carries that
    /// apply site `(site_id, flow_name, bindings)`. Nodes spliced by the same
    /// `| name` share a `site_id`, so `to_source` collapses the contiguous run
    /// back to `| name k=v …` (round-trip), while execution sees the plain
    /// desugared ops with the `$x` holes already bound to values (byte-identical
    /// to writing them inline). `bindings` are the `$x` value holes filled at
    /// this apply (§25.3), in source order. `None` for hand-written nodes.
    pub applied_from: Option<ApplySite>,
}

#[derive(Debug, Clone, Default)]
pub struct PlanGraph {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    /// Scope label → producing node.
    pub labels: HashMap<String, NodeId>,
}

impl PlanGraph {
    pub fn new() -> Self {
        PlanGraph::default()
    }

    /// Names of `$x` value holes that remain **unbound** in nodes that will
    /// execute (de-duplicated, sorted). A bound hole becomes a literal at its
    /// apply site, so anything still here is a hole reaching evaluation with no
    /// value — the runtime surfaces it (never-silent) instead of evaluating it
    /// to null in silence (§25.3).
    pub fn unbound_holes(&self) -> Vec<String> {
        let mut names = Vec::new();
        for node in &self.nodes {
            node.op.collect_holes(&mut names);
        }
        names.sort();
        names.dedup();
        names
    }

    pub fn add_node(&mut self, op: Op) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node {
            id,
            label: None,
            op,
            hooks: Vec::new(),
            leading_comments: Vec::new(),
            applied_from: None,
        });
        id
    }

    pub fn label_node(&mut self, id: NodeId, label: impl Into<String>) {
        let label = label.into();
        self.nodes[id].label = Some(label.clone());
        self.labels.insert(label, id);
    }

    pub fn add_edge(&mut self, from: NodeId, to: NodeId, kind: EdgeKind) {
        self.edges.push(Edge { from, to, kind });
    }

    pub fn add_hook(&mut self, id: NodeId, hook: Hook) {
        self.nodes[id].hooks.push(hook);
    }

    pub fn inputs_of(&self, id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.to == id && e.kind == EdgeKind::Stream)
            .map(|e| e.from)
            .collect()
    }

    pub fn outputs_of(&self, id: NodeId) -> Vec<NodeId> {
        self.edges
            .iter()
            .filter(|e| e.from == id && e.kind == EdgeKind::Stream)
            .map(|e| e.to)
            .collect()
    }

    /// Sinks / leaves: nodes with no downstream stream edge.
    pub fn leaves(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| self.outputs_of(n.id).is_empty())
            .map(|n| n.id)
            .collect()
    }

    /// Kahn topological order over stream edges. Returns `None` on a cycle
    /// (Rivus forbids cycles in the MVP; feedback edges are future work).
    pub fn topo_order(&self) -> Option<Vec<NodeId>> {
        let n = self.nodes.len();
        let mut indeg = vec![0usize; n];
        for e in &self.edges {
            if e.kind == EdgeKind::Stream {
                indeg[e.to] += 1;
            }
        }
        let mut queue: Vec<NodeId> = (0..n).filter(|&i| indeg[i] == 0).collect();
        let mut order = Vec::with_capacity(n);
        while let Some(id) = queue.pop() {
            order.push(id);
            for succ in self.outputs_of(id) {
                indeg[succ] -= 1;
                if indeg[succ] == 0 {
                    queue.push(succ);
                }
            }
        }
        if order.len() == n {
            Some(order)
        } else {
            None
        }
    }

    /// Regenerate readable Rivus source from the graph (Master principle #5:
    /// IR reversibility). Linear flows, merge/join scopes **and** `->` branch
    /// fan-out all round-trip: `parse(to_source(g))` is the same graph. (The
    /// optimizer can rewrite the graph first; we always render the result.)
    pub fn to_source(&self) -> String {
        let mut out = String::new();
        // Emit one block per labeled scope, in stable id order — except scopes
        // that are *branch children* of another scope, which are rendered inline
        // (`-> Label: …`) by their parent (see `write_chain`).
        let mut labeled: Vec<&Node> = self
            .nodes
            .iter()
            .filter(|n| n.label.is_some() && self.branch_parent_fanout(n.id).is_none())
            .collect();
        labeled.sort_by_key(|n| n.id);

        for node in labeled {
            let label = node.label.as_ref().unwrap();
            let inputs = self.inputs_of(node.id);

            // Merge / join scopes render as `Label: A + B ;`.
            match &node.op {
                Op::Merge => {
                    let names = self.input_labels(&inputs).join(" + ");
                    let _ = writeln!(out, "{label}:\n    {names}\n;");
                    continue;
                }
                Op::Join {
                    left_keys,
                    right_keys,
                    kind,
                } => {
                    let sep = format!(" {} ", kind.amp());
                    let names = self.input_labels(&inputs).join(&sep);
                    let on = join_on_clause(left_keys, right_keys);
                    let _ = writeln!(out, "{label}:\n    {names} {on}\n;");
                    continue;
                }
                _ => {}
            }

            // Otherwise walk the linear chain ending at this node, inlining any
            // branch children that fan out from it.
            let _ = writeln!(out, "{label}:");
            self.write_chain(&mut out, node.id, 1);
            let _ = writeln!(out, ";");
        }
        out
    }

    /// Write the linear chain ending at `tail`, one step per line indented
    /// `depth` levels, inlining branch children (`-> Label: … ;`) that fan out
    /// from any node in the chain. Recurses for nested branches.
    fn write_chain(&self, out: &mut String, tail: NodeId, depth: usize) {
        let pad = "    ".repeat(depth);
        let chain = self.linear_chain_to(tail);
        let mut i = 0;
        while i < chain.len() {
            let nid = chain[i];
            // Inert comment trivia preceding this step (§25.7), re-emitted in
            // source order at the step's indentation so `rivus fmt` round-trips.
            for c in &self.nodes[nid].leading_comments {
                let _ = writeln!(out, "{pad}{c}");
            }
            // A contiguous run spliced from one `| name` apply collapses back to
            // that single form (§25.4 round-trip), instead of the desugared ops.
            if let Some((site, name, bindings)) = &self.nodes[nid].applied_from {
                let mut line = format!("| {name}");
                for (k, v) in bindings {
                    match v {
                        Value::Str(s) => {
                            line.push_str(&format!(" {k}=\"{}\"", Expr::escape_string(s)))
                        }
                        other => line.push_str(&format!(" {k}={other}")),
                    }
                }
                let _ = writeln!(out, "{pad}{line}");
                i += 1;
                while i < chain.len()
                    && self.nodes[chain[i]].applied_from.as_ref().map(|(s, ..)| *s) == Some(*site)
                {
                    i += 1;
                }
                continue;
            }
            let _ = writeln!(out, "{pad}{}", self.nodes[nid].op.to_src_line());
            for h in &self.nodes[nid].hooks {
                self.write_hook(out, h);
            }
            // Branch children fanning out from this node, rendered inline so the
            // whole DAG round-trips (their flow continues from here).
            for child in self.branch_children_of(nid) {
                let clabel = self.nodes[child].label.as_ref().unwrap();
                let _ = writeln!(out, "{pad}-> {clabel}:");
                self.write_chain(out, child, depth + 1);
                let _ = writeln!(out, "{pad};");
            }
            i += 1;
        }
    }

    /// If the scope ending at `scope_tail` continues from a fan-out node in
    /// another scope, return that node — i.e. this is a branch child, rendered
    /// inline by its parent. `None` for an independent scope (its chain starts
    /// at a source) or a merge/join scope.
    fn branch_parent_fanout(&self, scope_tail: NodeId) -> Option<NodeId> {
        if matches!(self.nodes[scope_tail].op, Op::Merge | Op::Join { .. }) {
            return None;
        }
        let chain = self.linear_chain_to(scope_tail);
        let root = *chain.first()?;
        // A source-rooted chain has no input → independent scope; otherwise the
        // single input is the parent's fan-out point.
        self.inputs_of(root).first().copied()
    }

    /// Labeled branch-child scopes that fan out directly from `node`, id-sorted.
    fn branch_children_of(&self, node: NodeId) -> Vec<NodeId> {
        let mut kids: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| n.label.is_some() && self.branch_parent_fanout(n.id) == Some(node))
            .map(|n| n.id)
            .collect();
        kids.sort_unstable();
        kids
    }

    fn input_labels(&self, inputs: &[NodeId]) -> Vec<String> {
        inputs
            .iter()
            .map(|&i| {
                self.nodes[i]
                    .label
                    .clone()
                    .unwrap_or_else(|| format!("<{}>", self.nodes[i].op.kind_str()))
            })
            .collect()
    }

    /// The transform ops of the flow whose output node is `tail`, in source
    /// order, **excluding the source head and any sink** — i.e. exactly what the
    /// named-flow reuse form `| name` (§25.4) splices into another flow. A reuse
    /// recipe contributes only its transforms; it never drags the original
    /// flow's sink along (stops at the first sink). Cloned, so the caller can
    /// desugar `| name` into copies that execute byte-identically to writing
    /// those transforms inline. (Only the linear chain is taken; a referenced
    /// flow's own branches/merges are not spliced.)
    pub fn flow_transform_ops(&self, tail: NodeId) -> Vec<Op> {
        let chain = self.linear_chain_to(tail);
        chain
            .iter()
            .skip(1) // drop the source head
            .map(|&n| &self.nodes[n].op)
            .take_while(|op| !op.is_sink()) // stop before any sink
            .cloned()
            .collect()
    }

    /// Collect the linear chain of single-input nodes leading up to `id`,
    /// stopping at fan-in (merge/join) or labeled upstream scopes.
    fn linear_chain_to(&self, id: NodeId) -> Vec<NodeId> {
        let mut chain = vec![id];
        let mut cur = id;
        loop {
            let inputs = self.inputs_of(cur);
            if inputs.len() != 1 {
                break;
            }
            let prev = inputs[0];
            // Stop if the predecessor is a labeled node: a label always marks a
            // scope output, so it is a scope boundary regardless of fan-out
            // count. (Stopping only at fan-out >1 used to absorb a single-output
            // parent into this chain, which broke round-trip for a fan-out-of-one
            // `-> Child:` branch — it then re-rendered as a duplicated source.)
            if self.nodes[prev].label.is_some() {
                break;
            }
            chain.push(prev);
            cur = prev;
        }
        chain.reverse();
        chain
    }

    fn write_hook(&self, out: &mut String, h: &Hook) {
        let guard = match h.min_severity {
            Some(s) => format!(" severity >= {s}"),
            None => String::new(),
        };
        let _ = writeln!(out, "    on {}{}:", h.event.as_str(), guard);
        match &h.action {
            HookAction::Route(name) => {
                let _ = writeln!(out, "        {name}");
            }
            HookAction::Transition(mode) => {
                let _ = writeln!(out, "        transition {mode}");
            }
            HookAction::Log(msg) => {
                let _ = writeln!(out, "        log \"{msg}\"");
            }
        }
        let _ = writeln!(out, "    ;");
    }
}
