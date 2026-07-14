//! The DAG IR.
//!
//! Rivus is DAG-native (Master principle #3): even a "linear" pipeline is a
//! degenerate DAG. Nodes are flow scopes / transforms / events; edges are
//! streams (or error side-channels). The graph is the single source of truth
//! that the optimizer rewrites and that [`PlanGraph::to_source`] regenerates
//! back into readable Rivus source (Master principle #5: IR reversibility).

use crate::expr::{Access, CmpOp, Expr, PathExpr};
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

/// One named sub-view of a union view (§29.3, s2): the half-open **character**
/// range `[start, end)` of a fixed-width string column, named `name`. Defined by
/// a `:{ name@start..end … }` block and referenced as `base.name` via the `.`
/// accessor (lowered to [`crate::Expr::SubView`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubView {
    pub name: String,
    pub start: u32,
    pub end: u32,
}

/// A union-view definition attached to an `Op::ProjectExpr` item (§29.3, s2):
/// column `col` (kept physically as one string column) gains the logical
/// sub-views `subs`. `width` is the optional declared total width from
/// `:string(W)` — preserved here for faithful `to_source` because `DataType::Str`
/// carries no width. The sub-views are **not** materialized as columns; they are
/// resolved lazily by the `base.name` accessor as zero-copy slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewDef {
    pub col: String,
    pub width: Option<u32>,
    pub subs: Vec<SubView>,
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

/// The kind of time-series shift `shift col …` computes (#65). `n` (rows back)
/// is carried on `Op::Shift`, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShiftKind {
    /// `lag(col, n)` — the value `n` rows back (per group, source order).
    Lag,
    /// `diff(col, n)` — `col − lag(col, n)`; a datetime column yields an exact
    /// `Duration` (#57), otherwise the column's own numeric lane.
    Diff,
    /// `pct_change(col, n)` — `(col − lag)/lag` as `f64`.
    PctChange,
}

impl ShiftKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ShiftKind::Lag => "lag",
            ShiftKind::Diff => "diff",
            ShiftKind::PctChange => "pct_change",
        }
    }
    pub fn parse(s: &str) -> Option<ShiftKind> {
        match s {
            "lag" => Some(ShiftKind::Lag),
            "diff" => Some(ShiftKind::Diff),
            "pct_change" => Some(ShiftKind::PctChange),
            _ => None,
        }
    }
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
    /// `array_agg:col` (aliases `list_agg`, `arr`) — collect the group's
    /// **non-null** values into a `List` lane (§32 / #172), in source order. The
    /// dual of `explode`: `explode` flattens a list to rows, `array_agg` folds
    /// rows back into a list. Like `first`/`last` it is order-dependent but
    /// deterministic — the parallel partition→merge concatenates in source order,
    /// so serial == parallel == chunk-size (element order included). Buffers each
    /// group's values, so it is a pipeline-breaker bounded by group size.
    ArrayAgg,
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
            // `list_agg` / `arr` are aliases; the canonical name is `array_agg`.
            "array_agg" | "list_agg" | "arr" => AggFunc::ArrayAgg,
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
            AggFunc::ArrayAgg => "array_agg",
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
    /// Fixed-width text field `char[N]` (§29.4, s2 follow-up): `N` raw bytes
    /// decoded as UTF-8 text. Rides the `Str` lane; byte width `N`, 1-byte
    /// alignment (a C `char[]`). All `N` bytes are kept as the value (trailing
    /// NUL / padding included — §29.5-3), so a binary record has no empty cell.
    Char(u32),
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
            BinType::Char(n) => *n as usize,
        }
    }

    /// Natural alignment in bytes (for C `repr(C)` layout). For the numeric
    /// primitives alignment equals size; a `char[N]` is a byte array (align 1).
    pub fn align(&self) -> usize {
        match self {
            BinType::Char(_) => 1,
            _ => self.size(),
        }
    }

    /// Which columnar execution lane this decodes into.
    pub fn lane(&self) -> DataType {
        match self {
            BinType::Bool => DataType::Bool,
            BinType::F32 | BinType::F64 => DataType::F64,
            BinType::Char(_) => DataType::Str,
            _ => DataType::I64,
        }
    }

    /// Source spelling of the field type (`char[N]` carries its width, so this
    /// returns an owned `String` rather than a `&'static str`).
    pub fn label(&self) -> String {
        match self {
            BinType::I8 => "i8".to_string(),
            BinType::I16 => "i16".to_string(),
            BinType::I32 => "i32".to_string(),
            BinType::I64 => "i64".to_string(),
            BinType::U8 => "u8".to_string(),
            BinType::U16 => "u16".to_string(),
            BinType::U32 => "u32".to_string(),
            BinType::U64 => "u64".to_string(),
            BinType::F32 => "f32".to_string(),
            BinType::F64 => "f64".to_string(),
            BinType::Bool => "bool".to_string(),
            BinType::Char(n) => format!("char[{n}]"),
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
    /// **Unbounded** discovery (`watch "glob"`, design §28.12 / ratified #149):
    /// subscribe to the OS change-notification mechanism and emit a handle row
    /// per changed file matching the glob — a stream that never ends. Outside
    /// the deterministic-op set (§0.14): arrival order is environmental, so the
    /// byte-identity contract is asserted only on **bounded** sub-DAGs (see
    /// [`PlanGraph::unbounded_nodes`]). Parse / `to_source` are always-std;
    /// evaluation requires the off-by-default `unbounded` feature (a
    /// feature-less run refuses pre-run, never-silent).
    Watch(String),
    /// **Unbounded** network discovery (`subscribe "tcp://host:port"`, design
    /// §33 / §28.12.5): dial a TCP endpoint as a client and stream newline-
    /// delimited records — a feed that never ends. Like `Watch` it is outside the
    /// deterministic-op set (§0.14): arrival order is environmental. Parse /
    /// `to_source` are always-std; evaluation requires the off-by-default `net`
    /// feature (a feature-less run refuses pre-run, never-silent). Unlike `Watch`
    /// (which needs `unbounded` / `notify`) this rides the `net` feature — the two
    /// unbounded sources are gated apart.
    Subscribe(String),
}

/// Does `p` name an HTTP(S) URL (the networked `open`/`read` source scheme,
/// §33)? A cheap, std-only prefix test usable from the IR (no runtime `Scheme`
/// dependency) so `PlanGraph::uses_net` can gate the `net` feature before run.
pub fn is_http_url(p: &str) -> bool {
    // Byte-wise prefix test (ASCII case-insensitive). Operating on bytes avoids a
    // panic on a non-ASCII path where a fixed `str` byte index (`l[..7]`) would
    // fall mid-multibyte-char (e.g. a Japanese filename, #178).
    let b = p.trim_start().as_bytes();
    b.len() >= 7 && b[..7].eq_ignore_ascii_case(b"http://")
        || b.len() >= 8 && b[..8].eq_ignore_ascii_case(b"https://")
}

impl Discovery {
    /// The discovery's path/pattern string: the fixed path (`Fixed`), the glob
    /// pattern (`Glob`/`Watch`) or the endpoint (`Subscribe`). Used for
    /// `to_source` and the parallel-read size gate.
    pub fn path(&self) -> &str {
        match self {
            Discovery::Fixed(p)
            | Discovery::Glob(p)
            | Discovery::Watch(p)
            | Discovery::Subscribe(p) => p,
        }
    }

    /// Is this the unbounded **file-`watch`** discovery (needs the `unbounded` /
    /// `notify` feature), as opposed to the network `subscribe` (needs `net`)?
    /// The two unbounded sources are feature-gated apart.
    pub fn is_watch(&self) -> bool {
        matches!(self, Discovery::Watch(_))
    }

    /// Is this a **networked** discovery — `subscribe` (always), or a `Fixed` /
    /// `Glob` / `Watch` whose path is an `http://` / `https://` URL (§33)?
    /// Networked sources need the `net` feature; the runtime refuses a
    /// feature-less plan pre-run (never-silent).
    pub fn is_net(&self) -> bool {
        match self {
            Discovery::Subscribe(_) => true,
            Discovery::Fixed(p) | Discovery::Glob(p) | Discovery::Watch(p) => is_http_url(p),
        }
    }

    /// Is this discovery **unbounded** (a stream that never ends)? The source of
    /// the boundedness-derived determinism tag (§0.14 / §28.12): everything
    /// downstream of an unbounded discovery is outside the byte-identity
    /// contract and must not be re-ordered or re-combined by the optimizer or
    /// the parallel executor.
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Discovery::Watch(_) | Discovery::Subscribe(_))
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
    /// Apache Parquet (columnar, `.parquet`). Read-only in this slice, behind
    /// the runtime's off-by-default `parquet` feature (SUPPLY-CHAIN selected
    /// adapter): the IR/parser always know the codec (std-only, `explain`
    /// works in any build) and a feature-less run refuses the plan pre-run
    /// (never-silent, same shape as `regex`/`gzip`).
    Parquet,
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

/// **Route** (design §28.7): where a sink's encoded bytes go — the destination
/// resource(s), the output mirror of `Discovery`. Today this is always a single
/// fixed path (`save out.csv`); slice 4b adds the template / `by key`
/// partitioned form (issue #143) as further variants, without re-shaping `Op`.
#[derive(Debug, Clone)]
pub enum Route {
    /// A single fixed output path — the v1 `save` destination.
    Fixed(String),
    /// Partitioned / dynamic output (§28.7 route, ratified #143): rows split by
    /// the partition-key values, each group to its own file. `template` is the
    /// raw source template; `{col}` placeholders derive the keys
    /// (`save "out/{country}.csv"` ≡ `by country` — §27.3 is the degenerate
    /// form of §27.4). A plain path with explicit `by` uses the Hive `k=v/`
    /// layout (DuckDB-compatible), or `v1_v2.ext` with `flat`. `by` always
    /// carries the full key list (derived or explicit). The file set and every
    /// path are a **pure, injective function** of the key values
    /// (percent-escaped incl. `%`; null → `__HIVE_DEFAULT_PARTITION__`).
    Template {
        template: String,
        by: Vec<String>,
        flat: bool,
        /// Parsed expressions for the template's **computed** placeholders
        /// (`{substr(id,22,4)}`, #143 ① / s4c), in `RouteSeg::Raw` order. Each
        /// is its own anonymous partition key, evaluated per row. The raw
        /// `template` stays authoritative for `to_source` (verbatim).
        exprs: Vec<Expr>,
    },
}

impl Route {
    /// The route's single fixed path — `None` for the partitioned template
    /// route, so every caller must decide explicitly what a multi-file
    /// destination means for it (the slice-4a forcing function).
    pub fn path(&self) -> Option<&str> {
        match self {
            Route::Fixed(p) => Some(p),
            Route::Template { .. } => None,
        }
    }
}

/// One piece of a parsed `save` template: literal text or a `{col}` key
/// placeholder. `{{` / `}}` escape literal braces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteSeg {
    Lit(String),
    Key(String),
    /// A computed placeholder's raw source text (`substr(id,22,4)`), parsed
    /// into an [`Expr`] by the parser (carried on `Route::Template::exprs`).
    Raw(String),
}

/// Parse a `save` template into segments (shared by the parser's
/// declaration-time validation and the runtime's path rendering, so the two
/// can never drift). Errors are program errors (never-silent, §27.3):
/// unbalanced braces, an empty `{}`, or a non-identifier placeholder.
pub fn parse_route_template(s: &str) -> Result<Vec<RouteSeg>, String> {
    let mut segs = Vec::new();
    let mut lit = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                lit.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                lit.push('}');
            }
            '}' => return Err("unbalanced '}' in save template (use '}}' for a literal)".into()),
            '{' => {
                // Collect to the matching '}' then classify: identifier text is
                // a named column key; anything else is a computed-expression
                // placeholder (s4c, #143 ①), parsed by the parser. A '}' inside
                // the expression (e.g. in a string literal) is unsupported —
                // it closes the placeholder and the snippet fails to parse
                // (declaration-time, never silent).
                let mut text = String::new();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(ch) => text.push(ch),
                        None => return Err("unclosed '{' in save template".into()),
                    }
                }
                if text.is_empty() {
                    return Err("empty '{}' placeholder in save template".into());
                }
                if !lit.is_empty() {
                    segs.push(RouteSeg::Lit(std::mem::take(&mut lit)));
                }
                if text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    segs.push(RouteSeg::Key(text));
                } else {
                    segs.push(RouteSeg::Raw(text));
                }
            }
            other => lit.push(other),
        }
    }
    if !lit.is_empty() {
        segs.push(RouteSeg::Lit(lit));
    }
    Ok(segs)
}

/// **Sink codec** (design §28.7): how chunks encode into bytes — the write-side
/// mirror of [`Codec`]. Carries only encode configuration (no reader pushdowns).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkCodec {
    /// Delimited text; `delim` is the field byte (`b','` CSV / `b'\t'` TSV).
    Csv { delim: u8 },
    /// JSON Lines — one object per row, streaming.
    Jsonl,
    /// A single JSON array (`[{…},{…}]`); written incrementally (open bracket,
    /// comma-separated rows, close bracket) so it stays bounded-memory.
    Json,
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
    ///
    /// `views` carries any union sub-view definitions introduced by a
    /// `col :string(W) :{ name@start..end … }` block (§29.3, s2). It is empty for
    /// ordinary projections (behaviour unchanged); when present it is metadata
    /// only — the op materializes no extra columns — so the `base.name` accessor
    /// downstream can resolve sub-views and `to_source` can re-emit the block.
    ProjectExpr {
        items: Vec<(Expr, String)>,
        views: Vec<ViewDef>,
    },
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
    Sort { keys: Vec<(PathExpr, bool)> },
    /// `distinct [KEY ...]` — drop duplicate rows, keeping the first occurrence.
    /// With no keys, the whole row is the dedup key; otherwise only the named
    /// columns. Streaming (emits as it goes) but stateful (a global seen-set),
    /// so it runs on the serial path. Output order = first-occurrence order.
    Distinct { keys: Vec<PathExpr> },
    /// `describe` — replace the stream with a one-row-per-column summary
    /// (column, type, count, min, max, mean). A streaming, single-pass
    /// accumulator that emits on finish; stateful → serial path.
    Describe,
    /// `dropna [col ...]` — drop rows with a missing (empty) value in any of the
    /// named columns (or any column when none named). Streaming, stateless.
    DropNa { cols: Vec<String> },
    /// `explode COL` (alias `unnest COL`) — multiply rows over a `List` column
    /// (§32 s4c): one output row per list element, with the other columns
    /// repeated and `COL` replaced by the element (its lane = the list's element
    /// type). An empty or null list contributes **zero** rows (Arrow `UNNEST` /
    /// SQL semantics); expansion order is the list's physical order
    /// (deterministic). Streaming, stateless per chunk → partition-safe.
    Explode { col: String },
    /// `fill col VALUE|ffill|bfill` — replace missing (empty) cells of `col`.
    /// `VALUE` substitutes a constant (the column becomes text); `ffill` carries
    /// the last non-empty value forward, `bfill` the next non-empty value back.
    /// A constant fill is streaming/stateless; `ffill`/`bfill` are stateful
    /// (they carry state across rows and chunks) → serial path.
    Fill { col: String, method: FillMethod },
    /// `sessionize TS gap "30m" [by COL ...]` — session windows (§36.5 / #60):
    /// append a `session` column carrying each row's **session start** (a
    /// datetime on `ts`'s lane — the same "window start as key" shape as
    /// `bucket`/`hops`, so `|# session …` aggregates per session). A new
    /// session starts when the gap to the previous row's ts (per `by` group)
    /// exceeds `gap`. Stateful per group (last ts + current start), streaming
    /// per-chunk emit, order-dependent → serial path (like `ffill`); input is
    /// assumed time-ascending and a regression is counted + surfaced
    /// (continue-first, never-silent).
    Sessionize {
        ts: String,
        gap: String,
        by: Vec<String>,
    },
    /// `shift COL lag|diff|pct_change [N] [by COL ...] as ALIAS` — time-series
    /// shift/difference primitives (#65): append `out` carrying a value derived
    /// from an earlier row **within the same `by` group, in source order**.
    /// `Lag(n)` = the value `n` rows back (null for the first `n`); `Diff(n)` =
    /// `col − lag(col, n)` (a datetime column yields an exact `Duration`, #57);
    /// `PctChange(n)` = `(col − lag)/lag` as `f64`. Stateful per group (a ring
    /// of the last `n` values), streaming per-chunk emit, order-dependent →
    /// serial path (like `ffill`/`sessionize`); chunk-size independent because
    /// the shift is defined in source order. Backward-only in this slice;
    /// `lead` (look-ahead) and time-shift `lag(x, 5m)` (as-of) are follow-ups.
    Shift {
        col: String,
        kind: ShiftKind,
        n: u32,
        by: Vec<String>,
        out: String,
    },
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
        keys: Vec<PathExpr>,
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
        left_keys: Vec<PathExpr>,
        right_keys: Vec<PathExpr>,
        kind: JoinKind,
    },
    /// `&` **as-of / temporal join** (#64): `Left & Right [on KEY…] asof TS
    /// [within "DUR"]`. Enrich each left row with the right row whose `ts` is
    /// the **nearest ≤** the left's (backward), matched exactly on the `by`
    /// keys. Left-outer: every left row is kept (no match → null right
    /// columns). `tolerance` (a duration string) drops matches older than the
    /// bound. Both inputs are assumed time-ascending; the operator sorts the
    /// right side by `ts` per group so the result is chunk-size independent.
    /// Order-dependent → serial path (like `join`/`sessionize`). Backward-only
    /// in this slice; forward/nearest and inner variants are follow-ups.
    AsofJoin {
        by: Vec<String>,
        ts: String,
        tolerance: Option<String>,
    },
    /// `print` / default leaf sink — a display leaf, not an encoded file write,
    /// so it stays outside the `Sink` unification.
    SinkPrint,
    /// A **sink** (design §28.7/§28.8): encode chunks with `codec` and write
    /// them over `transport` to the destination(s) chosen by `route` — the
    /// output mirror of [`Op::Source`]. One composable node replacing the
    /// former format-specific `SinkCsv`/`SinkJsonl`/`SinkJson`, so routing
    /// (slice 4b's template / `by key` split, issue #143) and remote transports
    /// (slice 5) attach here without re-stratifying output by format. The v1
    /// surface forms — `save PATH [as FMT]`, `writecsv`/`writejson` — desugar
    /// to this (`Route::Fixed` + `Transport::Local` + the matching codec), and
    /// `to_source` restores the original surface form (reversible).
    Sink {
        route: Route,
        transport: Transport,
        codec: SinkCodec,
    },
}

/// The default CSV field delimiter.
pub const COMMA: u8 = b',';

/// Render a join's `on` clause faithfully for `to_source`: one token per key
/// pair, `lk` when the two names are equal else `lk:rk`, space-separated. So
/// `on id`, `on uid:oid`, and `on a b c` all round-trip, as does a mixed
/// `on a x:y`.
pub fn join_on_clause(left_keys: &[PathExpr], right_keys: &[PathExpr]) -> String {
    let parts: Vec<String> = left_keys
        .iter()
        .zip(right_keys.iter())
        .map(|(l, r)| {
            // A bare key path round-trips as its plain name (§32 s2 pin), so a
            // same-named pair stays `on uid`, never `on uid()`/`on uid:uid`.
            if l == r {
                l.to_string()
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
            Op::ProjectExpr { items, views } => Op::ProjectExpr {
                items: items
                    .iter()
                    .map(|(e, a)| (e.bind_holes(bindings), a.clone()))
                    .collect(),
                views: views.clone(),
            },
            Op::FilterProject { preds, fields } => Op::FilterProject {
                preds: preds.iter().map(|p| p.bind_holes(bindings)).collect(),
                fields: fields.clone(),
            },
            Op::Sink {
                route:
                    Route::Template {
                        template,
                        by,
                        flat,
                        exprs,
                    },
                transport,
                codec,
            } => Op::Sink {
                route: Route::Template {
                    template: template.clone(),
                    by: by.clone(),
                    flat: *flat,
                    exprs: exprs.iter().map(|e| e.bind_holes(bindings)).collect(),
                },
                transport: *transport,
                codec: *codec,
            },
            other => other.clone(),
        }
    }

    /// Collect the names of every `$x` value hole in this op's expressions.
    pub fn collect_holes(&self, out: &mut Vec<String>) {
        match self {
            Op::Sink {
                route: Route::Template { exprs, .. },
                ..
            } => exprs.iter().for_each(|e| e.collect_holes(out)),
            Op::Filter { pred } | Op::Validate { pred, .. } => pred.collect_holes(out),
            Op::ProjectExpr { items, .. } => items.iter().for_each(|(e, _)| e.collect_holes(out)),
            Op::FilterProject { preds, .. } => preds.iter().for_each(|p| p.collect_holes(out)),
            _ => {}
        }
    }

    /// Is this a sink (leaf writer)? Used so `| name` reuse splices only a
    /// flow's *transforms* and never drags its sink along (§25.4).
    pub fn is_sink(&self) -> bool {
        matches!(self, Op::SinkPrint | Op::Sink { .. })
    }

    /// A v1 fixed-path local file sink: `Route::Fixed(path)` +
    /// `Transport::Local` + `codec` — the `save` desugar target (the mirror of
    /// [`Op::source`]).
    pub fn sink(path: impl Into<String>, codec: SinkCodec) -> Op {
        Op::Sink {
            route: Route::Fixed(path.into()),
            transport: Transport::Local,
            codec,
        }
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
            // surface verb each desugars from). A discovery source is `ls`, or
            // `watch` when the discovery is the unbounded subscription (§28.12).
            Op::Source {
                codec, discovery, ..
            } => match codec {
                Codec::Binary { .. } => "readbin",
                Codec::Discover { .. } if discovery.is_unbounded() => "watch",
                Codec::Discover { .. } => "ls",
                // A data codec over the unbounded network `subscribe` source
                // (§33) is `subscribe`; otherwise a fixed/HTTP `open`.
                Codec::Csv { .. } | Codec::Jsonl
                    if matches!(discovery, Discovery::Subscribe(_)) =>
                {
                    "subscribe"
                }
                Codec::Csv { .. } | Codec::Jsonl | Codec::Parquet => "open",
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
            Op::Explode { .. } => "explode",
            Op::Fill { .. } => "fill",
            Op::Sessionize { .. } => "sessionize",
            Op::Shift { .. } => "shift",
            Op::Rename { .. } => "rename",
            Op::Drop { .. } => "drop",
            Op::Cast { .. } => "cast",
            Op::Reorder { .. } => "reorder",
            Op::FilterProject { .. } => "fused",
            Op::GroupBy { .. } => "group",
            Op::Branch => "branch",
            Op::Merge => "merge",
            Op::Join { .. } => "join",
            Op::AsofJoin { .. } => "asof",
            Op::SinkPrint => "print",
            Op::Sink { .. } => "save",
        }
    }

    /// Does this op **buffer/accumulate its whole input and emit only on
    /// `finish`** (so it shows `rows_out == 0` mid-run and can look "stuck" while
    /// it works)? The live dashboard uses this to show a "buffering N rows"
    /// working state for such a node (UX-J). The indicator only lights when the
    /// op is actually accumulating (`rows_in > rows_out && !finished`).
    ///
    /// **Streaming ops are excluded** even when stateful: `distinct` emits each
    /// first-occurrence immediately, `ffill` carries forward and emits per chunk,
    /// a constant `fill` rewrites in place — their `rows_out` tracks `rows_in`, so
    /// they never look stuck. Audited against the operators: `sort` / `group` /
    /// `describe` / `join` (buffers both sides) and the `fill` variants that need
    /// the whole input first — `bfill` (replays backward on finish) and
    /// `mean`/`median` (need the global statistic) — emit only on `finish`.
    pub fn is_blocking(&self) -> bool {
        match self {
            Op::Sort { .. }
            | Op::GroupBy { .. }
            | Op::Describe
            | Op::Join { .. }
            | Op::AsofJoin { .. } => true,
            Op::Fill { method, .. } => matches!(
                method,
                FillMethod::Bfill | FillMethod::Mean | FillMethod::Median
            ),
            _ => false,
        }
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
                        // `subscribe "tcp://…"` (§33) renders with the quoted
                        // endpoint and the `subscribe` verb; an `http://` URL is
                        // quoted so it re-lexes as one string token (reversible);
                        // a plain path stays bare. All share the modifiers below.
                        let mut s = match discovery {
                            Discovery::Subscribe(_) => format!("subscribe {path:?}"),
                            _ if is_http_url(path) => format!("open {path:?}"),
                            _ => format!("open {path}"),
                        };
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
                            .map(|(n, t)| format!("{n}:{}", t.label()))
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
                    // Parquet: a file `open` keeps the bare path (the `.parquet`
                    // extension picks the codec back up on re-parse); any other
                    // path spells the codec explicitly.
                    Codec::Parquet => {
                        let lower = path.to_ascii_lowercase();
                        if lower.ends_with(".parquet") {
                            format!("open {path}{}", provenance.modifier())
                        } else {
                            format!("open {path} as parquet{}", provenance.modifier())
                        }
                    }
                    // JSONL `subscribe` renders `subscribe "tcp://…" as json`; an
                    // `http://` `open` quotes the URL + `as json` (an endpoint /
                    // URL has no extension to infer the codec from); a file `open`
                    // keeps the bare path (extension picks the codec).
                    Codec::Jsonl if matches!(discovery, Discovery::Subscribe(_)) => {
                        format!("subscribe {path:?} as json{}", provenance.modifier())
                    }
                    Codec::Jsonl if is_http_url(path) => {
                        format!("open {path:?} as json{}", provenance.modifier())
                    }
                    Codec::Jsonl => format!("open {path}{}", provenance.modifier()),
                    // `ls "glob"` / `watch "glob"` — the path is the glob
                    // pattern; quote it so it re-lexes as one string token
                    // (reversible). Provenance is not applicable to a discovery
                    // source (it emits handles already). A pushed name
                    // pre-filter is an inert `#` comment (like CSV).
                    Codec::Discover { name_prefilter } => {
                        let verb = if discovery.is_unbounded() {
                            "watch"
                        } else {
                            "ls"
                        };
                        let mut s = format!("{verb} {path:?}");
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
            Op::ProjectExpr { items, views } => {
                let parts: Vec<String> = items
                    .iter()
                    .map(|(e, alias)| {
                        // A union-view definition item (§29.3, s2) renders as
                        // `col :str(W) :{ name@start..end … }`: the whole-view
                        // cast plus the sub-view block recovered from `views`.
                        // The declared width lives in `ViewDef` (the `Str` lane
                        // carries none), so it is re-appended here for round-trip.
                        if let Some(v) = views.iter().find(|v| &v.col == alias) {
                            if let Expr::Cast { expr, ty } = e {
                                if let Expr::Field {
                                    name,
                                    access: Access::Fast,
                                } = expr.as_ref()
                                {
                                    if name == alias {
                                        let width = match v.width {
                                            Some(w) => format!("({w})"),
                                            None => String::new(),
                                        };
                                        let subs: Vec<String> = v
                                            .subs
                                            .iter()
                                            .map(|s| format!("{}@{}..{}", s.name, s.start, s.end))
                                            .collect();
                                        return format!(
                                            "{name} :{ty}{width} :{{ {} }}",
                                            subs.join(" ")
                                        );
                                    }
                                }
                            }
                        }
                        // The `:` definition chain (§29.2) is the canonical
                        // spelling for plain select / rename / cast items.
                        // An alias that collides with a type word must not be
                        // emitted as `:alias` (it would re-parse as a cast),
                        // so it — like every computed item — keeps the
                        // parenthesized `(expr) as alias` form. `Arith`
                        // already self-parenthesizes; wrap anything that
                        // doesn't start with `(` (e.g. `case`, functions).
                        let chain = match e {
                            Expr::Field {
                                name,
                                access: Access::Fast,
                            } if name == alias => Some(name.clone()),
                            Expr::Field {
                                name,
                                access: Access::Fast,
                            } if !crate::expr::is_type_word(alias) => {
                                Some(format!("{name} :{alias}"))
                            }
                            Expr::Cast { expr, ty } => match expr.as_ref() {
                                Expr::Field {
                                    name,
                                    access: Access::Fast,
                                } if name == alias => Some(format!("{name} :{ty}")),
                                Expr::Field {
                                    name,
                                    access: Access::Fast,
                                } if !crate::expr::is_type_word(alias) => {
                                    Some(format!("{name} :{alias} :{ty}"))
                                }
                                _ => None,
                            },
                            _ => None,
                        };
                        chain.unwrap_or_else(|| {
                            // A computed item is `(expr) as alias`. Only `Arith`
                            // already renders as a self-contained `(…)`; anything
                            // else — including a `Cast` whose inner operand
                            // self-parenthesized (`($_.x ~ 'p'):int`, `(a + b):int`)
                            // — needs wrapping so the `(expr) as alias` form
                            // re-parses (the old `starts_with('(')` proxy mistook
                            // those leading-paren casts for fully wrapped exprs).
                            let s = e.to_string();
                            if matches!(e, Expr::Arith { .. }) {
                                format!("{s} as {alias}")
                            } else {
                                format!("({s}) as {alias}")
                            }
                        })
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
                            k.to_string()
                        }
                    })
                    .collect();
                format!("sort {}", parts.join(" "))
            }
            Op::Distinct { keys } => {
                if keys.is_empty() {
                    "distinct".to_string()
                } else {
                    let parts: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
                    format!("distinct {}", parts.join(" "))
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
            Op::Explode { col } => format!("explode {col}"),
            Op::Sessionize { ts, gap, by } => {
                let mut s = format!("sessionize {ts} gap \"{gap}\"");
                if !by.is_empty() {
                    s.push_str(&format!(" by {}", by.join(" ")));
                }
                s
            }
            Op::Shift {
                col,
                kind,
                n,
                by,
                out,
            } => {
                // `lag`/`pct_change` always print N; `diff` omits the default 1.
                let mut s = format!("shift {col} {}", kind.as_str());
                if *kind != ShiftKind::Diff || *n != 1 {
                    s.push_str(&format!(" {n}"));
                }
                if !by.is_empty() {
                    s.push_str(&format!(" by {}", by.join(" ")));
                }
                s.push_str(&format!(" as {out}"));
                s
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
                let key_parts: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
                let mut s = format!("|# {}", key_parts.join(" "));
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
            Op::AsofJoin { by, ts, tolerance } => {
                // `& [on k…] asof ts [within "dur"]` — the amp is the plain `&`
                // (as-of is a left-outer enrichment; a kind selector is a later
                // slice), the by-keys ride the `on` clause.
                let mut s = String::from("&");
                if !by.is_empty() {
                    s.push_str(&format!(" on {}", by.join(" ")));
                }
                s.push_str(&format!(" asof {ts}"));
                if let Some(t) = tolerance {
                    s.push_str(&format!(" within \"{t}\""));
                }
                s
            }
            Op::SinkPrint => "print".to_string(),
            // The v1 `save` forms restore byte-identically: codec/route carry
            // exactly what the parser desugared from (§28.8 reversibility).
            // The partitioned route renders its canonical form: quoted
            // template + the same format-modifier rules; `by` is emitted only
            // for a plain path (a template's placeholders derive the keys, so
            // re-parsing restores the identical IR without the clause).
            Op::Sink {
                route:
                    Route::Template {
                        template, by, flat, ..
                    },
                codec,
                ..
            } => {
                let mut s = format!("save \"{template}\"");
                let modifier = match codec {
                    SinkCodec::Csv { delim } => delim_modifier_for(template, *delim),
                    SinkCodec::Jsonl => {
                        let lower = template.to_ascii_lowercase();
                        (!lower.ends_with(".jsonl") && !lower.ends_with(".ndjson"))
                            .then(|| "as jsonl".to_string())
                    }
                    SinkCodec::Json => (!template.to_ascii_lowercase().ends_with(".json"))
                        .then(|| "as json".to_string()),
                };
                if let Some(m) = modifier {
                    s.push_str(&format!(" {m}"));
                }
                // `by` is derived (and omitted) only when the template really
                // has key placeholders — judged on the parsed segments, never
                // on a raw '{' (a literal `{{x}}` template must keep its `by`
                // or re-parsing would silently become a fixed save).
                let templated = parse_route_template(template)
                    .map(|segs| {
                        segs.iter()
                            .any(|g| matches!(g, RouteSeg::Key(_) | RouteSeg::Raw(_)))
                    })
                    .unwrap_or(false);
                if !templated {
                    s.push_str(&format!(" by {}", by.join(" ")));
                }
                if *flat {
                    s.push_str(" as flat");
                }
                s
            }
            Op::Sink {
                route: Route::Fixed(path),
                codec,
                ..
            } => {
                match codec {
                    SinkCodec::Csv { delim } => match delim_modifier_for(path, *delim) {
                        Some(m) => format!("save {path} {m}"),
                        None => format!("save {path}"),
                    },
                    SinkCodec::Jsonl => {
                        // `.jsonl`/`.ndjson` paths imply jsonl; else be explicit.
                        let lower = path.to_ascii_lowercase();
                        if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
                            format!("save {path}")
                        } else {
                            format!("save {path} as jsonl")
                        }
                    }
                    SinkCodec::Json => {
                        // A `.json` path implies a JSON array; else be explicit.
                        if path.to_ascii_lowercase().ends_with(".json") {
                            format!("save {path}")
                        } else {
                            format!("save {path} as json")
                        }
                    }
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

    /// Does any node's expression contain a regex test (`~` / `regexp()`)?
    /// The runtime consults this before running: a build without the `regex`
    /// feature refuses the plan explicitly (never-silent, §29.5-6 s4) instead
    /// of evaluating every test to false. Mirrors `Op::bind_holes`'s list of
    /// expression-carrying ops.
    pub fn uses_regexp(&self) -> bool {
        self.nodes.iter().any(|n| match &n.op {
            Op::Filter { pred } | Op::Validate { pred, .. } => pred.uses_regexp(),
            Op::ProjectExpr { items, .. } => items.iter().any(|(e, _)| e.uses_regexp()),
            Op::FilterProject { preds, .. } => preds.iter().any(Expr::uses_regexp),
            Op::Sink {
                route: Route::Template { exprs, .. },
                ..
            } => exprs.iter().any(Expr::uses_regexp),
            _ => false,
        })
    }

    /// Does the plan contain an **unbounded** source (`watch`, §28.12)? The
    /// runtime consults this before running: a build without the `unbounded`
    /// feature refuses the plan explicitly (never-silent, the `regex`/`gzip`
    /// shape), and the bounded-only execution strategies (parallel partition /
    /// byte-range / group merge) step aside — an unbounded flow runs on the
    /// serial streaming loop.
    pub fn uses_unbounded(&self) -> bool {
        self.nodes.iter().any(|n| match &n.op {
            Op::Source { discovery, .. } => discovery.is_unbounded(),
            _ => false,
        })
    }

    /// Does the plan contain the unbounded **file-`watch`** source specifically
    /// (gated by `unbounded` / `notify`), as opposed to the network `subscribe`
    /// (gated by `net`)? The two unbounded sources are feature-gated apart, so the
    /// runtime's pre-run feature check consults each separately.
    pub fn uses_watch(&self) -> bool {
        self.nodes.iter().any(|n| match &n.op {
            Op::Source { discovery, .. } => discovery.is_watch(),
            _ => false,
        })
    }

    /// Does the plan contain a **networked** source — `subscribe "tcp://…"`, or an
    /// `open`/`read` of an `http://` URL (§33)? A build without the `net` feature
    /// refuses such a plan pre-run (never-silent, the `regex`/`unbounded` shape).
    pub fn uses_net(&self) -> bool {
        self.nodes.iter().any(|n| match &n.op {
            Op::Source { discovery, .. } => discovery.is_net(),
            _ => false,
        })
    }

    /// Does any source read Parquet? Drives the runtime's `parquet` feature
    /// gate: a feature-less build refuses the plan pre-run (never-silent).
    pub fn uses_parquet(&self) -> bool {
        self.nodes.iter().any(|n| {
            matches!(
                &n.op,
                Op::Source {
                    codec: Codec::Parquet,
                    ..
                }
            )
        })
    }

    /// The **boundedness-derived determinism tag** (§0.14 / §28.12 ratified
    /// #149 ③): `tag[id]` is true iff `id` is an unbounded source or is
    /// (transitively) downstream of one. Derived from the discovery's
    /// boundedness on demand — never stored, so it cannot go stale and adds
    /// nothing to serialization (`to_source` stays reversible for free). The
    /// optimizer and the parallel executor must not re-order, re-combine or
    /// rewrite nodes inside this set; byte-identity is asserted only on the
    /// bounded complement.
    pub fn unbounded_nodes(&self) -> Vec<bool> {
        let mut tag = vec![false; self.nodes.len()];
        let mut stack: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|n| matches!(&n.op, Op::Source { discovery, .. } if discovery.is_unbounded()))
            .map(|n| n.id)
            .collect();
        while let Some(id) = stack.pop() {
            if tag[id] {
                continue;
            }
            tag[id] = true;
            stack.extend(self.outputs_of(id));
        }
        tag
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
                Op::AsofJoin { by, ts, tolerance } => {
                    // `Label: Left & Right [on k…] asof ts [within "dur"] ;`
                    let names = self.input_labels(&inputs).join(" & ");
                    let mut clause = String::new();
                    if !by.is_empty() {
                        clause.push_str(&format!("on {} ", by.join(" ")));
                    }
                    clause.push_str(&format!("asof {ts}"));
                    if let Some(t) = tolerance {
                        clause.push_str(&format!(" within \"{t}\""));
                    }
                    let _ = writeln!(out, "{label}:\n    {names} {clause}\n;");
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
            // A chain whose ROOT is a fan-in node (a merge/join scope with
            // downstream stages, #186) renders the binary head `A + B` /
            // `A &kind B on k` referencing both inputs by label — the generic
            // `to_src_line` would emit a headless `+ merge` / `& on k`, which
            // does not re-parse and orphans the second input.
            if i == 0 {
                let head = match &self.nodes[nid].op {
                    Op::Merge => Some(self.input_labels(&self.inputs_of(nid)).join(" + ")),
                    Op::Join {
                        left_keys,
                        right_keys,
                        kind,
                    } => {
                        let sep = format!(" {} ", kind.amp());
                        let names = self.input_labels(&self.inputs_of(nid)).join(&sep);
                        Some(format!("{names} {}", join_on_clause(left_keys, right_keys)))
                    }
                    _ => None,
                };
                if let Some(head) = head {
                    let _ = writeln!(out, "{pad}{head}");
                    for h in &self.nodes[nid].hooks {
                        self.write_hook(out, h);
                    }
                    i += 1;
                    continue;
                }
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
            // §29.5-6 s4: a contiguous run of ≥2 row contracts renders as one
            // `|! { pred disp; … }` bundle — the canonical multi-validate
            // spelling (order preserved; re-parses to the same Validate chain).
            // The run only absorbs *plain* nodes: any hook, apply-site, branch
            // fan-out or leading comment past the first node breaks it so
            // nothing is repositioned or lost.
            if matches!(self.nodes[nid].op, Op::Validate { .. })
                && self.nodes[nid].hooks.is_empty()
                && self.branch_children_of(nid).is_empty()
            {
                let mut j = i + 1;
                while j < chain.len() {
                    let m = chain[j];
                    let plain = matches!(self.nodes[m].op, Op::Validate { .. })
                        && self.nodes[m].applied_from.is_none()
                        && self.nodes[m].leading_comments.is_empty()
                        && self.nodes[m].hooks.is_empty()
                        && self.branch_children_of(m).is_empty();
                    if !plain {
                        break;
                    }
                    j += 1;
                }
                if j > i + 1 {
                    let entries: Vec<String> = chain[i..j]
                        .iter()
                        .map(|&v| match &self.nodes[v].op {
                            Op::Validate { pred, disposition } => {
                                format!("{pred} {}", disposition.as_str())
                            }
                            _ => unreachable!("run contains only Validate nodes"),
                        })
                        .collect();
                    let _ = writeln!(out, "{pad}|! {{ {} }}", entries.join("; "));
                    i = j;
                    continue;
                }
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
        // A source-rooted chain has no input → independent scope. A fan-in root
        // (Merge/Join, ≥2 inputs) is NOT a branch child either — a scope whose
        // chain *starts* at a merge/join is an independent `M: A + B …` scope;
        // classifying it under its first input rendered it as an inline
        // `-> M: + merge` branch, which drops the second input and does not
        // re-parse (#186). Only a single-input root continues a parent fan-out.
        let inputs = self.inputs_of(root);
        if inputs.len() != 1 {
            return None;
        }
        Some(inputs[0])
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
