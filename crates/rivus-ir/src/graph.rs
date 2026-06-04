//! The DAG IR.
//!
//! Rivus is DAG-native (Master principle #3): even a "linear" pipeline is a
//! degenerate DAG. Nodes are flow scopes / transforms / events; edges are
//! streams (or error side-channels). The graph is the single source of truth
//! that the optimizer rewrites and that [`PlanGraph::to_source`] regenerates
//! back into readable Rivus source (Master principle #5: IR reversibility).

use crate::expr::{Access, CmpOp, Expr};
use rivus_core::{DataType, Mode, Severity};
use std::collections::HashMap;
use std::fmt::Write as _;

pub type NodeId = usize;

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
    /// Number of distinct non-empty values (`nunique` is an accepted alias).
    CountDistinct,
    /// First non-empty value seen in the group (source order).
    First,
    /// Last non-empty value seen in the group (source order).
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

/// A flow operator. One enum spanning sources, transforms, fan-out/in and
/// sinks — because in Rivus they are all just nodes in the same graph.
#[derive(Debug, Clone)]
pub enum Op {
    /// `open path.csv`. `projection`, when set by the optimizer
    /// (`project_pushdown`), restricts which columns the reader builds — unused
    /// columns are never parsed or allocated. `prefilter`, set by
    /// `filter_pushdown`, lets the reader skip *building* rows whose numeric
    /// `(column, op, rhs)` conjunction is definitely false — a conservative
    /// pre-pass; the downstream `FilterProject` remains authoritative.
    /// `str_prefilter` carries required literal substrings (from `contains` /
    /// `starts_with` / `ends_with` / `==` / `like`-literal predicates): the
    /// reader skips any *raw line* lacking the substring before splitting it
    /// (a ripgrep-style superset filter, so the result is unchanged).
    OpenCsv {
        path: String,
        projection: Option<Vec<String>>,
        prefilter: Vec<(String, CmpOp, f64)>,
        str_prefilter: Vec<String>,
        /// Whether the first line is a header. `false` (`open f.csv noheader`)
        /// treats every line as data and names columns `c0, c1, …`.
        header: bool,
        /// Declared column schema `(name[:type] ...)`, set by
        /// `open f.csv (id:int name:str age:int)`. When present it names the
        /// columns positionally (overriding the header / `c0…`) and, where a
        /// type is given, fixes that column's lane instead of inferring it.
        declared: Option<Vec<(String, Option<DataType>)>>,
        /// Strptime-style parse formats for `:datetime("fmt")` columns, keyed by
        /// column name (design 23). Only columns given an *explicit* format
        /// appear here; a bare `:datetime` (auto-infer) has no entry. Carried
        /// alongside `declared` because the format is a read-time parse hint, not
        /// part of the (Copy) lane tag `DataType::DateTime`.
        dt_formats: Vec<(String, String)>,
        /// Field delimiter byte. `b','` for CSV (the default); `b'\t'` for a
        /// `.tsv`/`.tab` file or `open f.x as tsv`. Std-only — the reader just
        /// splits on a different byte.
        delim: u8,
    },
    /// `readbin path [le|be] [packed|aligned] (name:type ...)` — fixed-width
    /// binary records (a C struct dump). `endian` selects byte order;
    /// `c_align` true uses C `repr(C)` natural-alignment padding, false packs.
    OpenBinary {
        path: String,
        fields: Vec<(String, BinType)>,
        endian: Endian,
        c_align: bool,
    },
    /// `open path.jsonl` — JSON Lines (one flat JSON object per line).
    OpenJsonl { path: String },
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
    pub fn kind_str(&self) -> &'static str {
        match self {
            Op::OpenCsv { .. } => "open",
            Op::OpenBinary { .. } => "readbin",
            Op::OpenJsonl { .. } => "open",
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

    /// Render this op as the pipeline fragment that produced it.
    fn to_src_line(&self) -> String {
        match self {
            Op::OpenCsv {
                path,
                projection,
                prefilter,
                str_prefilter,
                header,
                declared,
                dt_formats,
                delim,
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
                            // (`datetime` or `datetime("fmt")`), not the DataType
                            // Display, so an explicit parse format round-trips.
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
            Op::OpenBinary {
                path,
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
                format!("readbin {path} {mods}({})", cols.join(" "))
            }
            Op::OpenJsonl { path } => format!("open {path}"),
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

    pub fn add_node(&mut self, op: Op) -> NodeId {
        let id = self.nodes.len();
        self.nodes.push(Node {
            id,
            label: None,
            op,
            hooks: Vec::new(),
            leading_comments: Vec::new(),
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
        for &nid in &chain {
            // Inert comment trivia preceding this step (§25.7), re-emitted in
            // source order at the step's indentation so `rivus fmt` round-trips.
            for c in &self.nodes[nid].leading_comments {
                let _ = writeln!(out, "{pad}{c}");
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
