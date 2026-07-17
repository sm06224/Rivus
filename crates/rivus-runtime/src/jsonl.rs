//! Minimal JSON Lines (NDJSON) reader: one JSON object per line.
//!
//! Scope (continue-first):
//! - Each non-empty line must be a JSON object `{ "k": value, ... }`. Lines that
//!   aren't are counted as `bad_rows` and skipped (never panics).
//! - Scalar values (string / number / bool / null) map onto the columnar lanes.
//! - **Nested values are first-class (§32 s3b):** a `{...}` value becomes a
//!   `Struct` lane, a `[...]` value becomes a `List` lane, both inferred
//!   recursively and fixed globally at schema time (so every chunk / parallel
//!   range builds byte-identically). There is no degrade-to-string: a JSON
//!   `null` / missing key on a nested column is a *typed null* (validity = 0),
//!   never a silent `""`. Only a genuinely heterogeneous column (a key seen as
//!   both scalar and nested, or both object and array) falls back to the string
//!   lane, rendering each cell as its JSON text — the one documented fallback,
//!   which clean data never hits.
//! - The column set and order come from the first valid object; later objects
//!   fill by key (missing key → null/default, extra keys ignored). The same rule
//!   applies recursively to a struct's child fields.
//!
//! A flat, allocation-conscious parser — no external dependencies (the shipped
//! runtime stays std-only).

use crate::transport::FileTransport;
use rivus_core::{
    Column, ColumnData, DataType, Field, ListColumn, Schema, StrColumn, StructColumn, Validity,
};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};

pub struct JsonlData {
    pub schema: Schema,
    pub columns: Vec<Column>,
    pub bad_rows: usize,
}

#[derive(Clone)]
enum JVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    /// A nested JSON object, parsed structurally (§32 s3b).
    Obj(Vec<(String, JVal)>),
    /// A nested JSON array, parsed structurally (§32 s3b).
    Arr(Vec<JVal>),
}

/// A recursively-inferred JSON column type (§32 s3b): either a scalar lane, or a
/// nested `Struct`/`List` whose shape is fixed globally in pass 1 so every chunk
/// / parallel range builds against the same shape (byte-identity).
#[derive(Clone, Debug)]
pub enum JType {
    Scalar(DataType),
    /// Named child types, parallel arrays (`names[i]` ↔ `children[i]`).
    Struct {
        names: Vec<String>,
        children: Vec<JType>,
    },
    /// The element type of a list lane.
    List(Box<JType>),
}

pub fn parse(text: &str) -> Result<JsonlData, String> {
    let mut names: Vec<String> = Vec::new();
    let mut started = false;
    let mut rows: Vec<Vec<(String, JVal)>> = Vec::new();
    let mut bad_rows = 0;

    // A document beginning with `[` is a JSON array of objects (e.g. an API
    // response); otherwise it is JSON Lines (one object per line).
    if text.trim_start().starts_with('[') {
        collect_array(text, &mut names, &mut started, &mut rows, &mut bad_rows);
    } else {
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            match parse_object(line) {
                Some(obj) => {
                    if !started {
                        names = obj.iter().map(|(k, _)| k.clone()).collect();
                        started = true;
                    }
                    rows.push(obj);
                }
                None => bad_rows += 1,
            }
        }
    }

    if names.is_empty() {
        return Err("JSON has no valid objects".to_string());
    }

    // Gather per-column values (by key), then infer a (possibly nested) type and
    // build. Inference uses the same recursive accumulator as the streaming
    // reader, so the two paths derive byte-identical schemas.
    let nrows = rows.len();
    let mut columns = Vec::with_capacity(names.len());
    let mut fields = Vec::with_capacity(names.len());
    for name in &names {
        let mut vals: Vec<JVal> = Vec::with_capacity(nrows);
        for obj in &rows {
            let v = obj.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone());
            vals.push(v.unwrap_or(JVal::Null));
        }
        let mut inf = Infer::new();
        for v in &vals {
            inf.observe(v);
        }
        let jt = inf.resolve();
        columns.push(build_column(&jt, &vals));
        fields.push(jtype_to_field(name.clone(), &jt));
    }

    Ok(JsonlData {
        schema: Schema::new(fields),
        columns,
        bad_rows,
    })
}

/// Collect objects from a top-level JSON array `[ {..}, {..}, ... ]` (which may
/// span multiple lines). Non-object elements are counted as bad rows and
/// skipped (continue-first).
fn collect_array(
    text: &str,
    names: &mut Vec<String>,
    started: &mut bool,
    rows: &mut Vec<Vec<(String, JVal)>>,
    bad_rows: &mut usize,
) {
    let b = text.as_bytes();
    let mut i = 0;
    while i < b.len() && b[i] != b'[' {
        i += 1;
    }
    i += 1; // past '['
    loop {
        skip_ws(b, &mut i);
        match b.get(i) {
            None | Some(b']') => break,
            Some(b',') => i += 1,
            Some(b'{') => {
                let start = i;
                // Capture the balanced object, then parse it.
                if skip_balanced(b, &mut i, b'{', b'}') {
                    if let Some(obj) = parse_object(&text[start..i]) {
                        if !*started {
                            *names = obj.iter().map(|(k, _)| k.clone()).collect();
                            *started = true;
                        }
                        rows.push(obj);
                    } else {
                        *bad_rows += 1;
                    }
                } else {
                    break; // unterminated
                }
            }
            Some(_) => {
                // A non-object element: count it and skip past it.
                *bad_rows += 1;
                if parse_value(b, &mut i).is_none() {
                    break;
                }
            }
        }
    }
}

// --------------------------------------------------------------- type inference

/// Streaming, recursive per-key type accumulator (§32 s3b). One value at a time
/// so the reader needn't buffer a whole column; `resolve` is deterministic over
/// what was observed and identical regardless of partitioning.
#[derive(Clone)]
struct Infer {
    scalar: Flags,
    saw_scalar: bool,
    structs: Option<StructInfer>,
    list: Option<Box<Infer>>,
}

#[derive(Clone)]
struct StructInfer {
    /// Child field order, fixed by the first object seen for this column.
    names: Vec<String>,
    children: Vec<Infer>,
}

impl Infer {
    fn new() -> Self {
        Infer {
            scalar: Flags::new(),
            saw_scalar: false,
            structs: None,
            list: None,
        }
    }

    fn observe(&mut self, v: &JVal) {
        match v {
            // A `null` (or missing key, assembled as `JVal::Null`) does not force
            // a lane — it becomes a typed null at build time (validity = 0).
            JVal::Null => {}
            JVal::Bool(_) | JVal::Int(_) | JVal::Float(_) | JVal::Str(_) => {
                self.saw_scalar = true;
                self.scalar.observe(v);
            }
            JVal::Obj(fields) => {
                let st = self.structs.get_or_insert_with(|| StructInfer {
                    names: fields.iter().map(|(k, _)| k.clone()).collect(),
                    children: fields.iter().map(|_| Infer::new()).collect(),
                });
                for (k, val) in fields {
                    if let Some(idx) = st.names.iter().position(|n| n == k) {
                        st.children[idx].observe(val);
                    }
                    // Extra keys (absent from the first-seen object) are ignored,
                    // mirroring the flat "column set comes from the first object".
                }
            }
            JVal::Arr(elems) => {
                let el = self.list.get_or_insert_with(|| Box::new(Infer::new()));
                for e in elems {
                    el.observe(e);
                }
            }
        }
    }

    fn resolve(&self) -> JType {
        let cats = self.structs.is_some() as u8 + self.list.is_some() as u8 + self.saw_scalar as u8;
        if cats > 1 {
            // Heterogeneous column (scalar mixed with nested, or object+array):
            // no single typed lane fits → string lane, each cell rendered as its
            // JSON text. The one documented fallback; clean data never hits it.
            return JType::Scalar(DataType::Str);
        }
        if let Some(st) = &self.structs {
            JType::Struct {
                names: st.names.clone(),
                children: st.children.iter().map(|c| c.resolve()).collect(),
            }
        } else if let Some(el) = &self.list {
            JType::List(Box::new(el.resolve()))
        } else {
            JType::Scalar(self.scalar.resolve())
        }
    }

    /// Fold a LATER range's observations into this (earlier-range) inference —
    /// the parallel-inference merge. Merging per-range `Infer`s **in range
    /// order** reproduces the sequential scan exactly:
    /// - scalar flags are commutative (`Flags::merge`);
    /// - a struct's child-name order comes from the first object seen for the
    ///   column — the earliest range that saw one (range-order fold ⇒ the same
    ///   object the sequential scan would have hit first); a later range's
    ///   extra child keys are ignored, mirroring `observe`;
    /// - list element inference merges recursively.
    fn merge(&mut self, other: &Infer) {
        self.saw_scalar |= other.saw_scalar;
        self.scalar.merge(&other.scalar);
        match (&mut self.structs, &other.structs) {
            (Some(a), Some(b)) => {
                for (k, child) in b.names.iter().zip(&b.children) {
                    if let Some(idx) = a.names.iter().position(|n| n == k) {
                        a.children[idx].merge(child);
                    }
                }
            }
            (None, Some(b)) => self.structs = Some(b.clone()),
            _ => {}
        }
        match (&mut self.list, &other.list) {
            (Some(a), Some(b)) => a.merge(b),
            (None, Some(b)) => self.list = Some(b.clone()),
            _ => {}
        }
    }
}

/// A recursive [`Field`] (with nested detail) for an inferred [`JType`].
fn jtype_to_field(name: String, jt: &JType) -> Field {
    match jt {
        JType::Scalar(dt) => Field::new(name, *dt),
        JType::Struct { names, children } => {
            let kids = names
                .iter()
                .zip(children)
                .map(|(n, c)| jtype_to_field(n.clone(), c))
                .collect();
            Field::struct_(name, kids)
        }
        // A list's element field is conventionally named `item` (§32 s3).
        JType::List(elem) => Field::list(name, jtype_to_field("item".to_string(), elem)),
    }
}

/// Build one (possibly nested) column for `jt` over `vals` (§32 s3b). A struct
/// row is valid iff the cell is an object; a list row iff the cell is an array;
/// any other cell is a typed null (validity = 0) — never silent. Children /
/// elements recurse, so the null model (§26) recurses too.
fn build_column(jt: &JType, vals: &[JVal]) -> Column {
    match jt {
        JType::Scalar(dt) => build_scalar(*dt, vals),
        JType::Struct { names, children } => {
            let valid: Vec<bool> = vals.iter().map(|v| matches!(v, JVal::Obj(_))).collect();
            let mut cols = Vec::with_capacity(children.len());
            for (ci, child_jt) in children.iter().enumerate() {
                let key = &names[ci];
                let child_vals: Vec<JVal> = vals
                    .iter()
                    .map(|v| match v {
                        JVal::Obj(fs) => fs
                            .iter()
                            .find(|(k, _)| k == key)
                            .map(|(_, x)| x.clone())
                            .unwrap_or(JVal::Null),
                        _ => JVal::Null,
                    })
                    .collect();
                cols.push(build_column(child_jt, &child_vals));
            }
            Column::new(
                ColumnData::Struct(StructColumn {
                    names: names.clone(),
                    columns: cols,
                    len: vals.len(),
                }),
                Validity::from_bits(&valid),
            )
        }
        JType::List(elem) => {
            let valid: Vec<bool> = vals.iter().map(|v| matches!(v, JVal::Arr(_))).collect();
            let mut offsets = Vec::with_capacity(vals.len() + 1);
            offsets.push(0i32);
            let mut flat: Vec<JVal> = Vec::new();
            let mut acc = 0i32;
            for v in vals {
                if let JVal::Arr(elems) = v {
                    flat.extend(elems.iter().cloned());
                    acc += elems.len() as i32;
                }
                offsets.push(acc);
            }
            let child = build_column(elem, &flat);
            Column::new(
                ColumnData::List(ListColumn {
                    offsets,
                    child: Box::new(child),
                }),
                Validity::from_bits(&valid),
            )
        }
    }
}

/// Build one **scalar** column, tracking **validity** (design 26 §26.3): a JSON
/// `null` — and a **missing key** (assembled as `JVal::Null`) — becomes a `null`
/// (validity = 0), never a silent `0`/`""`. A JSON empty string `""` stays a
/// real empty string (validity = 1). A nested cell only reaches here when the
/// column is the heterogeneous string fallback; it is rendered as JSON text.
fn build_scalar(dtype: DataType, vals: &[JVal]) -> Column {
    let mut valid = Vec::with_capacity(vals.len());
    let data = match dtype {
        DataType::I64 => ColumnData::I64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => {
                        valid.push(true);
                        *i
                    }
                    _ => {
                        valid.push(false);
                        0
                    }
                })
                .collect(),
        ),
        DataType::F64 => ColumnData::F64(
            vals.iter()
                .map(|v| match v {
                    JVal::Int(i) => {
                        valid.push(true);
                        *i as f64
                    }
                    JVal::Float(f) => {
                        valid.push(true);
                        *f
                    }
                    _ => {
                        valid.push(false);
                        0.0
                    }
                })
                .collect(),
        ),
        DataType::Bool => ColumnData::Bool(
            vals.iter()
                .map(|v| match v {
                    JVal::Bool(b) => {
                        valid.push(true);
                        *b
                    }
                    _ => {
                        valid.push(false);
                        false
                    }
                })
                .collect(),
        ),
        _ => {
            let mut s = StrColumn::with_capacity(vals.len(), vals.len() * 8);
            for v in vals {
                match v {
                    // JSON `null` / missing key → null (validity = 0). A real
                    // empty string arrives as `JVal::Str("")` and stays valid.
                    JVal::Null => {
                        s.push("");
                        valid.push(false);
                    }
                    JVal::Bool(b) => {
                        s.push(if *b { "true" } else { "false" });
                        valid.push(true);
                    }
                    JVal::Int(i) => {
                        s.push(&i.to_string());
                        valid.push(true);
                    }
                    JVal::Float(f) => {
                        s.push(&f.to_string());
                        valid.push(true);
                    }
                    JVal::Str(x) => {
                        s.push(x);
                        valid.push(true);
                    }
                    // Heterogeneous fallback only: render the nested value as its
                    // JSON text so nothing is silently dropped.
                    JVal::Obj(_) | JVal::Arr(_) => {
                        let mut t = String::new();
                        jval_json(v, &mut t);
                        s.push(&t);
                        valid.push(true);
                    }
                }
            }
            ColumnData::Str(s)
        }
    };
    Column::new(data, Validity::from_bits(&valid))
}

/// Serialize a [`JVal`] back to compact JSON text — used only for the
/// heterogeneous string fallback (so a nested cell is never silently dropped).
fn jval_json(v: &JVal, out: &mut String) {
    match v {
        JVal::Null => out.push_str("null"),
        JVal::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        JVal::Int(i) => out.push_str(&i.to_string()),
        JVal::Float(f) => out.push_str(&f.to_string()),
        JVal::Str(s) => json_string(out, s),
        JVal::Obj(fs) => {
            out.push('{');
            for (i, (k, val)) in fs.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_string(out, k);
                out.push(':');
                jval_json(val, out);
            }
            out.push('}');
        }
        JVal::Arr(es) => {
            out.push('[');
            for (i, e) in es.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                jval_json(e, out);
            }
            out.push(']');
        }
    }
}

/// Append `s` as a quoted, escaped JSON string.
fn json_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

// ------------------------------------------------------------- streaming reader

/// Streaming scalar-type flags — the scalar leaf of [`Infer`]. Accumulated one
/// value at a time; resolves to the narrowest scalar lane that fits.
#[derive(Clone)]
struct Flags {
    any: bool,
    all_int: bool,
    all_num: bool,
    all_bool: bool,
}

impl Flags {
    fn new() -> Self {
        Flags {
            any: false,
            all_int: true,
            all_num: true,
            all_bool: true,
        }
    }
    fn observe(&mut self, v: &JVal) {
        match v {
            JVal::Null => {}
            JVal::Int(_) => {
                self.any = true;
                self.all_bool = false;
            }
            JVal::Float(_) => {
                self.any = true;
                self.all_int = false;
                self.all_bool = false;
            }
            JVal::Bool(_) => {
                self.any = true;
                self.all_int = false;
                self.all_num = false;
            }
            // A scalar `Flags` only ever observes scalars; a nested value (only
            // possible in the heterogeneous fallback) forces the string lane.
            JVal::Str(_) | JVal::Obj(_) | JVal::Arr(_) => {
                self.any = true;
                self.all_int = false;
                self.all_num = false;
                self.all_bool = false;
            }
        }
    }
    /// Class-only observers for the allocation-free inference scanner — each
    /// mirrors the corresponding `observe` arm exactly (same flag updates), so
    /// scanning classifies identically to parsing.
    fn observe_int(&mut self) {
        self.any = true;
        self.all_bool = false;
    }
    fn observe_float(&mut self) {
        self.any = true;
        self.all_int = false;
        self.all_bool = false;
    }
    fn observe_bool(&mut self) {
        self.any = true;
        self.all_int = false;
        self.all_num = false;
    }
    fn observe_str(&mut self) {
        self.any = true;
        self.all_int = false;
        self.all_num = false;
        self.all_bool = false;
    }

    fn resolve(&self) -> DataType {
        if !self.any {
            DataType::Str
        } else if self.all_int {
            DataType::I64
        } else if self.all_num {
            DataType::F64
        } else if self.all_bool {
            DataType::Bool
        } else {
            DataType::Str
        }
    }
    /// Fold another range's observations in. Pure conjunction/disjunction, so
    /// merging per-range flags in any order equals observing every value
    /// sequentially — the parallel-inference merge is byte-identical.
    fn merge(&mut self, o: &Flags) {
        self.any |= o.any;
        self.all_int &= o.all_int;
        self.all_num &= o.all_num;
        self.all_bool &= o.all_bool;
    }
}

/// Does the file begin with a top-level JSON array (`[ … ]`)? Such a document is
/// not line-oriented (an element can span lines), so it can't be streamed or
/// byte-range split — the caller falls back to the whole-file [`parse`].
pub fn is_json_array(path: &str) -> bool {
    let Ok(mut r) = FileTransport::open(path) else {
        return false;
    };
    let mut byte = [0u8; 1];
    loop {
        match r.read(&mut byte) {
            Ok(0) => return false,
            Ok(_) => {
                let c = byte[0];
                if matches!(c, b' ' | b'\t' | b'\r' | b'\n') {
                    continue;
                }
                return c == b'[';
            }
            Err(_) => return false,
        }
    }
}

/// One range's pass-1 result: the first-seen keys with their inference state,
/// the range's first valid object's key list (`None` if the range saw no valid
/// object), and its malformed-line count.
type RangeInfer = (Vec<(String, Infer)>, Option<Vec<String>>, usize);

/// One byte range's inference (the parallel pass-1 worker): every first-seen
/// key in the range with its [`Infer`], the range's **first valid object's key
/// list** (the global column order comes from the earliest started range = the
/// file's first valid object), and the malformed-line count. Observes ALL keys
/// it encounters — a range may start inside rows that lack a global column, so
/// restricting to the range's first object (as the sequential scan does with
/// the file's first object) would drop observations the sequential scan makes.
/// The merge in [`plan_parallel`] restricts to the global names, so extra keys
/// are discarded there, exactly as the sequential scan never observes them.
fn infer_range(path: &str, start: u64, end: u64) -> Result<RangeInfer, String> {
    let mut r = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    r.seek(SeekFrom::Start(start)).map_err(|e| e.to_string())?;
    let mut seen: Vec<(String, Infer)> = Vec::new();
    // `Some` once the range saw a valid object — even an empty `{}` claims the
    // column order (matching the sequential scan's `started` flag exactly).
    let mut first_names: Option<Vec<String>> = None;
    let mut bad_rows = 0usize;
    // Block walk, like the CSV `infer_range` (slice 17): lines observed in
    // place inside the buffered block — one UTF-8 validation per block, no
    // per-line copy. The range end is newline-aligned, so capping each block
    // at `end − pos` always cuts on a line boundary.
    let mut pos = start;
    let mut carry: Vec<u8> = Vec::new();
    // Allocation-free scan (pass 1): observes classes directly into `seen`
    // without building a `JVal` — pinned byte-identical to the parser path
    // by `scanner_matches_parser_inference`.
    let observe = |l: &str,
                   seen: &mut Vec<(String, Infer)>,
                   first_names: &mut Option<Vec<String>>,
                   bad_rows: &mut usize| {
        if !l.trim().is_empty() && !scan_line_infer(l, seen, first_names) {
            *bad_rows += 1;
        }
    };
    loop {
        if pos >= end {
            break;
        }
        let buf = match r.fill_buf() {
            Ok(b) if !b.is_empty() => b,
            _ => {
                // EOF: a trailing unterminated line sits in the carry (only
                // possible when `end` is the file end); read errors observed
                // nothing more, exactly like the old `read_line` loop.
                if !carry.is_empty() {
                    let bytes = std::mem::take(&mut carry);
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        let l = s.trim_end_matches(['\n', '\r']);
                        observe(l, &mut seen, &mut first_names, &mut bad_rows);
                    }
                }
                break;
            }
        };
        let avail = usize::try_from(end - pos)
            .map(|lim| buf.len().min(lim))
            .unwrap_or(buf.len());
        let buf = &buf[..avail];

        if !carry.is_empty() {
            match buf.iter().position(|&b| b == b'\n') {
                None => {
                    carry.extend_from_slice(buf);
                    r.consume(avail);
                    pos += avail as u64;
                    continue;
                }
                Some(nl) => {
                    carry.extend_from_slice(&buf[..nl]);
                    let bytes = std::mem::take(&mut carry);
                    r.consume(nl + 1);
                    pos += (nl + 1) as u64;
                    match std::str::from_utf8(&bytes) {
                        Ok(s) => {
                            let l = s.trim_end_matches(['\n', '\r']);
                            observe(l, &mut seen, &mut first_names, &mut bad_rows);
                        }
                        Err(_) => break, // read_line would have errored here
                    }
                    continue;
                }
            }
        }

        let last_nl = match buf.iter().rposition(|&b| b == b'\n') {
            None => {
                carry.extend_from_slice(buf);
                r.consume(avail);
                pos += avail as u64;
                continue;
            }
            Some(p) => p,
        };
        let region = &buf[..=last_nl];
        let (text, stop_after) = match std::str::from_utf8(region) {
            Ok(t) => (t, false),
            Err(e) => match region[..e.valid_up_to()].iter().rposition(|&b| b == b'\n') {
                Some(c) => (
                    std::str::from_utf8(&region[..=c]).expect("valid prefix"),
                    true,
                ),
                None => break,
            },
        };
        let mut cur = 0usize;
        while cur < text.len() {
            let rest = &text[cur..];
            let nl = rest
                .as_bytes()
                .iter()
                .position(|&b| b == b'\n')
                .expect("region ends with newline");
            let raw = &rest[..nl];
            cur += nl + 1;
            let l = raw.trim_end_matches(['\n', '\r']);
            observe(l, &mut seen, &mut first_names, &mut bad_rows);
        }
        let walked = text.len();
        r.consume(walked);
        pos += walked as u64;
        if stop_after {
            break;
        }
    }
    Ok((seen, first_names, bad_rows))
}

/// Global type plan for a JSON-Lines file (pass 1): the column order from the
/// first valid object and each key's (possibly nested) type inferred over every
/// row, plus the malformed line count. Byte-identical to what [`parse`] derives.
fn infer_global(path: &str) -> Result<(Vec<String>, Vec<JType>, usize), String> {
    let mut r = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
    let mut names: Vec<String> = Vec::new();
    let mut infers: Vec<Infer> = Vec::new();
    let mut bad_rows = 0usize;
    let mut started = false;
    let mut line = String::new();
    loop {
        line.clear();
        match r.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }
        let l = line.trim_end_matches(['\n', '\r']);
        if l.trim().is_empty() {
            continue;
        }
        match parse_object(l) {
            Some(obj) => {
                if !started {
                    names = obj.iter().map(|(k, _)| k.clone()).collect();
                    infers = names.iter().map(|_| Infer::new()).collect();
                    started = true;
                }
                for (k, v) in &obj {
                    if let Some(i) = names.iter().position(|n| n == k) {
                        infers[i].observe(v);
                    }
                }
            }
            None => bad_rows += 1,
        }
    }
    if !started {
        return Err("JSON has no valid objects".to_string());
    }
    let jtypes = infers.iter().map(|f| f.resolve()).collect();
    Ok((names, jtypes, bad_rows))
}

/// Build a [`Schema`] from inferred column names + types (nested detail carried).
fn schema_from(names: &[String], jtypes: &[JType]) -> Schema {
    Schema::new(
        names
            .iter()
            .zip(jtypes)
            .map(|(n, t)| jtype_to_field(n.clone(), t))
            .collect(),
    )
}

/// A streaming JSON-Lines reader (bounded memory), two-pass like the CSV reader:
/// pass 1 ([`infer_global`]) fixes the schema, pass 2 ([`Self::next_columns`])
/// re-streams the file (or one byte range) yielding one chunk of typed columns at
/// a time. Byte-identical to the whole-file [`parse`] for line-oriented input.
pub struct JsonlChunker {
    reader: BufReader<File>,
    names: Vec<String>,
    jtypes: Vec<JType>,
    /// Key-order template for the fused fast scan (see [`RowTemplate`]).
    tmpl: RowTemplate,
    chunk_size: usize,
    line: String,
    /// Partial-line carry for the fused block-based reader (raw bytes — a
    /// multi-byte character may straddle a block; validated on completion).
    carry: Vec<u8>,
    eof: bool,
    pos: u64,
    limit: Option<u64>,
    pub bad_rows: usize,
}

impl JsonlChunker {
    /// Open `path` for whole-file streaming (serial, bounded memory).
    pub fn open(path: &str, chunk_size: usize) -> Result<(Schema, JsonlChunker), String> {
        let (names, jtypes, bad_rows) = infer_global(path)?;
        let schema = schema_from(&names, &jtypes);
        let reader = FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        Ok((
            schema,
            JsonlChunker {
                tmpl: RowTemplate::new(&names),
                reader,
                names,
                jtypes,
                chunk_size: chunk_size.max(1),
                line: String::new(),
                carry: Vec::new(),
                eof: false,
                pos: 0,
                limit: None,
                bad_rows,
            },
        ))
    }

    /// Open `path` for streaming one newline-aligned byte range `[start, end)`
    /// with a pre-inferred global schema — one parallel worker.
    pub fn for_range(
        path: &str,
        names: Vec<String>,
        jtypes: Vec<JType>,
        start: u64,
        end: u64,
        chunk_size: usize,
    ) -> Result<JsonlChunker, String> {
        let mut reader =
            FileTransport::open(path).map_err(|e| format!("cannot open '{path}': {e}"))?;
        reader
            .seek(SeekFrom::Start(start))
            .map_err(|e| e.to_string())?;
        Ok(JsonlChunker {
            tmpl: RowTemplate::new(&names),
            reader,
            names,
            jtypes,
            chunk_size: chunk_size.max(1),
            line: String::new(),
            carry: Vec::new(),
            eof: false,
            pos: start,
            limit: Some(end),
            bad_rows: 0,
        })
    }

    /// Yield up to `chunk_size` rows as typed columns, or `None` at the end of
    /// the file / byte range. Malformed lines are skipped (counted in pass 1).
    /// The fused flat-scalar decode (see the fused-decode header comment):
    /// one pass, values committed row-atomically from a reused scratch, so a
    /// malformed line contributes nothing (it was counted in pass 1).
    /// Block-based, like the CSV reader's `next_columns` (slice 13): lines are
    /// scanned **in place** inside the buffered block — one UTF-8 validation
    /// per block, no per-line copy — and the `ScVal` scratch (which borrows
    /// the line) is allocated once per block instead of once per row (the
    /// per-row `Vec` was ~10M heap allocations on the 10M standard). A worker
    /// byte range is newline-aligned, so capping each block at `limit − pos`
    /// always cuts on a line boundary. Only a block-straddling line is copied
    /// into the byte carry. Output is byte-identical to the `read_line` loop.
    fn next_columns_fused(&mut self) -> Option<Vec<Column>> {
        let dtypes: Vec<DataType> = self
            .jtypes
            .iter()
            .map(|t| match t {
                JType::Scalar(d) => *d,
                _ => unreachable!("fused path is flat-only"),
            })
            .collect();
        let mut builders: Vec<ColBuilder> = dtypes
            .iter()
            .map(|d| ColBuilder::new(*d, self.chunk_size))
            .collect();
        let mut got = 0usize;
        while got < self.chunk_size {
            if matches!(self.limit, Some(end) if self.pos >= end) {
                self.eof = true;
                break;
            }
            let buf = match self.reader.fill_buf() {
                Ok(b) if !b.is_empty() => b,
                Ok(_) => {
                    // EOF: a trailing unterminated line sits in the carry —
                    // decoded exactly as `read_line` returned it (JSONL trims
                    // trailing `\r`s with or without a newline). An
                    // invalid-UTF-8 tail is dropped, as a `read_line` error
                    // stopped the old loop.
                    self.eof = true;
                    if !self.carry.is_empty() {
                        let bytes = std::mem::take(&mut self.carry);
                        if let Ok(s) = std::str::from_utf8(&bytes) {
                            let l = s.trim_end_matches(['\n', '\r']);
                            if !l.trim().is_empty() {
                                let mut scratch: Vec<ScVal> = Vec::with_capacity(self.names.len());
                                if scan_row_fast(l, &self.tmpl, &self.names, &mut scratch) {
                                    for (b, v) in builders.iter_mut().zip(&scratch) {
                                        b.push(v);
                                    }
                                    got += 1;
                                }
                            }
                        }
                    }
                    break;
                }
                Err(_) => {
                    self.eof = true;
                    break;
                }
            };
            // Never look past the range end: it sits on a line boundary.
            let avail = match self.limit {
                Some(end) => usize::try_from(end - self.pos)
                    .map(|lim| buf.len().min(lim))
                    .unwrap_or(buf.len()),
                None => buf.len(),
            };
            let buf = &buf[..avail];

            // Complete a carried block-straddling line first (rare).
            if !self.carry.is_empty() {
                match buf.iter().position(|&b| b == b'\n') {
                    None => {
                        self.carry.extend_from_slice(buf);
                        self.reader.consume(avail);
                        self.pos += avail as u64;
                        continue;
                    }
                    Some(nl) => {
                        self.carry.extend_from_slice(&buf[..nl]);
                        let bytes = std::mem::take(&mut self.carry);
                        self.reader.consume(nl + 1);
                        self.pos += (nl + 1) as u64;
                        match std::str::from_utf8(&bytes) {
                            Ok(s) => {
                                let l = s.trim_end_matches(['\n', '\r']);
                                if !l.trim().is_empty() {
                                    let mut scratch: Vec<ScVal> =
                                        Vec::with_capacity(self.names.len());
                                    if scan_row_fast(l, &self.tmpl, &self.names, &mut scratch) {
                                        for (b, v) in builders.iter_mut().zip(&scratch) {
                                            b.push(v);
                                        }
                                        got += 1;
                                    }
                                }
                            }
                            Err(_) => {
                                self.eof = true;
                                break;
                            }
                        }
                        continue;
                    }
                }
            }

            let last_nl = match buf.iter().rposition(|&b| b == b'\n') {
                None => {
                    self.carry.extend_from_slice(buf);
                    self.reader.consume(avail);
                    self.pos += avail as u64;
                    continue;
                }
                Some(p) => p,
            };
            // One UTF-8 validation per complete-lines region ('\n' is a char
            // boundary). On an invalid byte: decode the valid lines before it,
            // then stop exactly where `read_line` errored.
            let region = &buf[..=last_nl];
            let (text, stop_after) = match std::str::from_utf8(region) {
                Ok(t) => (t, false),
                Err(e) => match region[..e.valid_up_to()].iter().rposition(|&b| b == b'\n') {
                    Some(c) => (
                        std::str::from_utf8(&region[..=c]).expect("valid prefix"),
                        true,
                    ),
                    None => {
                        self.eof = true;
                        break;
                    }
                },
            };
            let mut cur = 0usize;
            let mut walked = 0usize;
            {
                // Per-block scratch: `ScVal` borrows `text`, which outlives
                // every line in this block (the whole point of the block walk).
                let mut scratch: Vec<ScVal> = Vec::with_capacity(self.names.len());
                while cur < text.len() {
                    if got >= self.chunk_size {
                        break;
                    }
                    let rest = &text[cur..];
                    let nl = rest
                        .as_bytes()
                        .iter()
                        .position(|&b| b == b'\n')
                        .expect("region ends with newline");
                    let raw = &rest[..nl];
                    cur += nl + 1;
                    walked = cur;
                    let l = raw.trim_end_matches(['\n', '\r']);
                    if l.trim().is_empty() {
                        continue;
                    }
                    // Malformed lines are skipped (already counted in pass 1).
                    if scan_row_fast(l, &self.tmpl, &self.names, &mut scratch) {
                        for (b, v) in builders.iter_mut().zip(&scratch) {
                            b.push(v);
                        }
                        got += 1;
                    }
                }
            }
            let text_len = text.len();
            self.reader.consume(walked);
            self.pos += walked as u64;
            if stop_after && walked == text_len {
                self.eof = true;
                break;
            }
        }
        if got == 0 {
            return None;
        }
        Some(builders.into_iter().map(ColBuilder::finish).collect())
    }

    pub fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.eof {
            return None;
        }
        // Fused fast path: a FLAT all-scalar schema decodes straight into
        // per-column builders (no per-cell JVal; strings borrow the line).
        // Semantics pinned identical to the JVal path (see the fused-decode
        // header comment); nested schemas keep the general path below.
        if self.names.len() <= 128 && self.jtypes.iter().all(|t| matches!(t, JType::Scalar(_))) {
            return self.next_columns_fused();
        }
        let mut per_col: Vec<Vec<JVal>> = self.names.iter().map(|_| Vec::new()).collect();
        let mut got = 0usize;
        while got < self.chunk_size {
            if matches!(self.limit, Some(end) if self.pos >= end) {
                self.eof = true;
                break;
            }
            self.line.clear();
            let n = match self.reader.read_line(&mut self.line) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => n,
                Err(_) => {
                    self.eof = true;
                    break;
                }
            };
            self.pos += n as u64;
            let l = self.line.trim_end_matches(['\n', '\r']);
            if l.trim().is_empty() {
                continue;
            }
            // Malformed lines are skipped (already counted in pass 1).
            if let Some(obj) = parse_object(l) {
                // Move each value straight into its column (the old per-column
                // `find` cloned every JVal and paid O(cols²) compares). The
                // FIRST occurrence of a name wins (duplicate keys ignored) and
                // a missing name is Null — exactly the old `find` semantics.
                // Keys usually arrive in column order, so index j is an O(1)
                // hint; a reordered key falls back to the position scan.
                let ncols = self.names.len();
                let mut row: Vec<JVal> = (0..ncols).map(|_| JVal::Null).collect();
                let mut filled = vec![false; ncols];
                for (j, (k, v)) in obj.into_iter().enumerate() {
                    let idx = if self.names.get(j).is_some_and(|n| n == &k) {
                        Some(j)
                    } else {
                        self.names.iter().position(|n| n == &k)
                    };
                    if let Some(ix) = idx {
                        if !filled[ix] {
                            filled[ix] = true;
                            row[ix] = v;
                        }
                    }
                }
                for (i, v) in row.into_iter().enumerate() {
                    per_col[i].push(v);
                }
                got += 1;
            }
        }
        if got == 0 {
            return None;
        }
        Some(
            self.jtypes
                .iter()
                .zip(&per_col)
                .map(|(t, vals)| build_column(t, vals))
                .collect(),
        )
    }
}

/// Codec face (§28.5): the streaming JSONL reader *is* the decoder. It has no
/// prefilter / parse-failure accounting, so it uses the trait defaults.
impl crate::codec::Decoder for JsonlChunker {
    fn decode_chunk(&mut self) -> Option<Vec<Column>> {
        self.next_columns()
    }
}

/// A **single-pass** JSON-Lines reader over a non-seekable byte stream (an HTTP
/// body, design §33, feature `net`) — the JSONL analogue of
/// [`crate::csv::CompressedCsvReader`]. A network stream can't be re-read for the
/// two-pass [`infer_global`], so the schema (incl. nested shape, §32 s3b) is
/// inferred from a buffered sample of the first `chunk_size` objects, then the
/// rest is streamed. Same trade-off as the compressed CSV path: a key/type that
/// only appears (or widens) past the sample is missed (documented, §33).
#[cfg(any(feature = "net", feature = "gzip", feature = "zstd"))]
pub struct StreamJsonlReader {
    /// Key-order template for the fused fast scan (see [`RowTemplate`]).
    tmpl: RowTemplate,
    reader: Box<dyn BufRead + Send>,
    names: Vec<String>,
    jtypes: Vec<JType>,
    chunk_size: usize,
    /// Sample lines buffered during inference, replayed before streaming the rest.
    pending: Vec<String>,
    pending_pos: usize,
    line: String,
    eof: bool,
    pub bad_rows: usize,
}

#[cfg(any(feature = "net", feature = "gzip", feature = "zstd"))]
impl StreamJsonlReader {
    /// Sample-infer the schema from an already-opened stream, then yield rows.
    pub fn from_reader(
        mut reader: Box<dyn BufRead + Send>,
        chunk_size: usize,
    ) -> Result<(Schema, StreamJsonlReader), String> {
        let cs = chunk_size.max(1);
        let mut pending: Vec<String> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut infers: Vec<Infer> = Vec::new();
        let mut started = false;
        let mut bad_rows = 0usize;
        while pending.len() < cs {
            let mut l = String::new();
            if reader.read_line(&mut l).map_err(|e| e.to_string())? == 0 {
                break;
            }
            let t = l.trim_end_matches(['\n', '\r']);
            if t.trim().is_empty() {
                continue;
            }
            match parse_object(t) {
                Some(obj) => {
                    if !started {
                        names = obj.iter().map(|(k, _)| k.clone()).collect();
                        infers = names.iter().map(|_| Infer::new()).collect();
                        started = true;
                    }
                    for (k, v) in &obj {
                        if let Some(i) = names.iter().position(|n| n == k) {
                            infers[i].observe(v);
                        }
                    }
                    pending.push(t.to_string());
                }
                None => bad_rows += 1,
            }
        }
        if !started {
            return Err("JSON stream has no valid objects".to_string());
        }
        let jtypes: Vec<JType> = infers.iter().map(|f| f.resolve()).collect();
        let schema = schema_from(&names, &jtypes);
        Ok((
            schema,
            StreamJsonlReader {
                tmpl: RowTemplate::new(&names),
                reader,
                names,
                jtypes,
                chunk_size: cs,
                pending,
                pending_pos: 0,
                line: String::new(),
                eof: false,
                bad_rows,
            },
        ))
    }

    /// Project one parsed object onto the schema's column order (missing → null).
    /// Fused flat-scalar decode for the compressed stream (see
    /// `JsonlChunker::next_columns_fused`): sample replay first, then the
    /// stream; a malformed streamed line counts into `bad_rows` exactly like
    /// the general path (sample lines were counted at open).
    fn next_columns_fused(&mut self) -> Option<Vec<Column>> {
        let dtypes: Vec<DataType> = self
            .jtypes
            .iter()
            .map(|t| match t {
                JType::Scalar(d) => *d,
                _ => unreachable!("fused path is flat-only"),
            })
            .collect();
        let mut builders: Vec<ColBuilder> = dtypes
            .iter()
            .map(|d| ColBuilder::new(*d, self.chunk_size))
            .collect();
        let mut got = 0usize;
        while got < self.chunk_size && self.pending_pos < self.pending.len() {
            let line = std::mem::take(&mut self.pending[self.pending_pos]);
            self.pending_pos += 1;
            let mut scratch: Vec<ScVal> = Vec::with_capacity(self.names.len());
            if scan_row_fast(&line, &self.tmpl, &self.names, &mut scratch) {
                for (b, v) in builders.iter_mut().zip(&scratch) {
                    b.push(v);
                }
                got += 1;
            }
        }
        while got < self.chunk_size && !self.eof {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    self.eof = true;
                    break;
                }
            }
            let l = self.line.trim_end_matches(['\n', '\r']);
            if l.trim().is_empty() {
                continue;
            }
            let mut scratch: Vec<ScVal> = Vec::with_capacity(self.names.len());
            if scan_row_fast(l, &self.tmpl, &self.names, &mut scratch) {
                for (b, v) in builders.iter_mut().zip(&scratch) {
                    b.push(v);
                }
                got += 1;
            } else {
                self.bad_rows += 1;
            }
        }
        if got == 0 {
            return None;
        }
        Some(builders.into_iter().map(ColBuilder::finish).collect())
    }

    fn push_obj(&self, per_col: &mut [Vec<JVal>], obj: Vec<(String, JVal)>) {
        // Move each value straight into its column (the old per-column `find`
        // cloned every JVal and paid O(cols²) compares). First occurrence of a
        // name wins, missing → Null — the old `find` semantics exactly. Keys
        // usually arrive in column order → index j is an O(1) hint.
        let ncols = self.names.len();
        let mut row: Vec<JVal> = (0..ncols).map(|_| JVal::Null).collect();
        let mut filled = vec![false; ncols];
        for (j, (k, v)) in obj.into_iter().enumerate() {
            let idx = if self.names.get(j).is_some_and(|n| n == &k) {
                Some(j)
            } else {
                self.names.iter().position(|n| n == &k)
            };
            if let Some(ix) = idx {
                if !filled[ix] {
                    filled[ix] = true;
                    row[ix] = v;
                }
            }
        }
        for (i, v) in row.into_iter().enumerate() {
            per_col[i].push(v);
        }
    }

    pub fn next_columns(&mut self) -> Option<Vec<Column>> {
        if self.eof && self.pending_pos >= self.pending.len() {
            return None;
        }
        // Fused fast path (same machinery and pinned semantics as
        // `JsonlChunker::next_columns_fused`): a FLAT all-scalar schema decodes
        // straight into per-column builders — no per-cell JVal. The compressed
        // stream replays its buffered sample lines first, exactly like the
        // general path below.
        if self.names.len() <= 128 && self.jtypes.iter().all(|t| matches!(t, JType::Scalar(_))) {
            return self.next_columns_fused();
        }
        let mut per_col: Vec<Vec<JVal>> = self.names.iter().map(|_| Vec::new()).collect();
        let mut got = 0usize;
        // Drain the buffered sample first.
        while got < self.chunk_size && self.pending_pos < self.pending.len() {
            let line = std::mem::take(&mut self.pending[self.pending_pos]);
            self.pending_pos += 1;
            if let Some(obj) = parse_object(&line) {
                self.push_obj(&mut per_col, obj);
                got += 1;
            }
        }
        // Then stream the rest.
        while got < self.chunk_size && !self.eof {
            self.line.clear();
            match self.reader.read_line(&mut self.line) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    self.eof = true;
                    break;
                }
            }
            let l = self.line.trim_end_matches(['\n', '\r']);
            if l.trim().is_empty() {
                continue;
            }
            match parse_object(l) {
                Some(obj) => {
                    self.push_obj(&mut per_col, obj);
                    got += 1;
                }
                None => self.bad_rows += 1,
            }
        }
        if got == 0 {
            return None;
        }
        Some(
            self.jtypes
                .iter()
                .zip(&per_col)
                .map(|(t, vals)| build_column(t, vals))
                .collect(),
        )
    }
}

#[cfg(any(feature = "net", feature = "gzip", feature = "zstd"))]
impl crate::codec::Decoder for StreamJsonlReader {
    fn decode_chunk(&mut self) -> Option<Vec<Column>> {
        self.next_columns()
    }
}

/// Plan a byte-range parallel read of a JSON-Lines file: the global schema and
/// `nparts` newline-aligned ranges covering the file exactly once. Returns
/// `None` for a top-level array (not splittable) — the caller stays serial.
/// `(schema, column names, types, newline-aligned byte ranges, malformed rows)`.
pub type JsonlPlan = (Schema, Vec<String>, Vec<JType>, Vec<(u64, u64)>, usize);

pub fn plan_parallel(path: &str, nparts: usize) -> Option<JsonlPlan> {
    if is_json_array(path) {
        return None;
    }
    let ranges = snap_ranges(path, nparts)?;
    if ranges.len() < 2 {
        // Too small to split: the caller stays on the serial reader (which
        // runs `infer_global` itself).
        return None;
    }
    // Pass 1 IN PARALLEL: infer each newline-aligned range on its own thread,
    // then fold the results **in range order**. The fold reproduces the
    // sequential `infer_global` exactly: the global column order is the first
    // valid object's key list from the earliest started range (= the file's
    // first valid object), each global key's type merges every range's
    // observations of that key (`Infer::merge` is order-respecting), keys
    // outside the global set are discarded (the sequential scan never observes
    // them), and malformed-line counts sum. Previously this pass was a serial
    // full-file JSON parse — the dominant cost of a large JSONL `read`.
    let infers: Vec<Result<RangeInfer, String>> = std::thread::scope(|s| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|&(a, b)| s.spawn(move || infer_range(path, a, b)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut names: Option<Vec<String>> = None;
    let mut merged: Vec<(String, Infer)> = Vec::new();
    let mut bad_rows = 0usize;
    for r in infers {
        let (seen, first_names, bad) = r.ok()?;
        bad_rows += bad;
        if names.is_none() {
            names = first_names;
        }
        for (k, inf) in seen {
            match merged.iter_mut().find(|(n, _)| n == &k) {
                Some((_, m)) => m.merge(&inf),
                None => merged.push((k, inf)),
            }
        }
    }
    // No valid object anywhere → the serial reader surfaces its own error.
    let names = names?;
    let jtypes: Vec<JType> = names
        .iter()
        .map(|n| {
            merged
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, inf)| inf.resolve())
                .unwrap_or(JType::Scalar(DataType::Str))
        })
        .collect();
    let schema = schema_from(&names, &jtypes);
    Some((schema, names, jtypes, ranges, bad_rows))
}

/// Split the file into ≤ `nparts` newline-aligned `[start, end)` ranges (no
/// header, so the first range starts at 0). Each boundary is snapped forward to
/// the byte just after the next `\n`, so a line never straddles two ranges.
fn snap_ranges(path: &str, nparts: usize) -> Option<Vec<(u64, u64)>> {
    let len = std::fs::metadata(path).ok()?.len();
    if len == 0 {
        return None;
    }
    let mut f = FileTransport::open(path).ok()?;
    let mut bounds = vec![0u64];
    let mut scratch = String::new();
    for i in 1..nparts {
        let approx = len * (i as u64) / (nparts as u64);
        if approx <= *bounds.last().unwrap() {
            continue;
        }
        if f.seek(SeekFrom::Start(approx)).is_err() {
            continue;
        }
        scratch.clear();
        let consumed = f.read_line(&mut scratch).ok()?; // finish the partial line
        let boundary = approx + consumed as u64;
        if boundary < len && boundary > *bounds.last().unwrap() {
            bounds.push(boundary);
        }
    }
    bounds.push(len);
    Some(bounds.windows(2).map(|w| (w[0], w[1])).collect())
}

// ----------------------------------------------------------------- JSON parsing

// ---------------------------------------------------------------------------
// Allocation-free inference scanner (pass 1;統括カリカリ指示). Each `scan_*`
// mirrors the corresponding `parse_*` EXACTLY — same acceptance, same
// Int/Float/Bool/Str classification, same duplicate-key handling — but builds
// no `JVal` and no value `String`s (a key allocates only when first seen for
// its column). Inference over the scanner is therefore byte-identical to
// inference over the parser (pinned by `scanner_matches_parser_inference`)
// while skipping ~all pass-1 allocations of a large JSONL read.

/// Scan one line as a top-level object, observing directly into `seen`
/// (per-key [`Infer`]s, first-seen order) and capturing `first_names` from the
/// first valid object. `false` = malformed (counted by the caller), with the
/// same acceptance as [`parse_object`].
fn scan_line_infer(
    line: &str,
    seen: &mut Vec<(String, Infer)>,
    first_names: &mut Option<Vec<String>>,
) -> bool {
    let b = line.as_bytes();
    let mut i = 0usize;
    skip_ws(b, &mut i);
    if i >= b.len() || b[i] != b'{' {
        return false;
    }
    i += 1;
    skip_ws(b, &mut i);
    let first_line = first_names.is_none();
    let mut names_acc: Vec<String> = Vec::new();
    if i < b.len() && b[i] == b'}' {
        // empty object: still "started" (claims the column order), like parse.
        if first_line {
            *first_names = Some(names_acc);
        }
        return true;
    }
    loop {
        skip_ws(b, &mut i);
        let Some(key) = scan_key(b, &mut i) else {
            return false;
        };
        skip_ws(b, &mut i);
        if i >= b.len() || b[i] != b':' {
            return false;
        }
        i += 1;
        skip_ws(b, &mut i);
        if first_line {
            names_acc.push(key.clone().into_owned());
        }
        // Observe into the key's Infer (first-seen appends), or skip the value
        // structurally if this key is outside the tracked set. Mirrors
        // `infer_range`'s "observe every key" rule.
        let idx = match seen.iter().position(|(n, _)| n.as_str() == key.as_ref()) {
            Some(ix) => ix,
            None => {
                seen.push((key.into_owned(), Infer::new()));
                seen.len() - 1
            }
        };
        if !scan_value_into(b, &mut i, &mut seen[idx].1) {
            return false;
        }
        skip_ws(b, &mut i);
        match b.get(i) {
            Some(b',') => {
                i += 1;
                continue;
            }
            Some(b'}') => break,
            _ => return false,
        }
    }
    if first_line {
        *first_names = Some(names_acc);
    }
    true
}

/// Scan a JSON string, allocating only if it contains an escape (keys are
/// almost always escape-free → borrowed). Same acceptance as [`parse_string`].
fn scan_key<'a>(b: &'a [u8], i: &mut usize) -> Option<std::borrow::Cow<'a, str>> {
    if *i >= b.len() || b[*i] != b'"' {
        return None;
    }
    let start = *i + 1;
    let mut j = start;
    while j < b.len() {
        match b[j] {
            b'"' => {
                *i = j + 1;
                // SAFETY-free: the line came from &str, so the slice is UTF-8.
                return Some(std::borrow::Cow::Borrowed(
                    std::str::from_utf8(&b[start..j]).ok()?,
                ));
            }
            b'\\' => {
                // Escapes present: fall back to the allocating parser (rare).
                let mut k = start - 1;
                return parse_string(b, &mut k).map(|s| {
                    *i = k;
                    std::borrow::Cow::Owned(s)
                });
            }
            _ => j += 1,
        }
    }
    None
}

/// Scan a string VALUE without materializing it. Byte-for-byte the same
/// acceptance as [`parse_string`] (escape validity incl. `\uXXXX` hex).
fn scan_string(b: &[u8], i: &mut usize) -> bool {
    if *i >= b.len() || b[*i] != b'"' {
        return false;
    }
    *i += 1;
    while *i < b.len() {
        let c = b[*i];
        *i += 1;
        match c {
            b'"' => return true,
            b'\\' => {
                let Some(&e) = b.get(*i) else { return false };
                *i += 1;
                if e == b'u' {
                    let Some(hex) = b.get(*i..*i + 4) else {
                        return false;
                    };
                    if std::str::from_utf8(hex)
                        .ok()
                        .and_then(|h| u32::from_str_radix(h, 16).ok())
                        .is_none()
                    {
                        return false;
                    }
                    *i += 4;
                }
            }
            _ => {}
        }
    }
    false // unterminated
}

/// Scan any value, observing its class into `inf`. Mirrors [`parse_value`].
fn scan_value_into(b: &[u8], i: &mut usize, inf: &mut Infer) -> bool {
    skip_ws(b, i);
    let Some(&c) = b.get(*i) else { return false };
    match c {
        b'"' => {
            if !scan_string(b, i) {
                return false;
            }
            inf.saw_scalar = true;
            inf.scalar.observe_str();
            true
        }
        b'{' => scan_obj_into(b, i, inf),
        b'[' => scan_arr_into(b, i, inf),
        b't' => {
            if !scan_lit(b, i, "true") {
                return false;
            }
            inf.saw_scalar = true;
            inf.scalar.observe_bool();
            true
        }
        b'f' => {
            if !scan_lit(b, i, "false") {
                return false;
            }
            inf.saw_scalar = true;
            inf.scalar.observe_bool();
            true
        }
        b'n' => scan_lit(b, i, "null"), // null observes nothing (like observe)
        _ => match scan_number(b, i) {
            Some(is_float) => {
                inf.saw_scalar = true;
                if is_float {
                    inf.scalar.observe_float();
                } else {
                    inf.scalar.observe_int();
                }
                true
            }
            None => false,
        },
    }
}

/// Scan a nested object into the column's struct inference. Mirrors
/// `Infer::observe(JVal::Obj)` exactly: the FIRST object seen for this column
/// fixes the child-name order (duplicate keys included, matching the
/// `fields.iter().map(k).collect()` + first-`position` semantics); later
/// objects observe known children and ignore extra keys.
fn scan_obj_into(b: &[u8], i: &mut usize, inf: &mut Infer) -> bool {
    if *i >= b.len() || b[*i] != b'{' {
        return false;
    }
    *i += 1;
    let first_obj = inf.structs.is_none();
    let st = inf.structs.get_or_insert_with(|| StructInfer {
        names: Vec::new(),
        children: Vec::new(),
    });
    skip_ws(b, i);
    if *i < b.len() && b[*i] == b'}' {
        *i += 1;
        return true;
    }
    loop {
        skip_ws(b, i);
        let Some(key) = scan_key(b, i) else {
            return false;
        };
        skip_ws(b, i);
        if *i >= b.len() || b[*i] != b':' {
            return false;
        }
        *i += 1;
        // First object: append unconditionally (dupes included) — identical to
        // the parser path's names. Observation always targets the FIRST match.
        if first_obj {
            st.names.push(key.clone().into_owned());
            st.children.push(Infer::new());
        }
        match st.names.iter().position(|n| n.as_str() == key.as_ref()) {
            Some(ix) => {
                if !scan_value_into(b, i, &mut st.children[ix]) {
                    return false;
                }
            }
            None => {
                if !scan_value_skip(b, i) {
                    return false;
                }
            }
        }
        skip_ws(b, i);
        match b.get(*i) {
            Some(b',') => {
                *i += 1;
                continue;
            }
            Some(b'}') => {
                *i += 1;
                break;
            }
            _ => return false,
        }
    }
    true
}

/// Scan a nested array into the column's list inference (element-wise merge,
/// mirroring `Infer::observe(JVal::Arr)`).
fn scan_arr_into(b: &[u8], i: &mut usize, inf: &mut Infer) -> bool {
    if *i >= b.len() || b[*i] != b'[' {
        return false;
    }
    *i += 1;
    let el = inf.list.get_or_insert_with(|| Box::new(Infer::new()));
    skip_ws(b, i);
    if *i < b.len() && b[*i] == b']' {
        *i += 1;
        return true;
    }
    loop {
        skip_ws(b, i);
        if !scan_value_into(b, i, el) {
            return false;
        }
        skip_ws(b, i);
        match b.get(*i) {
            Some(b',') => {
                *i += 1;
                continue;
            }
            Some(b']') => {
                *i += 1;
                break;
            }
            _ => return false,
        }
    }
    true
}

/// Structurally skip any value (no observation), same acceptance as parsing.
fn scan_value_skip(b: &[u8], i: &mut usize) -> bool {
    let mut scratch = Infer::new();
    scan_value_into(b, i, &mut scratch)
}

fn scan_lit(b: &[u8], i: &mut usize, lit: &str) -> bool {
    if b[*i..].starts_with(lit.as_bytes()) {
        *i += lit.len();
        true
    } else {
        false
    }
}

/// Scan a number; `Some(is_float)` with [`parse_number`]'s exact
/// classification (incl. the i64-overflow → float fallback).
fn scan_number(b: &[u8], i: &mut usize) -> Option<bool> {
    let start = *i;
    let mut is_float = false;
    if b.get(*i) == Some(&b'-') {
        *i += 1;
    }
    while *i < b.len() {
        match b[*i] {
            b'0'..=b'9' => *i += 1,
            b'.' | b'e' | b'E' | b'+' | b'-' => {
                is_float = true;
                *i += 1;
            }
            _ => break,
        }
    }
    let text = std::str::from_utf8(&b[start..*i]).ok()?;
    if text.is_empty() || text == "-" {
        return None;
    }
    if is_float {
        text.parse::<f64>().ok().map(|_| true)
    } else {
        match text.parse::<i64>() {
            Ok(_) => Some(false),
            Err(_) => text.parse::<f64>().ok().map(|_| true),
        }
    }
}

// ---------------------------------------------------------------------------
// Fused decode (pass 2;統括「進めろ！打ち勝て！」): for a FLAT all-scalar
// schema, parse each line straight into per-column scalar builders — no
// per-cell `JVal`, no value `String`s (strings borrow the line; escapes and
// the rare hetero-nested cell fall back per value). Semantics mirror
// `parse_object` + `build_scalar` EXACTLY: same acceptance, first occurrence
// of a duplicate key wins, missing key → null, an I64 column nulls a float
// cell, a Str column re-renders numbers via `f64::to_string` (never the raw
// text slice — `"1.50"` renders `"1.5"` on both paths). A malformed line
// commits nothing (row scratch is discarded).

/// One scanned cell, borrowing from the line where possible.
enum ScVal<'a> {
    Null,
    I(i64),
    F(f64),
    B(bool),
    /// Escape-free string: a borrowed slice of the line.
    S(&'a str),
    /// String with escapes: unescaped (allocates, rare).
    SOwned(String),
    /// Nested value in a (heterogeneous) Str column: its JSON text.
    NestedJson(String),
}

/// Per-column output builder for the fused path (one per scalar column).
enum ColBuilder {
    I64(Vec<i64>, Vec<bool>),
    F64(Vec<f64>, Vec<bool>),
    Bool(Vec<bool>, Vec<bool>),
    Str(StrColumn, Vec<bool>),
}

impl ColBuilder {
    fn new(dtype: DataType, cap: usize) -> ColBuilder {
        match dtype {
            DataType::I64 => ColBuilder::I64(Vec::with_capacity(cap), Vec::with_capacity(cap)),
            DataType::F64 => ColBuilder::F64(Vec::with_capacity(cap), Vec::with_capacity(cap)),
            DataType::Bool => ColBuilder::Bool(Vec::with_capacity(cap), Vec::with_capacity(cap)),
            _ => ColBuilder::Str(
                StrColumn::with_capacity(cap, cap * 8),
                Vec::with_capacity(cap),
            ),
        }
    }
    fn push(&mut self, v: &ScVal<'_>) {
        match self {
            // Mirrors `build_scalar`'s I64 arm: Int valid, anything else null.
            ColBuilder::I64(out, valid) => match v {
                ScVal::I(i) => {
                    out.push(*i);
                    valid.push(true);
                }
                _ => {
                    out.push(0);
                    valid.push(false);
                }
            },
            ColBuilder::F64(out, valid) => match v {
                ScVal::I(i) => {
                    out.push(*i as f64);
                    valid.push(true);
                }
                ScVal::F(f) => {
                    out.push(*f);
                    valid.push(true);
                }
                _ => {
                    out.push(0.0);
                    valid.push(false);
                }
            },
            ColBuilder::Bool(out, valid) => match v {
                ScVal::B(b) => {
                    out.push(*b);
                    valid.push(true);
                }
                _ => {
                    out.push(false);
                    valid.push(false);
                }
            },
            ColBuilder::Str(out, valid) => match v {
                ScVal::Null => {
                    out.push("");
                    valid.push(false);
                }
                ScVal::B(b) => {
                    out.push(if *b { "true" } else { "false" });
                    valid.push(true);
                }
                ScVal::I(i) => {
                    out.push(&i.to_string());
                    valid.push(true);
                }
                ScVal::F(f) => {
                    out.push(&f.to_string());
                    valid.push(true);
                }
                ScVal::S(x) => {
                    out.push(x);
                    valid.push(true);
                }
                ScVal::SOwned(x) => {
                    out.push(x);
                    valid.push(true);
                }
                ScVal::NestedJson(t) => {
                    out.push(t);
                    valid.push(true);
                }
            },
        }
    }
    fn finish(self) -> Column {
        match self {
            ColBuilder::I64(out, valid) => {
                Column::new(ColumnData::I64(out), Validity::from_bits(&valid))
            }
            ColBuilder::F64(out, valid) => {
                Column::new(ColumnData::F64(out), Validity::from_bits(&valid))
            }
            ColBuilder::Bool(out, valid) => {
                Column::new(ColumnData::Bool(out), Validity::from_bits(&valid))
            }
            ColBuilder::Str(out, valid) => {
                Column::new(ColumnData::Str(out), Validity::from_bits(&valid))
            }
        }
    }
}

/// Scan one line into the reused row `scratch` (one slot per column,
/// first-occurrence-wins, missing → Null). `false` = malformed line (same
/// acceptance as `parse_object`); nothing is committed for it.
/// A literal key-segment template for the overwhelmingly common line shape:
/// keys in exact schema order with no interior whitespace —
/// `{"k0":<v>,"k1":<v>,…}`. One `memcmp` per key replaces the generic
/// key-scan + name-position lookup. ANY deviation (reordered/missing/extra
/// keys, whitespace, an escaped key, a scan failure, trailing bytes) falls
/// back to [`scan_row`] for that line, so the accepted language — and every
/// scanned value (`scan_cell` is shared) — is exactly the generic scanner's.
pub(crate) struct RowTemplate {
    segs: Vec<Vec<u8>>,
}

impl RowTemplate {
    pub(crate) fn new(names: &[String]) -> Self {
        let mut segs = Vec::with_capacity(names.len());
        for (k, n) in names.iter().enumerate() {
            let mut seg = Vec::with_capacity(n.len() + 4);
            seg.extend_from_slice(if k == 0 { b"{\"" } else { b",\"" });
            seg.extend_from_slice(n.as_bytes());
            seg.extend_from_slice(b"\":");
            segs.push(seg);
        }
        RowTemplate { segs }
    }
}

/// [`scan_row`] with the template fast path (see [`RowTemplate`]).
fn scan_row_fast<'a>(
    line: &'a str,
    tmpl: &RowTemplate,
    names: &[String],
    scratch: &mut Vec<ScVal<'a>>,
) -> bool {
    let b = line.as_bytes();
    let mut i = 0usize;
    scratch.clear();
    for seg in &tmpl.segs {
        if b.len() < i + seg.len() || &b[i..i + seg.len()] != seg.as_slice() {
            return scan_row(line, names, scratch);
        }
        i += seg.len();
        let Some(v) = scan_cell(b, &mut i) else {
            return scan_row(line, names, scratch);
        };
        scratch.push(v);
    }
    if i + 1 == b.len() && b[i] == b'}' && !tmpl.segs.is_empty() {
        return true;
    }
    scan_row(line, names, scratch)
}

fn scan_row<'a>(line: &'a str, names: &[String], scratch: &mut Vec<ScVal<'a>>) -> bool {
    let b = line.as_bytes();
    let mut i = 0usize;
    scratch.clear();
    for _ in 0..names.len() {
        scratch.push(ScVal::Null);
    }
    let mut filled = 0u128; // ncols ≤ 128 fused (checked at construction)
    skip_ws(b, &mut i);
    if i >= b.len() || b[i] != b'{' {
        return false;
    }
    i += 1;
    skip_ws(b, &mut i);
    if i < b.len() && b[i] == b'}' {
        return true; // empty object: every column null
    }
    loop {
        skip_ws(b, &mut i);
        let Some(key) = scan_key(b, &mut i) else {
            return false;
        };
        skip_ws(b, &mut i);
        if i >= b.len() || b[i] != b':' {
            return false;
        }
        i += 1;
        skip_ws(b, &mut i);
        // Column index: in-order hint impossible here (scratch has no j
        // counter tie); position scan over ≤ a handful of names is fine.
        let idx = names.iter().position(|n| n.as_str() == key.as_ref());
        match idx {
            Some(ix) if filled & (1u128 << ix) == 0 => {
                let Some(v) = scan_cell(b, &mut i) else {
                    return false;
                };
                scratch[ix] = v;
                filled |= 1u128 << ix;
            }
            _ => {
                // Unknown key or duplicate (first wins): structurally skip.
                let mut sink = Infer::new();
                if !scan_value_into(b, &mut i, &mut sink) {
                    return false;
                }
            }
        }
        skip_ws(b, &mut i);
        match b.get(i) {
            Some(b',') => {
                i += 1;
                continue;
            }
            Some(b'}') => break,
            _ => return false,
        }
    }
    true
}

/// Scan one scalar cell into a borrowed [`ScVal`]. Mirrors `parse_value`'s
/// acceptance and classification; a nested value renders to its JSON text
/// (the heterogeneous Str-column fallback of `build_scalar`).
fn scan_cell<'a>(b: &'a [u8], i: &mut usize) -> Option<ScVal<'a>> {
    match *b.get(*i)? {
        b'"' => {
            // Fast path: escape-free string borrows the line.
            let start = *i + 1;
            let mut j = start;
            while j < b.len() {
                match b[j] {
                    b'"' => {
                        *i = j + 1;
                        return std::str::from_utf8(&b[start..j]).ok().map(ScVal::S);
                    }
                    b'\\' => {
                        let mut k = start - 1;
                        return parse_string(b, &mut k).map(|s| {
                            *i = k;
                            ScVal::SOwned(s)
                        });
                    }
                    _ => j += 1,
                }
            }
            None
        }
        b'{' | b'[' => {
            // Heterogeneous cell in a Str column: parse + render as JSON text
            // (rare; identical to the JVal path's `jval_json`).
            let v = parse_value(b, i)?;
            let mut t = String::new();
            jval_json(&v, &mut t);
            Some(ScVal::NestedJson(t))
        }
        b't' => {
            if scan_lit(b, i, "true") {
                Some(ScVal::B(true))
            } else {
                None
            }
        }
        b'f' => {
            if scan_lit(b, i, "false") {
                Some(ScVal::B(false))
            } else {
                None
            }
        }
        b'n' => {
            if scan_lit(b, i, "null") {
                Some(ScVal::Null)
            } else {
                None
            }
        }
        _ => {
            // Numbers: identical classification to `parse_number`.
            let start = *i;
            let mut is_float = false;
            if b.get(*i) == Some(&b'-') {
                *i += 1;
            }
            while *i < b.len() {
                match b[*i] {
                    b'0'..=b'9' => *i += 1,
                    b'.' | b'e' | b'E' | b'+' | b'-' => {
                        is_float = true;
                        *i += 1;
                    }
                    _ => break,
                }
            }
            let text = std::str::from_utf8(&b[start..*i]).ok()?;
            if text.is_empty() || text == "-" {
                return None;
            }
            if is_float {
                text.parse::<f64>().ok().map(ScVal::F)
            } else {
                match text.parse::<i64>() {
                    Ok(n) => Some(ScVal::I(n)),
                    Err(_) => text.parse::<f64>().ok().map(ScVal::F),
                }
            }
        }
    }
}

/// Parse a single JSON object line into `(key, value)` pairs. Returns `None` if
/// the line is not a well-formed object (→ counted as a bad row). Nested values
/// are parsed structurally by [`parse_value`].
fn parse_object(line: &str) -> Option<Vec<(String, JVal)>> {
    let b = line.as_bytes();
    let mut i = 0usize;
    skip_ws(b, &mut i);
    parse_obj(b, &mut i)
}

/// Parse an object starting at `b[*i] == '{'`, consuming through its matching
/// `}`. Shared by the top-level line parser and nested values.
fn parse_obj(b: &[u8], i: &mut usize) -> Option<Vec<(String, JVal)>> {
    if *i >= b.len() || b[*i] != b'{' {
        return None;
    }
    *i += 1;
    let mut out = Vec::new();
    skip_ws(b, i);
    if *i < b.len() && b[*i] == b'}' {
        *i += 1;
        return Some(out); // empty object
    }
    loop {
        skip_ws(b, i);
        let key = parse_string(b, i)?;
        skip_ws(b, i);
        if *i >= b.len() || b[*i] != b':' {
            return None;
        }
        *i += 1;
        skip_ws(b, i);
        let val = parse_value(b, i)?;
        out.push((key, val));
        skip_ws(b, i);
        match b.get(*i) {
            Some(b',') => {
                *i += 1;
                continue;
            }
            Some(b'}') => {
                *i += 1;
                break;
            }
            _ => return None,
        }
    }
    Some(out)
}

/// Parse an array starting at `b[*i] == '['`, consuming through its matching `]`.
fn parse_arr(b: &[u8], i: &mut usize) -> Option<Vec<JVal>> {
    if *i >= b.len() || b[*i] != b'[' {
        return None;
    }
    *i += 1;
    let mut out = Vec::new();
    skip_ws(b, i);
    if *i < b.len() && b[*i] == b']' {
        *i += 1;
        return Some(out); // empty array
    }
    loop {
        skip_ws(b, i);
        let val = parse_value(b, i)?;
        out.push(val);
        skip_ws(b, i);
        match b.get(*i) {
            Some(b',') => {
                *i += 1;
                continue;
            }
            Some(b']') => {
                *i += 1;
                break;
            }
            _ => return None,
        }
    }
    Some(out)
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while *i < b.len() && matches!(b[*i], b' ' | b'\t' | b'\r' | b'\n') {
        *i += 1;
    }
}

fn parse_string(b: &[u8], i: &mut usize) -> Option<String> {
    if *i >= b.len() || b[*i] != b'"' {
        return None;
    }
    *i += 1;
    let mut s = String::new();
    while *i < b.len() {
        let c = b[*i];
        *i += 1;
        match c {
            b'"' => return Some(s),
            b'\\' => {
                let e = *b.get(*i)?;
                *i += 1;
                match e {
                    b'"' => s.push('"'),
                    b'\\' => s.push('\\'),
                    b'/' => s.push('/'),
                    b'n' => s.push('\n'),
                    b't' => s.push('\t'),
                    b'r' => s.push('\r'),
                    b'b' => s.push('\u{8}'),
                    b'f' => s.push('\u{c}'),
                    b'u' => {
                        // \uXXXX — decode a BMP code point (no surrogate pairing).
                        let hex = b.get(*i..*i + 4)?;
                        let code = u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
                        *i += 4;
                        s.push(char::from_u32(code).unwrap_or('\u{fffd}'));
                    }
                    other => s.push(other as char),
                }
            }
            // Multi-byte UTF-8 continuation: push raw bytes through.
            _ => {
                // Reconstruct the original char from this byte and any
                // continuation bytes (the slice is valid UTF-8).
                let start = *i - 1;
                while *i < b.len() && (b[*i] & 0xC0) == 0x80 {
                    *i += 1;
                }
                s.push_str(std::str::from_utf8(&b[start..*i]).ok()?);
            }
        }
    }
    None // unterminated string
}

fn parse_value(b: &[u8], i: &mut usize) -> Option<JVal> {
    skip_ws(b, i);
    match b.get(*i)? {
        b'"' => parse_string(b, i).map(JVal::Str),
        b'{' => parse_obj(b, i).map(JVal::Obj),
        b'[' => parse_arr(b, i).map(JVal::Arr),
        b't' => parse_lit(b, i, "true", JVal::Bool(true)),
        b'f' => parse_lit(b, i, "false", JVal::Bool(false)),
        b'n' => parse_lit(b, i, "null", JVal::Null),
        _ => parse_number(b, i),
    }
}

fn parse_lit(b: &[u8], i: &mut usize, lit: &str, val: JVal) -> Option<JVal> {
    if b[*i..].starts_with(lit.as_bytes()) {
        *i += lit.len();
        Some(val)
    } else {
        None
    }
}

fn parse_number(b: &[u8], i: &mut usize) -> Option<JVal> {
    let start = *i;
    let mut is_float = false;
    if b.get(*i) == Some(&b'-') {
        *i += 1;
    }
    while *i < b.len() {
        match b[*i] {
            b'0'..=b'9' => *i += 1,
            b'.' | b'e' | b'E' | b'+' | b'-' => {
                is_float = true;
                *i += 1;
            }
            _ => break,
        }
    }
    let text = std::str::from_utf8(&b[start..*i]).ok()?;
    if text.is_empty() || text == "-" {
        return None;
    }
    if is_float {
        text.parse::<f64>().ok().map(JVal::Float)
    } else {
        match text.parse::<i64>() {
            Ok(n) => Some(JVal::Int(n)),
            Err(_) => text.parse::<f64>().ok().map(JVal::Float),
        }
    }
}

/// Advance `*i` past a balanced `{...}` / `[...]` (string-aware), without
/// materializing it — used by [`collect_array`] to find object boundaries in a
/// multi-line top-level array.
fn skip_balanced(b: &[u8], i: &mut usize, open: u8, close: u8) -> bool {
    let mut depth = 0i32;
    let mut in_str = false;
    while *i < b.len() {
        let c = b[*i];
        *i += 1;
        if in_str {
            match c {
                b'\\' => {
                    *i += 1;
                }
                b'"' => in_str = false,
                _ => {}
            }
            continue;
        }
        match c {
            b'"' => in_str = true,
            x if x == open => depth += 1,
            x if x == close => {
                depth -= 1;
                if depth == 0 {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    /// The allocation-free inference scanner must ACCEPT exactly the lines the
    /// parser accepts and infer exactly the same shape — over a corpus that
    /// hits every divergence hazard: escapes (incl. bad `\\uXXXX`), numeric
    /// edges (i64 overflow → float, bare `-`, `1e5`), duplicate keys (top and
    /// nested — the first-object name-order rule), heterogeneous columns,
    /// nested obj/arr recursion, empty objects, truncated lines.
    #[test]
    fn scanner_matches_parser_inference() {
        let corpus: &[&str] = &[
            r#"{"a":1,"b":"x","c":true}"#,
            r#"{"a":1.5,"b":null}"#,
            r#"{"a":9223372036854775808}"#, // i64 overflow → float
            r#"{"a":-,"b":1}"#,             // bad number
            r#"{"a":1e5,"b":-2.5e-3}"#,
            r#"{"a":"esc\"q","b":"\u0041","c":"\u00ZZ"}"#, // bad \u hex → reject
            r#"{"a":"\u0041"}"#,
            r#"{"dup":1,"dup":2}"#,           // duplicate top-level keys
            r#"{"o":{"x":1,"x":2,"y":"s"}}"#, // duplicate nested keys
            r#"{"o":{"x":1},"o":{"y":2}}"#,
            r#"{"l":[1,2,3],"m":[{"k":1},{"k":2.5}]}"#,
            r#"{"l":[]}"#,
            r#"{}"#,
            r#"{"a":"#,  // truncated
            r#"{"a":1"#, // missing close
            r#"not json"#,
            r#"{"mixed":1}"#,
            r#"{"mixed":"s"}"#,
            r#"{"n":null}"#,
            r#"{"neg":-42,"z":0}"#,
        ];
        // Parser-path reference (the old infer_range body, verbatim semantics).
        let mut ref_seen: Vec<(String, Infer)> = Vec::new();
        let mut ref_first: Option<Vec<String>> = None;
        let mut ref_bad = 0usize;
        for l in corpus {
            match parse_object(l) {
                Some(obj) => {
                    if ref_first.is_none() {
                        ref_first = Some(obj.iter().map(|(k, _)| k.clone()).collect());
                    }
                    for (k, v) in &obj {
                        match ref_seen.iter_mut().find(|(n, _)| n == k) {
                            Some((_, inf)) => inf.observe(v),
                            None => {
                                let mut inf = Infer::new();
                                inf.observe(v);
                                ref_seen.push((k.clone(), inf));
                            }
                        }
                    }
                }
                None => ref_bad += 1,
            }
        }
        // Scanner path.
        let mut scan_seen: Vec<(String, Infer)> = Vec::new();
        let mut scan_first: Option<Vec<String>> = None;
        let mut scan_bad = 0usize;
        for l in corpus {
            if !scan_line_infer(l, &mut scan_seen, &mut scan_first) {
                scan_bad += 1;
            }
        }
        assert_eq!(scan_bad, ref_bad, "acceptance diverged");
        assert_eq!(scan_first, ref_first, "first-object names diverged");
        let names_r: Vec<&String> = ref_seen.iter().map(|(n, _)| n).collect();
        let names_s: Vec<&String> = scan_seen.iter().map(|(n, _)| n).collect();
        assert_eq!(names_s, names_r, "seen-key order diverged");
        for ((n, r), (_, sc)) in ref_seen.iter().zip(&scan_seen) {
            assert_eq!(
                format!("{:?}", sc.resolve()),
                format!("{:?}", r.resolve()),
                "inferred type diverged for key '{n}'"
            );
        }
    }

    use super::*;

    #[test]
    fn parses_flat_objects_and_infers_types() {
        let text = "{\"name\":\"aki\",\"age\":30,\"score\":1.5,\"ok\":true}\n\
                    {\"name\":\"ben\",\"age\":15,\"score\":2.0,\"ok\":false}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 0);
        assert_eq!(d.schema.field_names(), vec!["name", "age", "score", "ok"]);
        assert_eq!(d.schema.fields[1].dtype, DataType::I64);
        assert_eq!(d.schema.fields[2].dtype, DataType::F64);
        assert_eq!(d.schema.fields[3].dtype, DataType::Bool);
        match d.columns[0].data() {
            ColumnData::Str(s) => assert_eq!(s.get(0), "aki"),
            _ => panic!("expected str"),
        }
    }

    #[test]
    fn bad_lines_are_skipped() {
        let text = "{\"a\":1}\nnot json\n{\"a\":2}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 1);
        match d.columns[0].data() {
            ColumnData::I64(v) => assert_eq!(v, &[1, 2]),
            _ => panic!("expected i64"),
        }
    }

    #[test]
    fn parses_json_array_multiline() {
        // A top-level array (possibly pretty-printed) of objects, like an API
        // response, parses the same as JSON Lines.
        let text = "[\n  {\"name\":\"aki\",\"age\":30},\n  {\"name\":\"ben\",\"age\":15},\n  42,\n  {\"name\":\"cho\",\"age\":40}\n]";
        let d = parse(text).unwrap();
        assert_eq!(d.schema.field_names(), vec!["name", "age"]);
        assert_eq!(d.bad_rows, 1); // the bare `42` element
        match d.columns[1].data() {
            ColumnData::I64(v) => assert_eq!(v, &[30, 15, 40]),
            _ => panic!("expected i64 age"),
        }
    }

    #[test]
    fn nested_object_becomes_struct_column() {
        // §32 s3b: a nested object is a typed Struct lane (not raw text).
        let text = "{\"id\":1,\"meta\":{\"x\":2,\"y\":\"a\"}}\n\
                    {\"id\":2,\"meta\":{\"x\":5,\"y\":\"b\"}}\n";
        let d = parse(text).unwrap();
        let idx = d.schema.index_of("meta").unwrap();
        assert_eq!(d.schema.fields[idx].dtype, DataType::Struct);
        match d.columns[idx].data() {
            ColumnData::Struct(s) => {
                assert_eq!(s.names, vec!["x", "y"]);
                assert_eq!(s.len, 2);
                assert_eq!(s.columns[0].data().dtype(), DataType::I64);
                assert_eq!(s.columns[1].data().dtype(), DataType::Str);
                assert_eq!(s.columns[0].value_at(1), rivus_core::Value::I64(5));
            }
            _ => panic!("expected struct column"),
        }
    }

    #[test]
    fn nested_array_becomes_list_column() {
        // §32 s3b: a nested array is a typed List lane with i32 offsets.
        let text = "{\"id\":1,\"tags\":[10,20]}\n{\"id\":2,\"tags\":[30]}\n";
        let d = parse(text).unwrap();
        let idx = d.schema.index_of("tags").unwrap();
        assert_eq!(d.schema.fields[idx].dtype, DataType::List);
        match d.columns[idx].data() {
            ColumnData::List(l) => {
                assert_eq!(l.offsets, vec![0, 2, 3]);
                assert_eq!(l.child.data().dtype(), DataType::I64);
                assert_eq!(l.child.value_at(2), rivus_core::Value::I64(30));
            }
            _ => panic!("expected list column"),
        }
    }

    #[test]
    fn missing_or_null_nested_is_typed_null_not_silent() {
        // §32 s3b: a row missing the nested key (or with `null`) is a typed null
        // (validity = 0) on the struct lane — never a silent empty struct.
        let text = "{\"id\":1,\"meta\":{\"x\":2}}\n{\"id\":2}\n{\"id\":3,\"meta\":null}\n";
        let d = parse(text).unwrap();
        let idx = d.schema.index_of("meta").unwrap();
        assert_eq!(d.schema.fields[idx].dtype, DataType::Struct);
        let col = &d.columns[idx];
        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(col.is_null(2));
    }

    #[test]
    fn broken_nested_line_counted_as_bad_row() {
        // An unterminated nested object makes the whole line malformed → counted,
        // never a silent partial row.
        let text = "{\"id\":1,\"meta\":{\"x\":2}}\n{\"id\":2,\"meta\":{\"x\":}\n{\"id\":3,\"meta\":{\"x\":9}}\n";
        let d = parse(text).unwrap();
        assert_eq!(d.bad_rows, 1);
        assert_eq!(d.columns[0].len(), 2);
    }
}
