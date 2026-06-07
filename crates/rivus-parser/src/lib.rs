//! `rivus-parser` — turns Unified Flow Syntax source into a [`PlanGraph`].
//!
//! Conceptually `source -> AST -> IR`; for the MVP we lower directly into the
//! DAG IR while parsing (the IR *is* the AST in graph form). The grammar
//! implemented here is the runnable subset documented in
//! `docs/design/10-shell-syntax.md`:
//!
//! ```text
//! scope      := IDENT ':' body ';'
//! anonymous  := ':' body ';' IDENT?
//! body       := (source | ref-expr) (transform | branch | sink | hook)*
//! source     := 'open' PATH | 'stream' IDENT
//! ref-expr   := IDENT (('+' IDENT)+ | ('&' IDENT))?   // merge / join
//! transform  := '|?' expr | '|>' proj+ | '|#' field (AGG ':' field)*
//!             | ('take'|'limit'|'head') INT | 'sort' IDENT ('asc'|'desc')?
//!             | 'distinct' IDENT* | '|' 'map' block
//!   proj     := IDENT ('as' IDENT)? | '(' expr ')' 'as' IDENT  // computed cols
//!   expr     := … cmp over add(+,-) over mul(*,/,%) over primary; '(' expr ')'
//!               AGG := 'sum' | 'avg' | 'min' | 'max'   (count is always emitted)
//! branch     := '->' IDENT ':' body ';'
//! sink       := 'save' PATH | 'print'
//! hook       := 'on' EVENT ('severity' '>=' SEV)? ':' action ';'
//! ```

mod lexer;

use lexer::{Comment, Lexer, Tok};
use rivus_core::{DataType, Mode, Resource, RivusError, Severity, TimeUnit, Value};
use rivus_ir::{
    Access, AggFunc, ArithOp, BinType, CmpOp, Codec, Discovery, Disposition, EdgeKind, Endian,
    Expr, FillMethod, Func, Hook, HookAction, HookEvent, JoinKind, NodeId, Op, PlanGraph,
    Provenance, Transport,
};

pub fn parse(src: &str) -> Result<PlanGraph, RivusError> {
    let (toks, comments) = Lexer::new(src).tokenize().map_err(RivusError::Parse)?;
    let mut p = Parser {
        toks,
        comments,
        comment_cursor: 0,
        pos: 0,
        g: PlanGraph::new(),
        last_dt_fmt: None,
        apply_site: 0,
    };
    p.parse_program()?;
    Ok(p.g)
}

struct Parser {
    toks: Vec<(Tok, u32)>,
    /// Comment trivia from the lexer, each tagged with the token index it
    /// precedes (§25.7). Consumed in order via `comment_cursor` as the parser
    /// advances, and attached to the node whose statement they lead.
    comments: Vec<Comment>,
    comment_cursor: usize,
    pos: usize,
    g: PlanGraph,
    /// Scratch: the `:datetime("fmt")` format captured by the most recent
    /// `finish_type` call (design 23). `parse_decl_schema` takes it after each
    /// column so the format can be carried on `OpenCsv.dt_formats`. `None` for a
    /// bare `:datetime` (auto-infer) or any non-datetime type.
    last_dt_fmt: Option<String>,
    /// Monotonic id for each `| name` apply site (§25.4), stamped on the nodes it
    /// splices so `to_source` can collapse the run back to `| name`.
    apply_site: u32,
}

impl Parser {
    fn tok(&self) -> &Tok {
        &self.toks[self.pos].0
    }

    fn line(&self) -> u32 {
        self.toks[self.pos].1
    }

    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].0.clone();
        if self.pos + 1 < self.toks.len() {
            self.pos += 1;
        }
        t
    }

    fn at(&self, t: &Tok) -> bool {
        self.tok() == t
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.at(t) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: &Tok) -> Result<(), RivusError> {
        if self.eat(t) {
            Ok(())
        } else {
            Err(self.err(format!("expected {t:?}, found {:?}", self.tok())))
        }
    }

    fn err(&self, msg: impl Into<String>) -> RivusError {
        RivusError::Parse(format!("line {}: {}", self.line(), msg.into()))
    }

    fn word(&mut self) -> Result<String, RivusError> {
        match self.bump() {
            Tok::Word(w) => Ok(w),
            other => Err(self.err(format!("expected identifier, found {other:?}"))),
        }
    }

    /// Read a source/sink path token. Like [`word`], but also accepts a bare
    /// `-` (which the lexer tokenizes as `Minus`) as the stdin/stdout sentinel,
    /// so `open -` / `save -` work like `open stdin` / `save stdout`.
    fn path_word(&mut self) -> Result<String, RivusError> {
        match self.bump() {
            Tok::Word(w) => Ok(w),
            Tok::Minus => Ok("-".to_string()),
            other => Err(self.err(format!("expected a path, found {other:?}"))),
        }
    }

    fn peek_is_word(&self, w: &str) -> bool {
        matches!(self.tok(), Tok::Word(x) if x == w)
    }

    /// Take all pending comment trivia that precede the current token position,
    /// in source order (§25.7). Called at each statement boundary so the run of
    /// comments leading a step is attached to that step's node.
    fn take_leading_comments(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        while self.comment_cursor < self.comments.len()
            && self.comments[self.comment_cursor].0 <= self.pos
        {
            out.push(self.comments[self.comment_cursor].1.clone());
            self.comment_cursor += 1;
        }
        out
    }

    /// Parse zero or more value-hole bindings `name=value …` (§25.3), as used by
    /// `| flow min=0 max=120`. A binding is only consumed when a `name` is
    /// followed by `=`, so a trailing transform word (no `=`) ends the list.
    /// Values are plain literals (int / decimal / string / bool) — never source
    /// fragments — so a binding can only ever supply a value (injection-safe).
    fn parse_hole_bindings(&mut self) -> Result<Vec<(String, Value)>, RivusError> {
        let mut out = Vec::new();
        while let Tok::Word(k) = self.tok().clone() {
            if self.toks[self.pos + 1].0 != Tok::Assign {
                break; // a bare word with no `=` is the next transform, not a binding
            }
            self.bump(); // name
            self.bump(); // `=`
                         // An optional leading `-` (lexed as the bare word "-" outside parens)
                         // makes the literal negative, e.g. `min=-5`.
            let neg = matches!(self.tok(), Tok::Word(w) if w == "-");
            if neg {
                self.bump();
            }
            let v = match self.bump() {
                Tok::Int(n) => Value::I64(if neg { -n } else { n }),
                Tok::Float(_, d) => Value::Dec(if neg {
                    rivus_core::Decimal::new(-d.unscaled, d.scale)
                } else {
                    d
                }),
                Tok::Str(s) if !neg => Value::Str(s),
                Tok::Word(w) if !neg && w == "true" => Value::Bool(true),
                Tok::Word(w) if !neg && w == "false" => Value::Bool(false),
                other => {
                    return Err(self.err(format!(
                        "binding `{k}=` expects a literal value (int/decimal/\"string\"/bool), \
                         found {other:?}"
                    )))
                }
            };
            out.push((k, v));
        }
        Ok(out)
    }

    // ----------------------------------------------------------------- program

    fn parse_program(&mut self) -> Result<(), RivusError> {
        while !self.at(&Tok::Eof) {
            self.parse_top_item()?;
        }
        Ok(())
    }

    fn parse_top_item(&mut self) -> Result<(), RivusError> {
        match self.tok().clone() {
            // `Name: ... ;`
            Tok::Word(name) if self.toks[self.pos + 1].0 == Tok::Colon => {
                self.bump(); // name
                self.bump(); // ':'
                let tail = self.parse_body(None)?;
                self.expect(&Tok::Semicolon)?;
                self.g.label_node(tail, name);
                Ok(())
            }
            // `: ... ; Label`
            Tok::Colon => {
                self.bump();
                let tail = self.parse_body(None)?;
                self.expect(&Tok::Semicolon)?;
                if let Tok::Word(label) = self.tok().clone() {
                    self.bump();
                    self.g.label_node(tail, label);
                }
                Ok(())
            }
            // Runtime directives we accept but treat as no-ops in the MVP.
            Tok::Word(w) if matches!(w.as_str(), "monitor" | "watch" | "visualize" | "stop") => {
                self.skip_directive();
                Ok(())
            }
            other => Err(self.err(format!("expected a scope definition, found {other:?}"))),
        }
    }

    fn skip_directive(&mut self) {
        while !self.at(&Tok::Semicolon) && !self.at(&Tok::Eof) {
            self.bump();
        }
        self.eat(&Tok::Semicolon);
    }

    // -------------------------------------------------------------------- body

    /// Parse a scope body, returning the id of its tail (output) node.
    /// `input` is the upstream node for branch children (which continue an
    /// existing flow rather than starting a new source).
    fn parse_body(&mut self, input: Option<NodeId>) -> Result<NodeId, RivusError> {
        // Comments between `Label:` and the source lead the head node.
        let head_lead = self.take_leading_comments();
        let mut current = self.parse_body_head(input)?;
        self.g.nodes[current].leading_comments = head_lead;

        loop {
            // Snapshot the comments leading this step and the next node id, so
            // whatever node the matched transform creates first carries them.
            let lead = self.take_leading_comments();
            let mark = self.g.nodes.len();
            match self.tok().clone() {
                // `|? pred` / `|? a, b` (comma = AND). `where` is a readable alias.
                Tok::PipeFilter => {
                    self.bump();
                    let pred = self.parse_filter_preds()?;
                    let n = self.g.add_node(Op::Filter { pred });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "where" => {
                    self.bump();
                    let pred = self.parse_filter_preds()?;
                    let n = self.g.add_node(Op::Filter { pred });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `|! pred warn|reject|halt` — a row contract; the disposition is
                // required (no implicit default, so a silent policy is impossible).
                Tok::PipeValidate => {
                    self.bump();
                    let pred = self.parse_filter_preds()?;
                    let disposition = match self.tok().clone() {
                        Tok::Word(w) if Disposition::parse(&w).is_some() => {
                            self.bump();
                            Disposition::parse(&w).unwrap()
                        }
                        other => {
                            return Err(self.err(format!(
                                "`|!` needs a disposition (warn|reject|halt), found {other:?}"
                            )))
                        }
                    };
                    let n = self.g.add_node(Op::Validate { pred, disposition });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::PipeMap => {
                    self.bump();
                    let op = self.parse_projection()?;
                    let n = self.g.add_node(op);
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::PipeGroup => {
                    self.bump();
                    // One or more group keys, then optional aggregates. A word is
                    // an aggregate (not a key) when it's a known func immediately
                    // followed by `:` (e.g. `sum:score`); every other leading
                    // word is a key. At least one key is required.
                    let is_agg = |p: &Self| {
                        matches!(p.tok(), Tok::Word(w)
                            if AggFunc::parse(w).is_some()
                                && p.toks[p.pos + 1].0 == Tok::Colon)
                    };
                    let mut keys = Vec::new();
                    while let Tok::Word(_) = self.tok() {
                        if is_agg(self) {
                            break;
                        }
                        keys.push(self.word()?);
                    }
                    if keys.is_empty() {
                        return Err(self.err("group `|#` requires at least one key"));
                    }
                    // Aggregates: `func:col` repeated.
                    let mut aggs = Vec::new();
                    while let Tok::Word(w) = self.tok().clone() {
                        match AggFunc::parse(&w) {
                            Some(func) if self.toks[self.pos + 1].0 == Tok::Colon => {
                                self.bump(); // func
                                self.bump(); // ':'
                                let col = self.word()?;
                                aggs.push((func, col));
                            }
                            _ => break,
                        }
                    }
                    let n = self.g.add_node(Op::GroupBy { keys, aggs });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Pipe => {
                    self.bump(); // `|`
                    match self.tok().clone() {
                        // `| name` — named-flow reuse (§25.4): apply a previously
                        // defined flow's transforms to the current stream.
                        // Desugared by splicing copies of those ops (byte-identical
                        // to writing them inline); each spliced node is stamped with
                        // this apply site so `to_source` collapses the run back to
                        // `| name`.
                        Tok::Word(name) if self.g.labels.contains_key(&name) => {
                            self.bump();
                            // Optional value-hole bindings: `| clean min=0 max=120`
                            // (§25.3). Each binds a `$x` hole to a *value* — never
                            // to source text — so it cannot inject flow structure.
                            let bindings = self.parse_hole_bindings()?;
                            let bmap: std::collections::HashMap<String, Value> =
                                bindings.iter().cloned().collect();
                            let tail = self.g.labels[&name];
                            let ops = self.g.flow_transform_ops(tail);
                            let site = self.apply_site;
                            self.apply_site += 1;
                            for op in ops {
                                // Bind holes structurally as we splice (desugar is
                                // byte-identical to the inline, now-literal ops).
                                let nid = self.g.add_node(op.bind_holes(&bmap));
                                self.g.nodes[nid].applied_from =
                                    Some((site, name.clone(), bindings.clone()));
                                self.g.add_edge(current, nid, EdgeKind::Stream);
                                current = nid;
                            }
                        }
                        // `| map { ... }` — MVP: skip the block (no-op).
                        Tok::Word(w) if w == "map" => {
                            self.bump();
                            self.skip_block()?;
                        }
                        // A bare word that names no defined flow is a clear error
                        // (not a silent skip), matching the merge/join diagnostic.
                        Tok::Word(n) => {
                            return Err(self.err(format!("`| {n}`: unknown flow '{n}'")));
                        }
                        // `| { ... }` — MVP: skip the block (no-op).
                        _ => self.skip_block()?,
                    }
                }
                Tok::Arrow => {
                    // Branch: `-> Child: body ;` continuing from `current`.
                    self.bump();
                    let child_name = self.word()?;
                    self.expect(&Tok::Colon)?;
                    let child_tail = self.parse_body(Some(current))?;
                    self.expect(&Tok::Semicolon)?;
                    self.g.label_node(child_tail, child_name);
                    // `current` keeps fanning out: do not advance it.
                }
                Tok::Bang => {
                    // `Users!` materialize marker — recorded structurally as a
                    // no-op in the MVP (the boundary is implicit at a sink).
                    self.bump();
                }
                // `save PATH [as FMT]` — extension default, `as` overrides; the
                // sink mirrors the source format set (write what you can read).
                Tok::Word(w) if w == "save" => {
                    self.bump();
                    let path = norm_path(self.path_word()?);
                    let explicit = if self.peek_is_word("as") {
                        self.bump();
                        Some(self.word()?)
                    } else {
                        None
                    };
                    let delim = resolve_delim(&path, explicit.as_deref());
                    let fmt = resolve_format(&path, explicit.as_deref()).ok_or_else(|| {
                        self.err(format!("unknown format '{}'", explicit.unwrap_or_default()))
                    })?;
                    let n = self.g.add_node(fmt.into_sink_op(path, delim));
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "writecsv" => {
                    self.bump();
                    let path = self.word()?;
                    let n = self.g.add_node(Op::SinkCsv {
                        delim: rivus_ir::delim_for_path(&path),
                        path,
                    });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "writejson" => {
                    self.bump();
                    let path = self.word()?;
                    let n = self.g.add_node(Op::SinkJsonl { path });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `take N` / `limit N` / `head N` — cap the stream at N rows.
                Tok::Word(w) if w == "take" || w == "limit" || w == "head" => {
                    self.bump();
                    let n = match self.tok().clone() {
                        Tok::Int(v) if v >= 0 => {
                            self.bump();
                            v as usize
                        }
                        other => {
                            return Err(self
                                .err(format!("{w} expects a non-negative integer, got {other:?}")))
                        }
                    };
                    let node = self.g.add_node(Op::Take { n });
                    self.g.add_edge(current, node, EdgeKind::Stream);
                    current = node;
                }
                // `sort KEY [asc|desc] [KEY [asc|desc] ...]` — order by one or
                // more keys, each with its own direction (default ascending).
                Tok::Word(w) if w == "sort" => {
                    self.bump();
                    let mut keys = Vec::new();
                    while let Tok::Word(k) = self.tok().clone() {
                        if is_keyword(&k) {
                            break;
                        }
                        self.bump(); // key
                                     // `asc`/`desc` apply to the key just read; absence = asc.
                        let desc = if self.peek_is_word("desc") {
                            self.bump();
                            true
                        } else if self.peek_is_word("asc") {
                            self.bump();
                            false
                        } else {
                            false
                        };
                        keys.push((k, desc));
                    }
                    if keys.is_empty() {
                        return Err(self.err("sort expects at least one key"));
                    }
                    let n = self.g.add_node(Op::Sort { keys });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `distinct [KEY ...]` — drop duplicate rows (whole-row, or by
                // the named key columns). Bare words until the next transform.
                Tok::Word(w) if w == "distinct" => {
                    self.bump();
                    let mut keys = Vec::new();
                    while let Tok::Word(name) = self.tok().clone() {
                        if is_keyword(&name) {
                            break;
                        }
                        self.bump();
                        keys.push(name);
                    }
                    let n = self.g.add_node(Op::Distinct { keys });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `describe` — replace the stream with a per-column summary.
                Tok::Word(w) if w == "describe" => {
                    self.bump();
                    let n = self.g.add_node(Op::Describe);
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `dropna [col ...]` — drop rows with empty values.
                Tok::Word(w) if w == "dropna" => {
                    self.bump();
                    let mut cols = Vec::new();
                    while let Tok::Word(name) = self.tok().clone() {
                        if is_keyword(&name) {
                            break;
                        }
                        self.bump();
                        cols.push(name);
                    }
                    let n = self.g.add_node(Op::DropNa { cols });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `fill col VALUE|ffill|bfill` — fill empty cells of a text
                // column with a constant, or carry the last/next value over.
                Tok::Word(w) if w == "fill" => {
                    self.bump();
                    let col = self.word()?;
                    let method = match self.bump() {
                        Tok::Word(s) if s == "ffill" => FillMethod::Ffill,
                        Tok::Word(s) if s == "bfill" => FillMethod::Bfill,
                        Tok::Word(s) if s == "mean" => FillMethod::Mean,
                        Tok::Word(s) if s == "median" => FillMethod::Median,
                        Tok::Str(s) => FillMethod::Value(s),
                        Tok::Word(s) => FillMethod::Value(s),
                        Tok::Int(n) => FillMethod::Value(n.to_string()),
                        Tok::Float(f, _) => FillMethod::Value(f.to_string()),
                        other => {
                            return Err(self.err(format!("fill expects a value, found {other:?}")))
                        }
                    };
                    let n = self.g.add_node(Op::Fill { col, method });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `drop COL [COL ...]` — remove the named columns.
                Tok::Word(w) if w == "drop" => {
                    self.bump();
                    let mut cols = Vec::new();
                    while let Tok::Word(name) = self.tok().clone() {
                        if is_keyword(&name) {
                            break;
                        }
                        self.bump();
                        cols.push(name);
                    }
                    if cols.is_empty() {
                        return Err(self.err("drop expects at least one column name"));
                    }
                    let n = self.g.add_node(Op::Drop { cols });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `cast COL:type [COL:type ...]` — re-type named columns in place.
                Tok::Word(w) if w == "cast" => {
                    self.bump();
                    let mut casts = Vec::new();
                    while let Tok::Word(name) = self.tok().clone() {
                        if is_keyword(&name) {
                            break;
                        }
                        self.bump(); // column name
                        self.expect(&Tok::Colon)?;
                        let tyword = self.word()?;
                        let ty = self.finish_type(&tyword)?;
                        casts.push((name, ty));
                    }
                    if casts.is_empty() {
                        return Err(self.err("cast expects at least one COL:type"));
                    }
                    let n = self.g.add_node(Op::Cast { casts });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `reorder COL [COL ...]` — move named columns to the front.
                Tok::Word(w) if w == "reorder" => {
                    self.bump();
                    let mut cols = Vec::new();
                    while let Tok::Word(name) = self.tok().clone() {
                        if is_keyword(&name) {
                            break;
                        }
                        self.bump();
                        cols.push(name);
                    }
                    if cols.is_empty() {
                        return Err(self.err("reorder expects at least one column name"));
                    }
                    let n = self.g.add_node(Op::Reorder { cols });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                // `rename OLD NEW [OLD NEW ...]` — rename columns in place.
                Tok::Word(w) if w == "rename" => {
                    self.bump();
                    let mut pairs = Vec::new();
                    while let Tok::Word(from) = self.tok().clone() {
                        if is_keyword(&from) {
                            break;
                        }
                        self.bump();
                        let to = self.word()?;
                        pairs.push((from, to));
                    }
                    if pairs.is_empty() {
                        return Err(self.err("rename expects `OLD NEW` column pairs"));
                    }
                    let n = self.g.add_node(Op::Rename { pairs });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "print" => {
                    self.bump();
                    let n = self.g.add_node(Op::SinkPrint);
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "on" => {
                    let hook = self.parse_hook()?;
                    self.g.add_hook(current, hook);
                }
                Tok::Semicolon | Tok::Eof => break,
                other => return Err(self.err(format!("unexpected token in flow: {other:?}"))),
            }
            // Attach the step's leading comments to the first node it created.
            // Node-creating arms fall through here; `break`/hook arms do not
            // (so trailing comments before `;` and comments before a hook are
            // not preserved in this phase — a documented MVP limit).
            if self.g.nodes.len() > mark && !lead.is_empty() {
                self.g.nodes[mark].leading_comments = lead;
            }
        }
        Ok(current)
    }

    /// Parse the first element of a body: a source, a stream replay, a
    /// merge/join over named scopes, or (for branch children) the inherited
    /// Parse a declared column schema `( name[:type] name[:type] … )` for
    /// `open`. Space-separated (like `readbin`); a type fixes that column's
    /// lane, otherwise it is inferred. Types: `int`/`i64`, `float`/`f64`,
    /// `str`/`string`, `bool`.
    #[allow(clippy::type_complexity)]
    fn parse_decl_schema(
        &mut self,
    ) -> Result<(Vec<(String, Option<DataType>)>, Vec<(String, String)>), RivusError> {
        self.expect(&Tok::LParen)?;
        let mut cols = Vec::new();
        let mut dt_formats = Vec::new();
        while !self.at(&Tok::RParen) && !self.at(&Tok::Eof) {
            let name = self.word()?;
            let ty = if self.eat(&Tok::Colon) {
                let t = self.word()?;
                let dt = self.finish_type(&t)?;
                // A `:datetime("fmt")` column carries its explicit parse format
                // out-of-band (it is not part of the Copy lane tag). Design 23.
                if let Some(fmt) = self.last_dt_fmt.take() {
                    dt_formats.push((name.clone(), fmt));
                }
                Some(dt)
            } else {
                None
            };
            cols.push((name, ty));
        }
        self.expect(&Tok::RParen)?;
        if cols.is_empty() {
            return Err(self.err("declared schema `( … )` needs at least one column"));
        }
        Ok((cols, dt_formats))
    }

    /// Resolve a type annotation after its leading word has been consumed. Plain
    /// lanes (`int`/`f64`/`str`/`bool`) come straight from [`decl_type`];
    /// `decimal` additionally requires a `(N)` scale suffix (`decimal(2)`) — the
    /// exact fixed-point lane (design 21). Bare `decimal` (auto-scale) is not yet
    /// wired in the reader, so it is a clear error rather than a silent default.
    fn finish_type(&mut self, word: &str) -> Result<DataType, RivusError> {
        // Reset the datetime-format scratch; only a `datetime("fmt")` sets it.
        self.last_dt_fmt = None;
        if word.eq_ignore_ascii_case("datetime") {
            // Optional explicit strptime format: `datetime("yyMMddhhmmss")`. A
            // bare `datetime` auto-infers common formats at read time (design 23).
            // The unit is `Sec` in the MVP (sub-second tokens come later).
            if self.eat(&Tok::LParen) {
                match self.bump() {
                    Tok::Str(fmt) => self.last_dt_fmt = Some(fmt),
                    other => {
                        return Err(self.err(format!(
                            "datetime(\"fmt\"): expected a quoted format string, found {other:?}"
                        )))
                    }
                }
                self.expect(&Tok::RParen)?;
            }
            return Ok(DataType::DateTime {
                unit: TimeUnit::Sec,
            });
        }
        if word.eq_ignore_ascii_case("duration") {
            // Signed tick span, read from the human `HH:MM:SS[.frac]` form
            // (design 23 / #57). Unit is `Sec` in the MVP.
            return Ok(DataType::Duration {
                unit: TimeUnit::Sec,
            });
        }
        if word.eq_ignore_ascii_case("date") {
            // Calendar date, read from ISO `yyyy-MM-dd` (i32 epoch-day; #58).
            return Ok(DataType::Date);
        }
        if word.eq_ignore_ascii_case("time") {
            // Time-of-day, read from `HH:mm:ss[.frac]` (i64 ticks; #58, MVP Sec).
            return Ok(DataType::Time);
        }
        if word.eq_ignore_ascii_case("decimal") {
            if !self.eat(&Tok::LParen) {
                return Err(self.err("decimal needs a scale: write decimal(N), e.g. decimal(2)"));
            }
            let scale = match self.bump() {
                Tok::Int(v) if (0..=38).contains(&v) => v as u8,
                other => {
                    return Err(self.err(format!(
                        "decimal(N): N must be an integer 0..=38, found {other:?}"
                    )))
                }
            };
            self.expect(&Tok::RParen)?;
            Ok(DataType::Decimal { scale })
        } else {
            decl_type(word).ok_or_else(|| self.err(format!("unknown column type '{word}'")))
        }
    }

    /// Optional source-provenance modifier (design §28.6): `with source` (rides
    /// the origin handle on chunk metadata) or `with filename` (sugar: also a
    /// `filename` column). `with` is only used here, so an unrecognized follower
    /// is an error rather than silently ignored.
    fn parse_provenance(&mut self) -> Result<Provenance, RivusError> {
        if !self.peek_is_word("with") {
            return Ok(Provenance::Off);
        }
        match &self.toks[self.pos + 1].0 {
            Tok::Word(w) if w == "source" => {
                self.bump();
                self.bump();
                Ok(Provenance::Source)
            }
            Tok::Word(w) if w == "filename" => {
                self.bump();
                self.bump();
                Ok(Provenance::Filename)
            }
            other => Err(self.err(format!(
                "`with` expects `source` or `filename`, found {other:?}"
            ))),
        }
    }

    /// upstream node.
    fn parse_body_head(&mut self, input: Option<NodeId>) -> Result<NodeId, RivusError> {
        match self.tok().clone() {
            // `open PATH [as FMT]` — extension is only the default; an explicit
            // `as csv|tsv|json|jsonl|ndjson` overrides it (and works when the
            // path has no/odd extension). `readcsv`/`readjson`/`readbin` are
            // equivalent explicit aliases (lower cognitive load, fewer surprises).
            Tok::Word(w) if w == "open" => {
                self.bump();
                let path = norm_path(self.path_word()?);
                let explicit = if self.peek_is_word("as") {
                    self.bump();
                    Some(self.word()?)
                } else {
                    None
                };
                // Optional `noheader`: the file has no header row (CSV only).
                let noheader = self.peek_is_word("noheader");
                if noheader {
                    self.bump();
                }
                // Optional declared schema `(col[:type] ...)` (CSV only).
                let (decl, dtf) = if self.at(&Tok::LParen) {
                    let (cols, dtf) = self.parse_decl_schema()?;
                    (Some(cols), dtf)
                } else {
                    (None, Vec::new())
                };
                // Optional `with source` / `with filename` (any format, §28.6).
                let prov = self.parse_provenance()?;
                let delim = resolve_delim(&path, explicit.as_deref());
                let fmt = resolve_format(&path, explicit.as_deref()).ok_or_else(|| {
                    self.err(format!("unknown format '{}'", explicit.unwrap_or_default()))
                })?;
                let mut op = fmt.into_op(path, delim);
                // Layer the parsed read config / provenance onto the fresh source.
                if let Op::Source {
                    codec, provenance, ..
                } = &mut op
                {
                    *provenance = prov;
                    if let Codec::Csv {
                        header,
                        declared,
                        dt_formats,
                        ..
                    } = codec
                    {
                        if noheader {
                            *header = false;
                        }
                        *declared = decl;
                        *dt_formats = dtf;
                    }
                }
                Ok(self.g.add_node(op))
            }
            Tok::Word(w) if w == "readcsv" => {
                self.bump();
                let path = norm_path(self.path_word()?);
                let provenance = self.parse_provenance()?;
                let delim = rivus_ir::delim_for_path(&path);
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Fixed(path),
                    transport: Transport::Local,
                    codec: Codec::csv(delim),
                    provenance,
                }))
            }
            Tok::Word(w) if w == "readjson" => {
                self.bump();
                let path = norm_path(self.path_word()?);
                let provenance = self.parse_provenance()?;
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Fixed(path),
                    transport: Transport::Local,
                    codec: Codec::Jsonl,
                    provenance,
                }))
            }
            Tok::Word(w) if w == "stream" => {
                self.bump();
                let name = self.word()?;
                Ok(self.g.add_node(Op::StreamRef { name }))
            }
            // `ls "glob"` — discovery-as-flow (§28.3): enumerate files matching the
            // glob into a stream of file rows (`path`/`name`/`size`/`mtime`). The
            // pattern is a string literal (`**` recurses); no codec decode.
            // Aliases `gci` / `dir` (PowerShell), verb-only (no `Verb-Noun`).
            Tok::Word(w) if w == "ls" || w == "gci" || w == "dir" => {
                self.bump();
                let pattern = match self.bump() {
                    Tok::Str(s) => s,
                    other => {
                        return Err(
                            self.err(format!("ls expects a quoted glob pattern, found {other:?}"))
                        )
                    }
                };
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Glob(pattern),
                    transport: Transport::Local,
                    codec: Codec::discover(),
                    provenance: Provenance::Off,
                }))
            }
            // `readbin path [le|be] [packed|aligned] (name:type ...)`.
            Tok::Word(w) if w == "readbin" => {
                self.bump();
                let path = self.word()?;
                let mut endian = Endian::Little;
                let mut c_align = false;
                loop {
                    match self.tok() {
                        Tok::Word(m) if m == "le" => {
                            endian = Endian::Little;
                            self.bump();
                        }
                        Tok::Word(m) if m == "be" => {
                            endian = Endian::Big;
                            self.bump();
                        }
                        Tok::Word(m) if m == "packed" => {
                            c_align = false;
                            self.bump();
                        }
                        Tok::Word(m) if m == "aligned" => {
                            c_align = true;
                            self.bump();
                        }
                        _ => break,
                    }
                }
                self.expect(&Tok::LParen)?;
                let mut fields = Vec::new();
                while !self.at(&Tok::RParen) && !self.at(&Tok::Eof) {
                    let name = self.word()?;
                    self.expect(&Tok::Colon)?;
                    let ty = self.word()?;
                    let bt = BinType::parse(&ty)
                        .ok_or_else(|| self.err(format!("unknown binary type '{ty}'")))?;
                    fields.push((name, bt));
                }
                self.expect(&Tok::RParen)?;
                if fields.is_empty() {
                    return Err(self.err("readbin requires at least one field"));
                }
                let provenance = self.parse_provenance()?;
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Fixed(path),
                    transport: Transport::Local,
                    codec: Codec::Binary {
                        fields,
                        endian,
                        c_align,
                    },
                    provenance,
                }))
            }
            // Reference to a named scope → merge (`+`) or join (`&`).
            Tok::Word(name) if self.g.labels.contains_key(&name) => {
                let first = self.g.labels[&name];
                self.bump();
                if self.at(&Tok::Plus) {
                    let merge = self.g.add_node(Op::Merge);
                    self.g.add_edge(first, merge, EdgeKind::Stream);
                    while self.eat(&Tok::Plus) {
                        let nm = self.word()?;
                        let id = *self
                            .g
                            .labels
                            .get(&nm)
                            .ok_or_else(|| self.err(format!("unknown flow '{nm}'")))?;
                        self.g.add_edge(id, merge, EdgeKind::Stream);
                    }
                    Ok(merge)
                } else if self.eat(&Tok::Amp) {
                    // `&` is an inner join; `&left`/`&right`/`&full` are outer
                    // joins (the qualifier lexes as a bare word right after `&`).
                    let kind = if self.peek_is_word("left") {
                        self.bump();
                        JoinKind::Left
                    } else if self.peek_is_word("right") {
                        self.bump();
                        JoinKind::Right
                    } else if self.peek_is_word("full") {
                        self.bump();
                        JoinKind::Full
                    } else {
                        JoinKind::Inner
                    };
                    let rhs = self.word()?;
                    let rid = *self
                        .g
                        .labels
                        .get(&rhs)
                        .ok_or_else(|| self.err(format!("unknown flow '{rhs}'")))?;
                    // `on k [k ...]` — each key is `lkey` (same name both sides)
                    // or `lkey:rkey`. One or more pairs form a composite key.
                    if !self.peek_is_word("on") {
                        return Err(self.err("join `A & B` requires `on <key>` (or `on lk:rk`)"));
                    }
                    self.bump(); // `on`
                    let mut left_keys = Vec::new();
                    let mut right_keys = Vec::new();
                    while let Tok::Word(w) = self.tok().clone() {
                        if is_keyword(&w) {
                            break;
                        }
                        let lk = self.word()?;
                        let rk = if self.eat(&Tok::Colon) {
                            self.word()?
                        } else {
                            lk.clone()
                        };
                        left_keys.push(lk);
                        right_keys.push(rk);
                    }
                    if left_keys.is_empty() {
                        return Err(self.err("join `on` requires at least one key"));
                    }
                    let join = self.g.add_node(Op::Join {
                        left_keys,
                        right_keys,
                        kind,
                    });
                    self.g.add_edge(first, join, EdgeKind::Stream);
                    self.g.add_edge(rid, join, EdgeKind::Stream);
                    Ok(join)
                } else {
                    // Bare reference (e.g. anonymous scope label assignment).
                    Ok(first)
                }
            }
            _ => {
                // Branch child / continuation: flow inherits the parent node.
                input.ok_or_else(|| self.err("expected a source (open/stream) or flow reference"))
            }
        }
    }

    /// Parse a `|>` projection list. Items are bare fields (`name`), renames
    /// (`name as alias`), or computed columns (`(expr) as alias`). When every
    /// item is a bare field this lowers to the pure-selection `Op::Project`
    /// (so existing fusion/pushdown are untouched); otherwise to `ProjectExpr`.
    fn parse_projection(&mut self) -> Result<Op, RivusError> {
        let mut items: Vec<(Expr, String)> = Vec::new();
        let mut all_bare = true;
        loop {
            match self.tok().clone() {
                // `(expr) as alias` — computed column.
                Tok::LParen => {
                    let e = self.parse_primary()?; // consumes the parenthesized expr
                    if !self.peek_is_word("as") {
                        return Err(self.err("computed projection `(expr)` requires `as <name>`"));
                    }
                    self.bump(); // `as`
                    let alias = self.word()?;
                    items.push((e, alias));
                    all_bare = false;
                }
                // `name` or `name as alias`.
                Tok::Word(w) if !is_keyword(&w) => {
                    self.bump();
                    if self.peek_is_word("as") {
                        self.bump();
                        let alias = self.word()?;
                        items.push((Expr::field(&w), alias));
                        all_bare = false;
                    } else {
                        items.push((Expr::field(&w), w));
                    }
                }
                _ => break,
            }
        }
        if items.is_empty() {
            return Err(self.err("`|>` requires at least one field or computed column"));
        }
        if all_bare {
            let fields = items.into_iter().map(|(_, alias)| alias).collect();
            Ok(Op::Project { fields })
        } else {
            Ok(Op::ProjectExpr { items })
        }
    }

    fn skip_block(&mut self) -> Result<(), RivusError> {
        self.expect(&Tok::LBrace)?;
        let mut depth = 1;
        while depth > 0 {
            match self.bump() {
                Tok::LBrace => depth += 1,
                Tok::RBrace => depth -= 1,
                Tok::Eof => return Err(self.err("unterminated `{ ... }` block")),
                _ => {}
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------- hooks

    fn parse_hook(&mut self) -> Result<Hook, RivusError> {
        self.bump(); // 'on'
        let ev = self.word()?;
        let event =
            HookEvent::parse(&ev).ok_or_else(|| self.err(format!("unknown hook event '{ev}'")))?;

        let mut min_severity = None;
        if self.peek_is_word("severity") {
            self.bump();
            self.expect(&Tok::Cmp(CmpOp::Ge))?;
            let sev = self.word()?;
            min_severity = Some(
                Severity::parse(&sev)
                    .ok_or_else(|| self.err(format!("unknown severity '{sev}'")))?,
            );
        }
        self.expect(&Tok::Colon)?;

        let action = self.parse_hook_action()?;
        // Consume any trailing tokens up to the hook terminator.
        while !self.at(&Tok::Semicolon) && !self.at(&Tok::Eof) {
            self.bump();
        }
        self.expect(&Tok::Semicolon)?;
        Ok(Hook {
            event,
            min_severity,
            action,
        })
    }

    fn parse_hook_action(&mut self) -> Result<HookAction, RivusError> {
        match self.tok().clone() {
            Tok::Word(w) if w == "transition" => {
                self.bump();
                let m = self.word()?;
                Ok(HookAction::Transition(
                    parse_mode(&m).ok_or_else(|| self.err(format!("unknown mode '{m}'")))?,
                ))
            }
            Tok::Word(w) if w == "log" => {
                self.bump();
                match self.bump() {
                    Tok::Str(s) => Ok(HookAction::Log(s)),
                    other => Err(self.err(format!("log expects a string, found {other:?}"))),
                }
            }
            // `route Errors` or bare `Errors`
            Tok::Word(w) if w == "route" || w == "reroute" => {
                self.bump();
                Ok(HookAction::Route(self.word()?))
            }
            Tok::Word(name) => {
                self.bump();
                Ok(HookAction::Route(name))
            }
            other => Err(self.err(format!("unexpected hook action {other:?}"))),
        }
    }

    // -------------------------------------------------------------- expressions

    fn parse_expr(&mut self) -> Result<Expr, RivusError> {
        self.parse_or()
    }

    /// A filter predicate, optionally comma-separated where `,` means AND —
    /// `|? age >= 20, country == "JP"` reads better than chained `and`.
    fn parse_filter_preds(&mut self) -> Result<Expr, RivusError> {
        let mut pred = self.parse_expr()?;
        while self.eat(&Tok::Comma) {
            let rhs = self.parse_expr()?;
            pred = Expr::And(Box::new(pred), Box::new(rhs));
        }
        Ok(pred)
    }

    fn parse_or(&mut self) -> Result<Expr, RivusError> {
        let mut e = self.parse_and()?;
        while self.peek_is_word("or") {
            self.bump();
            let rhs = self.parse_and()?;
            e = Expr::Or(Box::new(e), Box::new(rhs));
        }
        Ok(e)
    }

    fn parse_and(&mut self) -> Result<Expr, RivusError> {
        let mut e = self.parse_cmp()?;
        while self.peek_is_word("and") {
            self.bump();
            let rhs = self.parse_cmp()?;
            e = Expr::And(Box::new(e), Box::new(rhs));
        }
        Ok(e)
    }

    fn parse_cmp(&mut self) -> Result<Expr, RivusError> {
        let left = self.parse_add()?;
        if let Tok::Cmp(op) = self.tok().clone() {
            self.bump();
            let right = self.parse_add()?;
            Ok(Expr::Compare {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else if self.at(&Tok::Assign) {
            // A lone `=` in a predicate is almost always a `==` typo (the bare
            // `=` is reserved for value-hole bindings, `| flow k=v`).
            Err(self.err("unexpected '=' in a predicate (did you mean '=='?)"))
        } else {
            Ok(left)
        }
    }

    /// Additive level: `+` / `-` (left-associative, lower precedence than `*`).
    fn parse_add(&mut self) -> Result<Expr, RivusError> {
        let mut e = self.parse_mul()?;
        loop {
            let op = match self.tok() {
                Tok::Plus => ArithOp::Add,
                Tok::Minus => ArithOp::Sub,
                _ => break,
            };
            self.bump();
            let right = self.parse_mul()?;
            e = Expr::Arith {
                left: Box::new(e),
                op,
                right: Box::new(right),
            };
        }
        Ok(e)
    }

    /// Multiplicative level: `*` / `/` / `%` (binds tighter than `+`/`-`).
    fn parse_mul(&mut self) -> Result<Expr, RivusError> {
        let mut e = self.parse_cast()?;
        loop {
            let op = match self.tok() {
                Tok::Star => ArithOp::Mul,
                Tok::Slash => ArithOp::Div,
                Tok::Percent => ArithOp::Mod,
                _ => break,
            };
            self.bump();
            let right = self.parse_cast()?;
            e = Expr::Arith {
                left: Box::new(e),
                op,
                right: Box::new(right),
            };
        }
        Ok(e)
    }

    /// A primary with an optional trailing type cast `:type` (binds tightest),
    /// e.g. `age:int`, `(price + tax):f64`. Only a recognized type word after
    /// `:` is a cast; otherwise the `:` is left for the caller.
    fn parse_cast(&mut self) -> Result<Expr, RivusError> {
        let e = self.parse_primary()?;
        if self.at(&Tok::Colon) {
            if let Tok::Word(w) = self.toks[self.pos + 1].0.clone() {
                // A recognized lane word, or `decimal` (which carries a `(N)`
                // suffix), turns `:` into a cast; otherwise leave `:` for the caller.
                if decl_type(&w).is_some() || w.eq_ignore_ascii_case("decimal") {
                    self.bump(); // ':'
                    self.bump(); // type word
                    let ty = self.finish_type(&w)?;
                    return Ok(Expr::Cast {
                        expr: Box::new(e),
                        ty,
                    });
                }
            }
        }
        Ok(e)
    }

    fn parse_primary(&mut self) -> Result<Expr, RivusError> {
        match self.tok().clone() {
            // Parenthesized sub-expression (and the entry to expression mode for
            // arithmetic, which the lexer only tokenizes inside parens).
            Tok::LParen => {
                self.bump();
                let e = self.parse_expr()?;
                self.expect(&Tok::RParen)?;
                Ok(e)
            }
            Tok::Int(n) => {
                self.bump();
                Ok(Expr::Literal(Value::I64(n)))
            }
            Tok::Float(_, d) => {
                // A written decimal literal keeps its exact value (so a compare
                // against a `decimal` column never rounds it); it still rides the
                // f64 lane via `Decimal::to_f64()` everywhere else.
                self.bump();
                Ok(Expr::Literal(Value::Dec(d)))
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Literal(Value::Str(s)))
            }
            Tok::DollarCur | Tok::DollarStack(_) => {
                self.bump();
                self.parse_field_tail()
            }
            // `$name` — a value hole (§25.3), filled by a binding / parameter.
            Tok::Hole(name) => {
                self.bump();
                Ok(Expr::Hole(name))
            }
            Tok::Word(w) if w == "true" => {
                self.bump();
                Ok(Expr::Literal(Value::Bool(true)))
            }
            Tok::Word(w) if w == "false" => {
                self.bump();
                Ok(Expr::Literal(Value::Bool(false)))
            }
            // `item("field")` — dynamic resolution.
            Tok::Word(w) if w == "item" => {
                self.bump();
                self.expect(&Tok::LParen)?;
                let name = match self.bump() {
                    Tok::Str(s) => s,
                    other => {
                        return Err(self.err(format!("item() expects a string, found {other:?}")))
                    }
                };
                self.expect(&Tok::RParen)?;
                Ok(Expr::Field {
                    name,
                    access: Access::Dynamic,
                })
            }
            // `resource(EXPR)` — a Resource handle (design §28.1). A bare string
            // literal stays a first-class Resource *literal* (uri = the string;
            // `size`/`mtime` are discovery-filled later, §00 0.14). Any other
            // expression (a column, a computed path) is a **cast to resource** —
            // `resource(filepath)` ≡ `filepath:resource` — so a manifest column or
            // a computed uri becomes a handle stream that `read` consumes (3c).
            Tok::Word(w) if w == "resource" => {
                self.bump();
                self.expect(&Tok::LParen)?;
                let e = self.parse_expr()?;
                self.expect(&Tok::RParen)?;
                Ok(match e {
                    Expr::Literal(Value::Str(s)) => {
                        Expr::Literal(Value::Resource(Resource::new(s)))
                    }
                    other => Expr::Cast {
                        expr: Box::new(other),
                        ty: DataType::Resource,
                    },
                })
            }
            // Scalar function call `func(args…)` — e.g. `upper(name)`,
            // `substr(name, 0, 3)`, `contains(city, "NY")`.
            Tok::Word(ref w)
                if Func::parse(w).is_some() && self.toks[self.pos + 1].0 == Tok::LParen =>
            {
                let func = Func::parse(w).unwrap();
                self.bump(); // func name
                self.expect(&Tok::LParen)?;
                let mut args = Vec::new();
                if !self.at(&Tok::RParen) {
                    args.push(self.parse_expr()?);
                    while self.eat(&Tok::Comma) {
                        args.push(self.parse_expr()?);
                    }
                }
                self.expect(&Tok::RParen)?;
                Ok(Expr::Func { func, args })
            }
            // `case when COND then VAL [when …] [else VAL] end` conditional.
            Tok::Word(ref w) if w == "case" => {
                self.bump(); // case
                self.parse_case()
            }
            // `source.field` — provenance accessor (design §28.6): a field of the
            // chunk's origin Resource (`source.uri` / `source.scheme`). `source`
            // is reserved only when followed by `.field`; a bare `source` (no
            // dot) stays an ordinary column reference, so an actual `source`
            // column is still reachable. Inside parens the lexer tokenizes
            // `source.uri` as `Word("source") Dot Word("uri")` (identifiers are
            // dot-free in expression mode), so the `.field` tail is parsed here.
            Tok::Word(ref w) if w == "source" && self.toks[self.pos + 1].0 == Tok::Dot => {
                self.bump(); // source
                self.expect(&Tok::Dot)?;
                let name = self.word()?;
                Ok(Expr::Field {
                    name,
                    access: Access::Source,
                })
            }
            // Bare field of the current object: `age`. Outside parens (flow mode)
            // the lexer folds `a.b` into a single identifier, so a dotted bare word
            // here is almost always a mis-placed handle accessor (`source.uri`,
            // §28.6) or a dotted column name. Rather than silently build a field
            // literally named `a.b` (never-silent + it would not round-trip), it is
            // an explicit error: handle fields go in a computed column `|> (…)`, and
            // a genuinely dotted column name is reached with `item("a.b")`.
            Tok::Word(name) => {
                self.bump();
                if name.contains('.') {
                    return Err(self.err(format!(
                        "dotted field `{name}` is ambiguous here; use it in a computed column \
                         `|> (…)` (e.g. `source.uri`), or `item(\"{name}\")` for a real column"
                    )));
                }
                Ok(Expr::Field {
                    name,
                    access: Access::Fast,
                })
            }
            other => Err(self.err(format!("unexpected token in expression: {other:?}"))),
        }
    }

    /// Parse the tail of `case when COND then VAL [when COND then VAL ...]
    /// [else VAL] end` (the `case` keyword is already consumed). At least one
    /// `when` branch is required; `else` is optional (defaults to "" at eval).
    fn parse_case(&mut self) -> Result<Expr, RivusError> {
        let mut branches = Vec::new();
        while self.peek_is_word("when") {
            self.bump(); // when
            let cond = self.parse_expr()?;
            if !self.peek_is_word("then") {
                return Err(self.err("case: expected `then` after a `when` condition"));
            }
            self.bump(); // then
            let val = self.parse_expr()?;
            branches.push((cond, val));
        }
        if branches.is_empty() {
            return Err(self.err("case: expected at least one `when … then …` branch"));
        }
        let default = if self.peek_is_word("else") {
            self.bump();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        if !self.peek_is_word("end") {
            return Err(self.err("case: expected `end` to close the expression"));
        }
        self.bump(); // end
        Ok(Expr::Case { branches, default })
    }

    /// After consuming `$_` / `$_:N`, read the field accessor tail.
    fn parse_field_tail(&mut self) -> Result<Expr, RivusError> {
        if self.eat(&Tok::DotDot) {
            let name = self.word()?;
            Ok(Expr::Field {
                name,
                access: Access::Deep,
            })
        } else if self.eat(&Tok::Dot) {
            let name = self.word()?;
            Ok(Expr::Field {
                name,
                access: Access::Fast,
            })
        } else {
            Err(self.err("expected '.field' or '..field' after $_"))
        }
    }
}

/// Normalize a source/sink path: `stdin` / `stdout` / `-` all map to the `-`
/// sentinel (read stdin / write stdout, direction inferred from source vs sink).
/// Map a declared column type name to a `DataType` lane.
fn decl_type(s: &str) -> Option<DataType> {
    Some(match s.to_ascii_lowercase().as_str() {
        "int" | "i64" | "integer" => DataType::I64,
        "float" | "f64" | "double" => DataType::F64,
        "str" | "string" | "text" => DataType::Str,
        "bool" | "boolean" => DataType::Bool,
        // I/O resource handle (§28.1): enables the `expr:resource` cast (and so
        // the `resource(EXPR)` computed form) and a `(col:resource)` declaration.
        "resource" => DataType::Resource,
        _ => return None,
    })
}

fn norm_path(p: String) -> String {
    if p == "stdin" || p == "stdout" || p == "-" {
        "-".to_string()
    } else {
        p
    }
}

/// A text source format selectable on `open` (binary goes via `readbin`).
enum Format {
    Csv,
    Jsonl,
    /// JSON array output (`[{…},{…}]`). On the *read* side it behaves like
    /// `Jsonl` (the JSON reader already accepts both an array and NDJSON).
    Json,
}

impl Format {
    fn into_op(self, path: String, delim: u8) -> Op {
        let codec = match self {
            Format::Csv => Codec::csv(delim),
            // The JSON reader accepts both NDJSON and a top-level array, so both
            // surface forms open the same source.
            Format::Jsonl | Format::Json => Codec::Jsonl,
        };
        Op::source(path, codec)
    }

    fn into_sink_op(self, path: String, delim: u8) -> Op {
        match self {
            Format::Csv => Op::SinkCsv { path, delim },
            Format::Jsonl => Op::SinkJsonl { path },
            Format::Json => Op::SinkJson { path },
        }
    }
}

/// Resolve the field delimiter for `open`/`save`: an explicit `as csv|tsv` wins
/// (`tsv` → tab, `csv` → comma); otherwise it follows the path extension
/// (`.tsv`/`.tab` → tab, else comma). JSON formats ignore this.
fn resolve_delim(path: &str, explicit: Option<&str>) -> u8 {
    match explicit.map(|f| f.to_ascii_lowercase()).as_deref() {
        Some("tsv") => b'\t',
        Some("csv") => rivus_ir::COMMA,
        _ => rivus_ir::delim_for_path(path),
    }
}

/// Resolve the format for `open`: an explicit `as FMT` wins; otherwise fall back
/// to the file extension; otherwise default to CSV. Returns `None` only for an
/// unrecognized explicit format name.
fn resolve_format(path: &str, explicit: Option<&str>) -> Option<Format> {
    if let Some(f) = explicit {
        return match f.to_ascii_lowercase().as_str() {
            "csv" | "tsv" => Some(Format::Csv),
            // `json` = a single JSON array; `jsonl`/`ndjson` = one object per line.
            "json" => Some(Format::Json),
            "jsonl" | "ndjson" => Some(Format::Jsonl),
            _ => None,
        };
    }
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
        Some(Format::Jsonl)
    } else if lower.ends_with(".json") {
        Some(Format::Json) // a `.json` file is conventionally a JSON array
    } else {
        Some(Format::Csv) // default
    }
}

fn is_keyword(w: &str) -> bool {
    matches!(
        w,
        "open"
            | "readbin"
            | "readcsv"
            | "readjson"
            | "as"
            | "noheader"
            | "writecsv"
            | "writejson"
            | "stream"
            | "save"
            | "print"
            | "take"
            | "limit"
            | "head"
            | "sort"
            | "distinct"
            | "describe"
            | "dropna"
            | "fill"
            | "drop"
            | "cast"
            | "reorder"
            | "rename"
            | "where"
            | "on"
            | "map"
            | "mode"
            | "stop"
            | "monitor"
            | "watch"
            | "visualize"
            | "transition"
            | "log"
            | "route"
            | "reroute"
    )
}

fn parse_mode(s: &str) -> Option<Mode> {
    Some(match s {
        "normal" => Mode::Normal,
        "degraded" => Mode::Degraded,
        "recovery" => Mode::Recovery,
        "isolation" => Mode::Isolation,
        "emergency" => Mode::Emergency,
        "halted" => Mode::Halted,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rivus_ir::Op;

    fn first_op(src: &str) -> Op {
        let g = parse(src).unwrap();
        g.nodes[0].op.clone()
    }

    fn nth_op(src: &str, n: usize) -> Op {
        let g = parse(src).unwrap();
        g.nodes[n].op.clone()
    }

    #[test]
    fn block_comments_are_inert_trivia_and_dont_change_the_graph() {
        // `#{ ... }#` (and `# ...`) are comments: removing them leaves the same
        // ops. The comment must not be absorbed into any token (e.g. a path).
        let with = parse(
            "F:\n #{ scope note }#\n open d.csv\n # keep adults\n |? age >= 20\n |> name age\n;",
        )
        .unwrap();
        let without = parse("F:\n open d.csv\n |? age >= 20\n |> name age\n;").unwrap();
        assert_eq!(with.nodes.len(), without.nodes.len());
        for (a, b) in with.nodes.iter().zip(without.nodes.iter()) {
            assert_eq!(a.op.kind_str(), b.op.kind_str());
        }
    }

    #[test]
    fn comments_attach_as_leading_trivia_to_the_following_step() {
        let g =
            parse("F:\n #{ source }#\n open d.csv\n # adults only\n |? age >= 20\n |> name age\n;")
                .unwrap();
        // head (open) carries the scope-leading block comment
        assert_eq!(
            g.nodes[0].leading_comments,
            vec!["#{ source }#".to_string()]
        );
        // the filter carries the line comment that preceded it
        assert_eq!(
            g.nodes[1].leading_comments,
            vec!["# adults only".to_string()]
        );
        // the project had no comment
        assert!(g.nodes[2].leading_comments.is_empty());
    }

    #[test]
    fn comment_trivia_round_trips_through_to_source_and_is_idempotent() {
        let src = "F:\n #{ note }#\n open d.csv\n # adults\n |? age >= 20\n |> name age\n;";
        let once = parse(src).unwrap().to_source();
        // The comments survive the IR round-trip.
        assert!(once.contains("#{ note }#"), "block comment lost: {once}");
        assert!(once.contains("# adults"), "line comment lost: {once}");
        // fmt is idempotent: formatting the formatted source is a fixed point,
        // and the comments are still there.
        let twice = parse(&once).unwrap().to_source();
        assert_eq!(once, twice, "to_source is not idempotent");
        assert!(twice.contains("#{ note }#") && twice.contains("# adults"));
    }

    #[test]
    fn comma_filter_is_and_and_where_is_an_alias() {
        // `|? a, b`, `|? a and b`, and `where a, b` all lower to the same
        // Filter(And(a, b)).
        let want = |op: &Op| {
            matches!(
                op,
                Op::Filter {
                    pred: Expr::And(..)
                }
            )
        };
        assert!(want(&nth_op(
            "F:\n open d.csv\n |? age >= 20, country == \"JP\"\n;",
            1
        )));
        assert!(want(&nth_op(
            "F:\n open d.csv\n |? age >= 20 and country == \"JP\"\n;",
            1
        )));
        assert!(want(&nth_op(
            "F:\n open d.csv\n where age >= 20, country == \"JP\"\n;",
            1
        )));
    }

    #[test]
    fn projection_pure_vs_computed_lowering() {
        // All bare fields → pure Op::Project (keeps fusion/pushdown intact).
        assert!(matches!(
            nth_op("F:\n open a.csv\n |> name age\n;", 1),
            Op::Project { .. }
        ));
        // Any computed item → Op::ProjectExpr.
        match nth_op("F:\n open a.csv\n |> name (age * 12) as months\n;", 1) {
            Op::ProjectExpr { items } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[1].1, "months");
                assert!(matches!(items[1].0, Expr::Arith { .. }));
            }
            other => panic!("expected ProjectExpr, got {other:?}"),
        }
    }

    #[test]
    fn computed_projection_round_trips() {
        // Reversibility: source → IR → source must re-parse to the same source.
        let src = "F:\n open a.csv\n |> name (age + 2 * 10) as v\n;";
        let g1 = parse(src).unwrap();
        let s1 = g1.to_source();
        let g2 = parse(&s1).unwrap();
        assert_eq!(s1, g2.to_source(), "computed projection not reversible");
        // Precedence preserved as (age + (2 * 10)), aliased to v.
        assert!(
            s1.contains("($_.age + (2 * 10)) as v"),
            "unexpected reversed source: {s1}"
        );
    }

    #[test]
    fn scalar_funcs_parse_and_round_trip() {
        use rivus_ir::Func;
        // Each name maps to its Func variant.
        for (name, want) in [
            ("replace", Func::Replace),
            ("split_part", Func::SplitPart),
            ("concat", Func::Concat),
            ("abs", Func::Abs),
            ("round", Func::Round),
            ("floor", Func::Floor),
            ("ceil", Func::Ceil),
            ("coalesce", Func::Coalesce),
        ] {
            assert_eq!(Func::parse(name), Some(want), "parse {name}");
        }
        // String + numeric + coalesce funcs survive source -> IR -> source.
        let src = "F:\n open a.csv\n |> (abs(v)) as a (round(v)) as r (floor(v)) as fl (ceil(v)) as c (coalesce(name, \"NA\")) as nm (replace(p, \"/\", \"-\")) as rp\n;";
        let s = parse(src).unwrap().to_source();
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        for needle in ["abs(", "round(", "floor(", "ceil(", "coalesce(", "replace("] {
            assert!(s.contains(needle), "missing {needle} in {s}");
        }
    }

    #[test]
    fn resource_literal_round_trips_uri_only() {
        // `resource("uri")` is a first-class handle literal (§28.1): it parses to
        // a Resource value and survives source -> IR -> source as its uri (the uri
        // is the in-contract identity; metadata is never emitted, §00 0.14).
        let src = "F:\n open a.csv\n |> (resource(\"file:///data/a.csv\")) as src\n;";
        let g = parse(src).unwrap();
        let s = g.to_source();
        assert!(
            s.contains("resource(\"file:///data/a.csv\")"),
            "resource literal lost in {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
    }

    #[test]
    fn resource_expr_is_a_cast_literal_stays_literal() {
        // `resource(EXPR)` (§28.3, slice 3c): a string literal stays a Resource
        // literal; any other expression is a cast to resource (`resource(col)` ≡
        // `col:resource`) — for manifest / computed-path handle streams.
        let g = parse(
            "M:\n open m.csv\n |> (resource(filepath)) as path (resource(\"file:///x\")) as fixed\n;",
        )
        .unwrap();
        let items = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::ProjectExpr { items } => Some(items),
                _ => None,
            })
            .expect("computed projection");
        // computed path → Cast(_, Resource); literal → Resource literal.
        assert!(matches!(
            &items[0].0,
            Expr::Cast {
                ty: DataType::Resource,
                ..
            }
        ));
        assert!(matches!(&items[1].0, Expr::Literal(Value::Resource(_))));
        let s = g.to_source();
        assert!(
            s.contains(":resource"),
            "computed resource cast lost in {s}"
        );
        assert!(
            s.contains("resource(\"file:///x\")"),
            "resource literal lost in {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
    }

    #[test]
    fn provenance_modifiers_parse_and_round_trip() {
        // `with source` / `with filename` parse on every format and survive
        // source -> IR -> source (the modifier is inert until slice 2-②; here we
        // fix the syntax + `to_source` reversibility). §28.6.
        for (src, needle) in [
            ("F:\n open a.csv with source\n |> id\n;", "with source"),
            (
                "F:\n open a.csv (id:int v:str) with filename\n |> id\n;",
                "with filename",
            ),
            (
                "F:\n readbin a.bin (x:i32) with source\n |> x\n;",
                "with source",
            ),
        ] {
            let s = parse(src).unwrap().to_source();
            assert!(s.contains(needle), "missing `{needle}` in {s}");
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
        // `with` followed by garbage is an error, not silently ignored.
        assert!(parse("F:\n open a.csv with wat\n |> id\n;").is_err());
    }

    #[test]
    fn source_accessor_parses_and_round_trips() {
        // `source.<field>` (§28.6) parses to a Field with Access::Source and the
        // bare field name (the `.uri`/`.scheme` is generic, not baked in), and
        // survives source -> IR -> source as `(source.uri) as …`.
        let src =
            "F:\n open a.csv with source\n |> id (source.uri) as path (source.scheme) as sch\n;";
        let g = parse(src).unwrap();
        // The two accessors resolve to generic Resource-field references.
        let proj = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::ProjectExpr { items } => Some(items),
                _ => None,
            })
            .expect("computed projection");
        assert!(matches!(
            &proj[1].0,
            Expr::Field { name, access: Access::Source } if name == "uri"
        ));
        assert!(matches!(
            &proj[2].0,
            Expr::Field { name, access: Access::Source } if name == "scheme"
        ));
        let s = g.to_source();
        assert!(
            s.contains("(source.uri) as path") && s.contains("(source.scheme) as sch"),
            "source accessor lost in {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");

        // A bare `source` (no `.field`) stays an ordinary column reference, so an
        // actual column named `source` is still reachable (the accessor reserves
        // `source` only when a `.field` follows).
        let bare = parse("F:\n open a.csv\n |> (source) as s\n;").unwrap();
        let bproj = bare
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::ProjectExpr { items } => Some(items),
                _ => None,
            })
            .expect("computed projection");
        assert!(matches!(
            &bproj[0].0,
            Expr::Field { name, access: Access::Fast } if name == "source"
        ));
    }

    #[test]
    fn ls_discovery_parses_and_round_trips() {
        // `ls "glob"` (§28.3) → a discovery source (Glob + Codec::Discover) that
        // emits ordinary file columns; `name`/`size` are *bare* fields (no handle
        // accessor), so predicate + projection round-trip cleanly.
        let src = "L:\n ls \"logs/**/*.csv\"\n |? size > 1000\n |> path name\n;";
        let g = parse(src).unwrap();
        assert!(matches!(
            &g.nodes[0].op,
            Op::Source {
                discovery: Discovery::Glob(p),
                codec: Codec::Discover { .. },
                ..
            } if p == "logs/**/*.csv"
        ));
        let s = g.to_source();
        assert!(s.contains("ls \"logs/**/*.csv\""), "ls lost in {s}");
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        // Aliases `gci` / `dir` parse to the same discovery codec (verb-only).
        for alias in ["gci", "dir"] {
            let a = parse(&format!("A:\n {alias} \"*.csv\"\n;")).unwrap();
            assert!(matches!(
                &a.nodes[0].op,
                Op::Source {
                    codec: Codec::Discover { .. },
                    ..
                }
            ));
        }
    }

    #[test]
    fn dotted_bare_field_is_rejected_outside_a_computed_column() {
        // A handle accessor (`source.uri`) belongs in a computed column; a bare
        // dotted field in a predicate is an explicit error (never-silent +
        // reversibility), not a silently-built field named "source.uri".
        assert!(parse("F:\n open a.csv with source\n |? source.uri == \"x\"\n;").is_err());
        assert!(parse("L:\n ls \"*.csv\"\n |? path.name == \"a\"\n;").is_err());
        // In a computed column it is the provenance accessor (works, slice 2).
        assert!(parse("F:\n open a.csv with source\n |> (source.uri) as u\n;").is_ok());
    }

    #[test]
    fn decimal_literal_is_preserved_exactly() {
        // A written decimal literal must survive to_source as itself (its exact
        // text), not as an f64 re-render — so a compare against a decimal column
        // never rounds it. `19.995` and `0.305` are the accounting-contract cases.
        for (src, lit) in [
            ("F:\n open s.csv\n |? amount > 19.995\n;", "19.995"),
            ("F:\n open s.csv\n |? amount == 0.305\n;", "0.305"),
            ("F:\n open s.csv\n |? x < 100.00\n;", "100.00"),
        ] {
            let g = parse(src).unwrap();
            let s = g.to_source();
            assert!(s.contains(lit), "literal {lit} lost in {s}");
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
    }

    #[test]
    fn decimal_type_parses_and_is_reversible() {
        // `decimal(N)` in a declared schema and as an expression/verb cast.
        for src in [
            "F:\n open sales.csv (id amount:decimal(2))\n |> id amount\n;",
            "F:\n open sales.csv\n |> id (amount:decimal(4)) as a\n;",
            "F:\n open sales.csv\n cast amount:decimal(3)\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("decimal("), "decimal type lost in {s}");
        }
        // Bare `decimal` (auto-scale) is a clear error, not a silent default.
        assert!(parse("F:\n open s.csv (a:decimal)\n;").is_err());
        // A non-integer / out-of-range scale is rejected.
        assert!(parse("F:\n open s.csv (a:decimal(x))\n;").is_err());
    }

    #[test]
    fn datetime_type_parses_and_is_reversible() {
        // Explicit `:datetime("fmt")` and bare `:datetime` (auto-infer), both in
        // a declared schema and as a cast (design 23).
        for src in [
            "F:\n open log.csv (ts:datetime(\"yyMMddHHmmss\") msg)\n |> ts msg\n;",
            "F:\n open log.csv (ts:datetime id:int)\n |> ts\n;",
            "F:\n open log.csv\n cast ts:datetime\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("datetime"), "datetime type lost in {s}");
        }
        // The explicit format string survives verbatim through to_source.
        let s = parse("F:\n open log.csv (ts:datetime(\"yyMMddHHmmss\"))\n;")
            .unwrap()
            .to_source();
        assert!(s.contains("yyMMddHHmmss"), "format lost in {s}");
        // The format must be carried on the OpenCsv op (not just the type tag).
        match &parse("F:\n open log.csv (ts:datetime(\"yyyy-MM-dd\"))\n;")
            .unwrap()
            .nodes[0]
            .op
        {
            Op::Source {
                codec: Codec::Csv { dt_formats, .. },
                ..
            } => {
                assert_eq!(dt_formats, &[("ts".to_string(), "yyyy-MM-dd".to_string())]);
            }
            o => panic!("expected a CSV source, got {o:?}"),
        }
        // A non-string format argument is a clear error, not a silent default.
        assert!(parse("F:\n open s.csv (a:datetime(123))\n;").is_err());
    }

    #[test]
    fn duration_type_parses_and_is_reversible() {
        // `:duration` in a declared schema and as a cast (design 23 / #57).
        for src in [
            "F:\n open log.csv (id:int elapsed:duration)\n |> elapsed\n;",
            "F:\n open log.csv\n cast elapsed:duration\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("duration"), "duration type lost in {s}");
        }
    }

    #[test]
    fn date_type_parses_and_is_reversible() {
        // `:date` in a declared schema and as a cast (#58, ISO yyyy-MM-dd lane).
        for src in [
            "F:\n open events.csv (id:int day:date)\n |> day\n;",
            "F:\n open events.csv\n cast day:date\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("date"), "date type lost in {s}");
        }
    }

    #[test]
    fn validate_op_round_trips_and_requires_disposition() {
        // `|! pred warn|reject|halt` round-trips; the disposition is required so
        // a silent policy is impossible (#83 §24).
        for (src, disp) in [
            ("F:\n open d.csv\n |! age >= 0 warn\n;", "warn"),
            (
                "F:\n open d.csv\n |! age >= 0, age <= 120 reject\n;",
                "reject",
            ),
            ("F:\n open d.csv\n |! id == 1 halt\n;", "halt"),
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("|!") && s.contains(disp), "validate lost in {s}");
        }
        assert!(
            parse("F:\n open d.csv\n |! age >= 0\n;").is_err(),
            "validate must require an explicit disposition"
        );
    }

    #[test]
    fn time_type_and_function_parse_and_are_reversible() {
        // `:time` type + the `time(x)` extractor round-trip through to_source (#58).
        for src in [
            "F:\n open log.csv (start:time)\n |> start\n;",
            "F:\n open log.csv\n |> (time(ts)) as tod\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("time"), "time lost in {s}");
        }
    }

    #[test]
    fn date_functions_parse_and_are_reversible() {
        // The #58 date extractors survive `to_source` round-trips.
        let src = "F:\n open log.csv\n |> (weekday(ts)) as wd (is_weekend(ts)) as we (date(ts)) as day\n;";
        let s = parse(src).unwrap().to_source();
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        for needle in ["weekday(", "is_weekend(", "date("] {
            assert!(s.contains(needle), "missing {needle} in {s}");
        }
    }

    #[test]
    fn datetime_functions_parse_and_are_reversible() {
        // The design-23 datetime functions survive `to_source` round-trips.
        let src = "F:\n open log.csv\n |> (year(ts)) as y (month(ts)) as mo (day(ts)) as d (hour(ts)) as h (minute(ts)) as mi (second(ts)) as se (trunc(ts, \"day\")) as bucket (format(ts, \"yyyy-MM-dd\")) as f\n;";
        let s = parse(src).unwrap().to_source();
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        for needle in [
            "year(",
            "month(",
            "day(",
            "hour(",
            "minute(",
            "second(",
            "trunc(",
            "format(",
            "\"day\"",
            "\"yyyy-MM-dd\"",
        ] {
            assert!(s.contains(needle), "missing {needle} in {s}");
        }
    }

    #[test]
    fn stdio_paths_normalize() {
        // `stdin`/`stdout` (and `-`) map to the "-" sentinel for source & sink.
        let g = parse("F:\n open stdin\n save stdout\n;").unwrap();
        match &g.nodes[0].op {
            Op::Source { discovery, .. } => assert_eq!(discovery.path(), "-"),
            o => panic!("expected a source, got {o:?}"),
        }
        let sink = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::SinkCsv { path, .. } => Some(path.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(sink, "-");
        // stdin with an explicit format.
        assert!(matches!(
            &parse("F:\n open stdin as json\n;").unwrap().nodes[0].op,
            Op::Source { discovery, codec: Codec::Jsonl, .. } if discovery.path() == "-"
        ));
    }

    #[test]
    fn bare_dash_is_stdio_sentinel() {
        // `open -` / `save -` lex the lone dash as the stdin/stdout sentinel
        // (distinct from `->` branch and expression `-`), mapping to "-".
        let g = parse("F:\n open -\n |> name\n save -\n;").unwrap();
        match &g.nodes[0].op {
            Op::Source { discovery, .. } => assert_eq!(discovery.path(), "-"),
            o => panic!("expected a source, got {o:?}"),
        }
        let sink = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::SinkCsv { path, .. } => Some(path.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(sink, "-");
        // `->` branch must still tokenize as a branch, not a dash word.
        assert!(parse("F:\n open a.csv\n -> Kids:\n |? age < 18\n ;\n;").is_ok());
    }

    #[test]
    fn sink_format_selection() {
        // The sink mirrors the source format set: extension default + `as` + aliases.
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.csv\n;", 1),
            Op::SinkCsv { .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.jsonl\n;", 1),
            Op::SinkJsonl { .. }
        ));
        // `as json` (and a `.json` extension) is a JSON *array*; `.jsonl` /
        // `.ndjson` / `as jsonl` stay one-object-per-line.
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.dat as json\n;", 1),
            Op::SinkJson { .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.json\n;", 1),
            Op::SinkJson { .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.dat as jsonl\n;", 1),
            Op::SinkJsonl { .. }
        ));
        // `writejson` keeps emitting NDJSON (backward-compatible).
        assert!(matches!(
            nth_op("F:\n open a.csv\n writejson o.x\n;", 1),
            Op::SinkJsonl { .. }
        ));
        // Round-trip: `save o.json` (array) and `save o.jsonl` survive to_source.
        for prog in [
            "F:\n open a.csv\n save o.json\n;",
            "F:\n open a.csv\n save o.jsonl\n;",
        ] {
            let s = parse(prog).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "round-trip: {s}");
        }
        assert!(matches!(
            nth_op("F:\n open a.csv\n writecsv o.x\n;", 1),
            Op::SinkCsv { .. }
        ));
    }

    #[test]
    fn format_selection_extension_alias_and_override() {
        // Default: extension picks the format (codec).
        assert!(matches!(
            first_op("F:\n open d.csv\n;"),
            Op::Source {
                codec: Codec::Csv { .. },
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n open d.jsonl\n;"),
            Op::Source {
                codec: Codec::Jsonl,
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n open d.ndjson\n;"),
            Op::Source {
                codec: Codec::Jsonl,
                ..
            }
        ));
        // Odd/absent extension defaults to CSV...
        assert!(matches!(
            first_op("F:\n open d.dat\n;"),
            Op::Source {
                codec: Codec::Csv { .. },
                ..
            }
        ));
        // ...but `as FMT` overrides the extension entirely.
        assert!(matches!(
            first_op("F:\n open d.dat as json\n;"),
            Op::Source {
                codec: Codec::Jsonl,
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n open d.jsonl as csv\n;"),
            Op::Source {
                codec: Codec::Csv { .. },
                ..
            }
        ));
        // Explicit aliases ignore the extension.
        assert!(matches!(
            first_op("F:\n readjson d.weird\n;"),
            Op::Source {
                codec: Codec::Jsonl,
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n readcsv d.weird\n;"),
            Op::Source {
                codec: Codec::Csv { .. },
                ..
            }
        ));
        // Unknown explicit format is an error.
        assert!(parse("F:\n open d.x as toml\n;").is_err());
    }

    #[test]
    fn parses_linear_scope() {
        let src = "Users:\n    open users.csv\n    |? age >= 20\n    |> name\n;";
        let g = parse(src).unwrap();
        assert!(g.labels.contains_key("Users"));
        let src2 = g.to_source();
        assert!(src2.contains("open users.csv"));
        assert!(src2.contains("|? $_.age >= 20"));
    }

    #[test]
    fn parses_branch_and_merge() {
        let src = "\
Users:
    open users.csv
    -> Adults:
        |? age >= 20
    ;
    -> Minors:
        |? age < 20
    ;
;
Merged:
    Adults + Minors
;";
        let g = parse(src).unwrap();
        assert!(g.labels.contains_key("Adults"));
        assert!(g.labels.contains_key("Minors"));
        assert!(g.labels.contains_key("Merged"));
        // The merge node has two inputs.
        let merged = g.labels["Merged"];
        assert_eq!(g.inputs_of(merged).len(), 2);
    }

    /// A structural fingerprint of a graph (node ops in id order, sorted edge
    /// endpoints, sorted label set) flattened to a string. Two graphs equal here
    /// are the same DAG up to rendering.
    fn fingerprint(g: &PlanGraph) -> String {
        let ops: Vec<&str> = g.nodes.iter().map(|n| n.op.kind_str()).collect();
        let mut edges: Vec<(NodeId, NodeId, bool)> = g
            .edges
            .iter()
            .map(|e| (e.from, e.to, e.kind == EdgeKind::Stream))
            .collect();
        edges.sort_unstable();
        let mut labels: Vec<String> = g.labels.keys().cloned().collect();
        labels.sort();
        format!("ops={ops:?} edges={edges:?} labels={labels:?}")
    }

    #[test]
    fn to_source_round_trips_a_branch_dag() {
        // `->` fan-out must round-trip: parse → to_source → parse is the same
        // DAG (faithful branch rendering, not the old `... ;` placeholder), and
        // formatting is idempotent.
        let src = "\
Users:
    open users.csv
    |? active == true
    -> Adults:
        |? age >= 20
        |> name age
    ;
    -> Minors:
        |? age < 20
    ;
;";
        let g1 = parse(src).unwrap();
        let rendered = g1.to_source();
        // The faithful form is emitted (no lossy placeholder), and re-parses.
        assert!(rendered.contains("-> Adults:"), "rendered:\n{rendered}");
        assert!(
            !rendered.contains("..."),
            "lossy placeholder leaked:\n{rendered}"
        );
        let g2 = parse(&rendered).unwrap();
        assert_eq!(
            fingerprint(&g1),
            fingerprint(&g2),
            "branch DAG changed across to_source round-trip:\n{rendered}"
        );
        // Idempotent.
        assert_eq!(
            rendered,
            g2.to_source(),
            "to_source not idempotent on a branch"
        );
    }

    /// The ordered op chain ending at `tail`, walked back to the source.
    fn chain_ops(g: &PlanGraph, tail: NodeId) -> Vec<String> {
        let mut ops = Vec::new();
        let mut cur = tail;
        loop {
            ops.push(format!("{:?}", g.nodes[cur].op));
            let ins = g.inputs_of(cur);
            if ins.len() != 1 {
                break;
            }
            cur = ins[0];
        }
        ops.reverse();
        ops
    }

    #[test]
    fn named_flow_apply_desugars_byte_identical_to_inline() {
        // `| clean` (§25.4) splices `clean`'s transforms; the resulting op chain
        // must be *identical* to writing those transforms inline — and an
        // identical op sequence executes observationally identically (the engine
        // is deterministic per op chain), which is the byte-identity contract.
        let applied = parse(
            "clean:\n open c.csv\n |? age >= 20\n |! id >= 1 warn\n |> id age\n;\n\
             R:\n open d.csv\n | clean\n |# country\n;",
        )
        .unwrap();
        let inline =
            parse("R:\n open d.csv\n |? age >= 20\n |! id >= 1 warn\n |> id age\n |# country\n;")
                .unwrap();
        assert_eq!(
            chain_ops(&applied, applied.labels["R"]),
            chain_ops(&inline, inline.labels["R"]),
            "`| clean` desugar is not op-identical to the inline transforms"
        );
    }

    #[test]
    fn named_flow_apply_round_trips_as_pipe_name() {
        // `| clean` survives to_source as `| clean` (not the desugared ops), and
        // re-parses to the same graph; idempotent.
        let src = "clean:\n open c.csv\n |? age >= 20\n |> id age\n;\n\
                   R:\n open d.csv\n | clean\n |# country\n;";
        let g1 = parse(src).unwrap();
        let rendered = g1.to_source();
        assert!(rendered.contains("| clean"), "lost `| clean`:\n{rendered}");
        let g2 = parse(&rendered).unwrap();
        assert_eq!(
            chain_ops(&g1, g1.labels["R"]),
            chain_ops(&g2, g2.labels["R"]),
            "graph changed across `| clean` round-trip:\n{rendered}"
        );
        assert_eq!(rendered, g2.to_source(), "not idempotent:\n{rendered}");
    }

    #[test]
    fn unknown_named_flow_apply_is_an_error() {
        // `| nope` with no such flow defined is a parse error (not a silent skip).
        let err = parse("R:\n open d.csv\n | nope\n;").unwrap_err();
        assert!(
            format!("{err:?}").contains("unknown flow 'nope'"),
            "expected an `unknown flow` diagnostic, got {err:?}"
        );
    }

    #[test]
    fn negative_value_hole_binding() {
        // `min=-5` binds a negative literal value (parsed, not a structure), and
        // round-trips through to_source with its sign.
        let g = parse("clean:\n open c.csv\n |? x >= $min\n;\nR:\n open d.csv\n | clean min=-5\n;")
            .unwrap();
        let ops = chain_ops(&g, g.labels["R"]);
        assert!(
            ops.iter().any(|o| o.contains("I64(-5)")),
            "negative binding not bound to -5: {ops:?}"
        );
        assert!(
            g.to_source().contains("min=-5"),
            "negative sign lost in to_source"
        );
    }

    #[test]
    fn string_binding_with_quotes_round_trips_escaped() {
        // A bound string containing a `"` must escape on to_source so it re-parses.
        let src = "clean:\n open c.csv\n |> (concat(a, $s)) as b\n;\n\
                   R:\n open d.csv\n | clean s=\"a\\\"b\"\n;";
        let g1 = parse(src).unwrap();
        let rendered = g1.to_source();
        let g2 = parse(&rendered).unwrap();
        assert_eq!(
            chain_ops(&g1, g1.labels["R"]),
            chain_ops(&g2, g2.labels["R"]),
            "escaped string binding broke round-trip:\n{rendered}"
        );
        assert_eq!(rendered, g2.to_source(), "not idempotent:\n{rendered}");
    }

    #[test]
    fn lone_equals_in_predicate_hints_at_double_equals() {
        // The bare `=` (now the binding token) must still give a helpful hint when
        // mistyped for `==` in a predicate.
        let err = parse("R:\n open d.csv\n |? age = 5\n;").unwrap_err();
        assert!(
            format!("{err:?}").contains("did you mean '=='"),
            "expected a `did you mean ==` hint, got {err:?}"
        );
    }

    #[test]
    fn value_hole_parses_and_round_trips() {
        // `$min` is a value hole that survives to_source as `$min` (§25.3).
        let g = parse("R:\n open d.csv\n |? age >= $min\n;").unwrap();
        let rendered = g.to_source();
        assert!(rendered.contains("$min"), "lost the hole:\n{rendered}");
        assert_eq!(
            rendered,
            parse(&rendered).unwrap().to_source(),
            "not idempotent"
        );
    }

    #[test]
    fn bound_hole_desugars_byte_identical_to_an_inline_literal() {
        // `| clean min=20` over `clean: … |? age >= $min` must produce the *same*
        // op chain as writing `|? age >= 20` inline — the binding is structural
        // (hole → value literal), so the desugar is byte-identical.
        let applied =
            parse("clean:\n open c.csv\n |? age >= $min\n;\nR:\n open d.csv\n | clean min=20\n;")
                .unwrap();
        let inline = parse("R:\n open d.csv\n |? age >= 20\n;").unwrap();
        assert_eq!(
            chain_ops(&applied, applied.labels["R"]),
            chain_ops(&inline, inline.labels["R"]),
            "bound-hole desugar is not identical to the inline literal"
        );
    }

    #[test]
    fn hole_binding_round_trips_with_its_values() {
        // `| clean min=20 tag="vip"` round-trips the bindings (not just `| clean`).
        let src = "clean:\n open c.csv\n |? age >= $min\n |> (concat(name, $tag)) as who\n;\n\
                   R:\n open d.csv\n | clean min=20 tag=\"vip\"\n;";
        let g1 = parse(src).unwrap();
        let rendered = g1.to_source();
        assert!(
            rendered.contains("| clean min=20 tag=\"vip\""),
            "bindings lost:\n{rendered}"
        );
        let g2 = parse(&rendered).unwrap();
        assert_eq!(
            chain_ops(&g1, g1.labels["R"]),
            chain_ops(&g2, g2.labels["R"]),
            "bound apply changed across round-trip:\n{rendered}"
        );
        assert_eq!(rendered, g2.to_source(), "not idempotent:\n{rendered}");
    }

    #[test]
    fn hole_binding_is_injection_safe() {
        // A binding value is *only* a value: text that looks like flow syntax is
        // kept verbatim as a string literal, never parsed as structure. So the
        // applied chain has exactly clean's transform count — no injected ops.
        let evil = "x >= 0 |? 1 == 1 ; DROP";
        let src = format!(
            "clean:\n open c.csv\n |> (concat(name, $note)) as label\n;\n\
             R:\n open d.csv\n | clean note=\"{evil}\"\n;"
        );
        let g = parse(&src).unwrap();
        // R = open + exactly one spliced ProjectExpr (clean's single transform).
        let ops = chain_ops(&g, g.labels["R"]);
        assert_eq!(ops.len(), 2, "injection changed the op count: {ops:?}");
        // The hole resolved to the verbatim string, not parsed tokens.
        assert!(
            ops[1].contains(evil),
            "the binding value was not kept verbatim: {:?}",
            ops[1]
        );
    }

    #[test]
    fn adjacent_same_name_applies_round_trip_separately() {
        // `| clean | clean` must NOT collapse into a single `| clean` — distinct
        // apply sites keep distinct site_ids, so to_source re-emits both, and the
        // doubled transforms re-parse to the same op chain.
        let src = "clean:\n open c.csv\n |? age >= 20\n;\nR:\n open d.csv\n | clean\n | clean\n;";
        let g1 = parse(src).unwrap();
        let rendered = g1.to_source();
        assert_eq!(
            rendered.matches("| clean").count(),
            2,
            "two applies must render as two `| clean`:\n{rendered}"
        );
        let g2 = parse(&rendered).unwrap();
        assert_eq!(
            chain_ops(&g1, g1.labels["R"]),
            chain_ops(&g2, g2.labels["R"]),
            "doubled apply changed across round-trip:\n{rendered}"
        );
        assert_eq!(rendered, g2.to_source(), "not idempotent:\n{rendered}");
    }

    #[test]
    fn named_flow_apply_drops_the_recipe_sink() {
        // A reuse recipe that ends in a sink must contribute only its transforms;
        // `| clean` never drags clean's `save …` along (footgun fix).
        let g = parse(
            "clean:\n open c.csv\n |? age >= 20\n save out.csv\n;\nR:\n open d.csv\n | clean\n;",
        )
        .unwrap();
        let r_ops = chain_ops(&g, g.labels["R"]);
        assert!(
            !r_ops.iter().any(|o| o.contains("Sink")),
            "the recipe's sink leaked into `| clean`: {r_ops:?}"
        );
    }

    #[test]
    fn to_source_round_trips_a_single_branch_and_nested_branches() {
        // Two shapes that previously did NOT round-trip:
        //  (a) fan-out of one (`-> Only:`) — a single-output parent was absorbed
        //      into the child chain and re-rendered as a duplicated source;
        //  (b) a nested branch (depth > 1) — `write_chain` recurses, so a
        //      grandchild must round-trip too.
        for src in [
            "U:\n open u.csv\n -> Only: |? age >= 20 ;\n;",
            "U:\n open u.csv\n -> A:\n   |? age >= 20\n   -> A1: |? age >= 65 ;\n ;\n -> B: |? age < 20 ;\n;",
        ] {
            let g1 = parse(src).unwrap();
            let rendered = g1.to_source();
            assert!(!rendered.contains("..."), "lossy placeholder:\n{rendered}");
            let g2 = parse(&rendered).unwrap();
            assert_eq!(
                fingerprint(&g1),
                fingerprint(&g2),
                "graph changed across round-trip for:\n{src}\n--- rendered ---\n{rendered}"
            );
            assert_eq!(rendered, g2.to_source(), "not idempotent:\n{rendered}");
        }
    }

    #[test]
    fn parses_error_hook() {
        let src = "\
Import:
    open users.csv
    |? $_.age > 20
    on error severity >= warning:
        transition degraded
    ;
;";
        let g = parse(src).unwrap();
        let n = g.labels["Import"];
        assert!(!g.nodes[n].hooks.is_empty());
    }

    #[test]
    fn tsv_extension_sets_tab_delim() {
        // `.tsv`/`.tab` open as tab-delimited without an explicit `as tsv`.
        assert!(matches!(
            first_op("F:\n open d.tsv\n;"),
            Op::Source {
                codec: Codec::Csv { delim: b'\t', .. },
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n open d.tab\n;"),
            Op::Source {
                codec: Codec::Csv { delim: b'\t', .. },
                ..
            }
        ));
        // A plain `.csv` stays comma.
        assert!(matches!(
            first_op("F:\n open d.csv\n;"),
            Op::Source {
                codec: Codec::Csv { delim: b',', .. },
                ..
            }
        ));
    }

    #[test]
    fn as_tsv_overrides_extension() {
        // `as tsv` forces a tab on any path; `as csv` forces a comma back.
        assert!(matches!(
            first_op("F:\n open d.txt as tsv\n;"),
            Op::Source {
                codec: Codec::Csv { delim: b'\t', .. },
                ..
            }
        ));
        assert!(matches!(
            first_op("F:\n open d.tsv as csv\n;"),
            Op::Source {
                codec: Codec::Csv { delim: b',', .. },
                ..
            }
        ));
    }

    #[test]
    fn tsv_sink_delim_from_extension_and_override() {
        // Sinks pick up the delimiter the same way sources do.
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.tsv\n;", 1),
            Op::SinkCsv { delim: b'\t', .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.csv as tsv\n;", 1),
            Op::SinkCsv { delim: b'\t', .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.csv\n;", 1),
            Op::SinkCsv { delim: b',', .. }
        ));
    }

    #[test]
    fn tsv_round_trips_cleanly() {
        // A `.tsv` source/sink needs no modifier (extension implies tab), so the
        // regenerated source is clean and re-parses identically.
        let g = parse("F:\n open a.tsv\n |> name age\n save out.tsv\n;").unwrap();
        let src = g.to_source();
        assert!(src.contains("open a.tsv"), "got: {src}");
        assert!(src.contains("save out.tsv"), "got: {src}");
        assert!(!src.contains("as tsv"), "redundant modifier in: {src}");
        assert_eq!(src, parse(&src).unwrap().to_source());
    }

    #[test]
    fn explicit_tsv_modifier_round_trips() {
        // When the delimiter disagrees with the extension, `as tsv`/`as csv` is
        // emitted so the round-trip stays faithful.
        let g = parse("F:\n open a.txt as tsv\n save out.dat as tsv\n;").unwrap();
        let src = g.to_source();
        assert!(src.contains("open a.txt as tsv"), "got: {src}");
        assert!(src.contains("save out.dat as tsv"), "got: {src}");
        assert_eq!(src, parse(&src).unwrap().to_source());
    }

    #[test]
    fn group_parses_extended_aggregates() {
        // std / count_distinct / nunique / first / last all parse into GroupBy.
        use rivus_ir::AggFunc;
        let op = nth_op(
            "G:\n open a.csv\n |# team std:score count_distinct:p nunique:p first:p last:p\n;",
            1,
        );
        let Op::GroupBy { aggs, .. } = op else {
            panic!("expected GroupBy, got {op:?}");
        };
        let funcs: Vec<AggFunc> = aggs.iter().map(|(f, _)| *f).collect();
        assert_eq!(
            funcs,
            vec![
                AggFunc::Std,
                AggFunc::CountDistinct,
                AggFunc::CountDistinct, // nunique is an alias
                AggFunc::First,
                AggFunc::Last,
            ]
        );
    }

    #[test]
    fn group_parses_percentiles_and_round_trips() {
        // `median` is p50; `pNN` parses to Pct(NN); out-of-range / bad names fail.
        use rivus_ir::AggFunc;
        let op = nth_op("G:\n open a.csv\n |# t median:s p90:s p99:s\n;", 1);
        let Op::GroupBy { aggs, .. } = op else {
            panic!("expected GroupBy, got {op:?}");
        };
        let funcs: Vec<AggFunc> = aggs.iter().map(|(f, _)| *f).collect();
        assert_eq!(
            funcs,
            vec![AggFunc::Pct(50), AggFunc::Pct(90), AggFunc::Pct(99)]
        );
        // Reversible: median stays `median`, others render `pNN`.
        let g = parse("G:\n open a.csv\n |# t median:s p90:s\n;").unwrap();
        let s = g.to_source();
        assert!(s.contains("median:s"), "got: {s}");
        assert!(s.contains("p90:s"), "got: {s}");
        assert_eq!(s, parse(&s).unwrap().to_source());
        // `p101` is out of range → not an aggregate; the leftover `p101:s`
        // tokens are then invalid flow, so the whole program fails to parse.
        assert!(parse("G:\n open a.csv\n |# t p101:s\n;").is_err());
    }

    #[test]
    fn rename_and_drop_parse_and_round_trip() {
        // `rename OLD NEW ...` lowers to Op::Rename with ordered pairs.
        match nth_op("F:\n open a.csv\n rename age years city loc\n;", 1) {
            Op::Rename { pairs } => assert_eq!(
                pairs,
                vec![
                    ("age".to_string(), "years".to_string()),
                    ("city".to_string(), "loc".to_string()),
                ]
            ),
            o => panic!("expected Rename, got {o:?}"),
        }
        // `drop COL ...` lowers to Op::Drop.
        match nth_op("F:\n open a.csv\n drop city zip\n;", 1) {
            Op::Drop { cols } => assert_eq!(cols, vec!["city".to_string(), "zip".to_string()]),
            o => panic!("expected Drop, got {o:?}"),
        }
        // Reversible: source -> IR -> source re-parses identically.
        let g = parse("F:\n open a.csv\n rename age years\n drop city\n;").unwrap();
        let s = g.to_source();
        assert!(s.contains("rename age years"), "got: {s}");
        assert!(s.contains("drop city"), "got: {s}");
        assert_eq!(s, parse(&s).unwrap().to_source());
    }

    #[test]
    fn reorder_parses_and_round_trips() {
        match nth_op("F:\n open a.csv\n reorder city age\n;", 1) {
            Op::Reorder { cols } => {
                assert_eq!(cols, vec!["city".to_string(), "age".to_string()])
            }
            o => panic!("expected Reorder, got {o:?}"),
        }
        let s = parse("F:\n open a.csv\n reorder city age\n;")
            .unwrap()
            .to_source();
        assert!(s.contains("reorder city age"), "got: {s}");
        assert_eq!(s, parse(&s).unwrap().to_source());
        // Empty operand list is rejected.
        assert!(parse("F:\n open a.csv\n reorder\n;").is_err());
    }

    #[test]
    fn cast_verb_parses_and_round_trips() {
        match nth_op("F:\n open a.csv\n cast age:int price:f64\n;", 1) {
            Op::Cast { casts } => assert_eq!(
                casts,
                vec![
                    ("age".to_string(), DataType::I64),
                    ("price".to_string(), DataType::F64),
                ]
            ),
            o => panic!("expected Cast, got {o:?}"),
        }
        // Reversible (types render canonically: int -> i64).
        let s = parse("F:\n open a.csv\n cast age:int price:f64\n;")
            .unwrap()
            .to_source();
        assert!(s.contains("cast age:i64 price:f64"), "got: {s}");
        assert_eq!(s, parse(&s).unwrap().to_source());
        // Errors: empty list, missing type, unknown type.
        assert!(parse("F:\n open a.csv\n cast\n;").is_err());
        assert!(parse("F:\n open a.csv\n cast age\n;").is_err());
        assert!(parse("F:\n open a.csv\n cast age:wat\n;").is_err());
    }

    #[test]
    fn join_kinds_parse_and_round_trip() {
        // `&` is inner, `&left` is a left outer join; `on lk:rk` sets distinct keys.
        let g = parse("U: open u.csv ;\nO: open o.csv ;\nJ: U & O on id ;").unwrap();
        let inner = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::Join { kind, .. } => Some(*kind),
                _ => None,
            })
            .expect("a join node");
        assert_eq!(inner, JoinKind::Inner);

        let g = parse("U: open u.csv ;\nO: open o.csv ;\nJ: U &left O on uid:oid ;").unwrap();
        match g.nodes.iter().find_map(|n| match &n.op {
            Op::Join {
                kind,
                left_keys,
                right_keys,
            } => Some((*kind, left_keys.clone(), right_keys.clone())),
            _ => None,
        }) {
            Some((kind, lk, rk)) => {
                assert_eq!(kind, JoinKind::Left);
                assert_eq!(lk, vec!["uid".to_string()]);
                assert_eq!(rk, vec!["oid".to_string()]);
            }
            None => panic!("expected a join node"),
        }

        // Multi-key: `on a b` joins on the (a, b) tuple, same name both sides.
        let g = parse("U: open u.csv ;\nO: open o.csv ;\nJ: U & O on country region ;").unwrap();
        let (lk, rk) = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::Join {
                    left_keys,
                    right_keys,
                    ..
                } => Some((left_keys.clone(), right_keys.clone())),
                _ => None,
            })
            .expect("a join node");
        assert_eq!(lk, vec!["country".to_string(), "region".to_string()]);
        assert_eq!(rk, vec!["country".to_string(), "region".to_string()]);

        // right/full lower to their kinds.
        match parse("U: open u.csv ;\nO: open o.csv ;\nJ: U &right O on id ;")
            .unwrap()
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::Join { kind, .. } => Some(*kind),
                _ => None,
            }) {
            Some(k) => assert_eq!(k, JoinKind::Right),
            None => panic!("expected join"),
        }

        // Reversible: every join kind (and a distinct-key join) survives a
        // source -> IR -> source round-trip.
        for prog in [
            "U: open u.csv ;\nO: open o.csv ;\nJ: U & O on id ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U &left O on id ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U &right O on id ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U &full O on id ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U &left O on uid:oid ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U & O on country region ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U &full O on a x:y ;",
        ] {
            let s = parse(prog).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "round-trip: {s}");
        }
    }

    #[test]
    fn fill_methods_parse_and_round_trip() {
        use rivus_ir::FillMethod;
        // `fill col ffill` / `bfill` lower to the directional methods.
        match nth_op("F:\n open a.csv\n fill tag ffill\n;", 1) {
            Op::Fill { col, method } => {
                assert_eq!(col, "tag");
                assert_eq!(method, FillMethod::Ffill);
            }
            o => panic!("expected Fill, got {o:?}"),
        }
        match nth_op("F:\n open a.csv\n fill tag bfill\n;", 1) {
            Op::Fill { method, .. } => assert_eq!(method, FillMethod::Bfill),
            o => panic!("expected Fill, got {o:?}"),
        }
        // A constant value still lowers to FillMethod::Value.
        match nth_op("F:\n open a.csv\n fill tag \"NA\"\n;", 1) {
            Op::Fill { method, .. } => assert_eq!(method, FillMethod::Value("NA".to_string())),
            o => panic!("expected Fill, got {o:?}"),
        }
        // mean/median lower to the statistical methods.
        match nth_op("F:\n open a.csv\n fill score mean\n;", 1) {
            Op::Fill { method, .. } => assert_eq!(method, FillMethod::Mean),
            o => panic!("expected Fill, got {o:?}"),
        }
        match nth_op("F:\n open a.csv\n fill score median\n;", 1) {
            Op::Fill { method, .. } => assert_eq!(method, FillMethod::Median),
            o => panic!("expected Fill, got {o:?}"),
        }
        // Reversible: every method survives source -> IR -> source.
        for prog in [
            "F:\n open a.csv\n fill tag ffill\n;",
            "F:\n open a.csv\n fill tag bfill\n;",
            "F:\n open a.csv\n fill score mean\n;",
            "F:\n open a.csv\n fill score median\n;",
            "F:\n open a.csv\n fill tag \"NA\"\n;",
        ] {
            let s = parse(prog).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "round-trip: {s}");
        }
    }

    #[test]
    fn rename_and_drop_reject_empty() {
        // Both verbs require at least one operand.
        assert!(parse("F:\n open a.csv\n rename\n;").is_err());
        assert!(parse("F:\n open a.csv\n drop\n;").is_err());
    }

    #[test]
    fn case_when_parses_and_round_trips() {
        // `case when … then … [else …] end` lowers to Expr::Case and survives a
        // source -> IR -> source round-trip (re-parses identically).
        let src = "F:\n open a.csv\n |> (case when age >= 60 then \"senior\" else \"other\" end) as bucket\n;";
        let g = parse(src).unwrap();
        match &g.nodes[1].op {
            Op::ProjectExpr { items } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].1, "bucket");
                match &items[0].0 {
                    Expr::Case { branches, default } => {
                        assert_eq!(branches.len(), 1);
                        assert!(default.is_some());
                    }
                    other => panic!("expected Case, got {other:?}"),
                }
            }
            other => panic!("expected ProjectExpr, got {other:?}"),
        }
        let s1 = g.to_source();
        assert!(s1.contains("(case when"), "got: {s1}");
        assert!(s1.contains("then \"senior\""), "got: {s1}");
        assert!(s1.contains("else \"other\" end)"), "got: {s1}");
        assert_eq!(s1, parse(&s1).unwrap().to_source(), "case not reversible");
    }

    #[test]
    fn case_without_else_and_errors() {
        // `else` is optional.
        let g =
            parse("F:\n open a.csv\n |> (case when age >= 60 then \"old\" end) as b\n;").unwrap();
        assert!(matches!(&g.nodes[1].op, Op::ProjectExpr { .. }));
        // A `case` with no branch, or missing `then`/`end`, is a parse error.
        assert!(parse("F:\n open a.csv\n |> (case end) as b\n;").is_err());
        assert!(parse("F:\n open a.csv\n |> (case when age >= 1 \"x\" end) as b\n;").is_err());
        assert!(parse("F:\n open a.csv\n |> (case when age >= 1 then \"x\") as b\n;").is_err());
    }

    #[test]
    fn bare_dash_is_stdin_stdout_sentinel() {
        // `open -` / `save -` map to the "-" sentinel, like `open stdin` /
        // `save stdout` (the bare dash lexes as Minus; path_word accepts it).
        let g = parse("F:\n open -\n |> name\n save -\n;").unwrap();
        assert!(matches!(&g.nodes[0].op, Op::Source { discovery, .. } if discovery.path() == "-"));
        let sink = g
            .nodes
            .iter()
            .find_map(|n| match &n.op {
                Op::SinkCsv { path, .. } => Some(path.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(sink, "-");
    }
}
