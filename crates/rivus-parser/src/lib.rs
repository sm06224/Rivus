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
//!   proj     := IDENT (':' IDENT)? (':' TYPE)?   // §29.2 definition chain
//!             | IDENT 'as' IDENT | '(' expr ')' 'as' IDENT     // computed cols
//!   expr     := … cmp over add(+,-) over mul(*,/,%) over primary; '(' expr ')'
//!               AGG := 'sum' | 'avg' | 'min' | 'max'   (count is always emitted)
//! branch     := '->' IDENT ':' body ';'
//! sink       := 'save' PATH | 'print'
//! hook       := 'on' EVENT ('severity' '>=' SEV)? ':' action ';'
//! ```

mod lexer;

pub mod literate;

use lexer::{Comment, Lexer, Tok};
use rivus_core::{DataType, Mode, Resource, RivusError, Severity, TimeUnit, Value};
use rivus_ir::{
    is_type_word, parse_route_template, Access, AggFunc, ArithOp, BinType, CmpOp, Codec, Discovery,
    Disposition, EdgeKind, Endian, Expr, FillMethod, Func, Hook, HookAction, HookEvent, JoinKind,
    NodeId, Op, PathExpr, PathSeg, PlanGraph, Provenance, ReadFmt, Route, RouteSeg, SinkCodec,
    SubView, Transport, ViewDef,
};

/// Lower a surface key token to a [`PathExpr`] (§32 s2). A plain `name` becomes
/// the degenerate bare path (round-trips byte-for-byte and resolves on the flat
/// fast path); a dotted / indexed spelling (`user.age`, `tags[0]`) parses to a
/// nested path. A spelling that doesn't parse falls back to a bare path of the
/// whole token, so nothing is silently dropped.
fn key_path(s: String) -> PathExpr {
    PathExpr::parse(&s).unwrap_or_else(|| PathExpr::bare(s))
}

pub fn parse(src: &str) -> Result<PlanGraph, RivusError> {
    // Strip a leading UTF-8 BOM (BUG-E): editors on Windows often save a flow
    // script with a `\u{FEFF}` prefix, which would otherwise lex as an
    // `unexpected character` on line 1. The flow *script* is BOM-tolerant; data
    // files handle their own BOM in the reader.
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let (toks, comments) = Lexer::new(src).tokenize().map_err(RivusError::Parse)?;
    let mut p = Parser {
        toks,
        comments,
        comment_cursor: 0,
        pos: 0,
        g: PlanGraph::new(),
        last_dt_fmt: None,
        apply_site: 0,
        view_defs: std::collections::HashMap::new(),
    };
    p.parse_program()?;
    Ok(p.g)
}

/// Parse a `.riv.md` Literate document (§31) into a [`PlanGraph`]. The document
/// is split into frontmatter / prose / ```flow fences ([`literate::parse_literate`]);
/// the concatenated flow bodies are the executable program (§31.2), parsed by
/// [`parse`]. Prose and frontmatter carry no execution meaning (stage 1 is a
/// zero semantic change), so the resulting graph is identical to parsing the
/// equivalent `.riv`. A document with no `flow` fence is an error (never-silent).
pub fn parse_md(src: &str) -> Result<PlanGraph, RivusError> {
    let doc = literate::parse_literate(src)?;
    if !doc.has_flow() {
        return Err(RivusError::Parse(
            "no ```flow fence found: a .riv.md document needs at least one executable \
             ```flow block (untagged or other-language fences are inert display)"
                .to_string(),
        ));
    }
    parse(&doc.flow_source())
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
    /// Union sub-view definitions seen so far (§29.3, s2): column → its sub-views.
    /// Populated by a `col :string(W) :{ name@start..end … }` block and consulted
    /// when an expression references `base.name`, which lowers to
    /// [`Expr::SubView`] with the range inlined from here.
    view_defs: std::collections::HashMap<String, Vec<SubView>>,
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
            // A quoted path — needed for an `http://…` URL (§33), and useful for
            // any path with characters the bare-word lexer would split.
            Tok::Str(s) => Ok(s),
            Tok::Minus => Ok("-".to_string()),
            other => Err(self.err(format!("expected a path, found {other:?}"))),
        }
    }

    fn peek_is_word(&self, w: &str) -> bool {
        matches!(self.tok(), Tok::Word(x) if x == w)
    }

    /// Is the token *after* the current one a stage keyword other than `map`?
    /// Used for the optional-leading-pipe sugar (§25, #171): a bare `|` directly
    /// before a keyword stage (`| where`, `| sort`, `| group`, …) is consumed so
    /// the stage parses as usual, while `| name` stays named-flow reuse (§25.4)
    /// and `| map …`/`|> …` stay the projection forms. `map` is excluded so a
    /// `| map` after a named-flow apply is never mis-read as a leading pipe.
    fn peek_is_keyword_except_map(&self) -> bool {
        matches!(self.toks.get(self.pos + 1).map(|t| &t.0),
            Some(Tok::Word(w)) if is_keyword(w) && w != "map")
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
            // Optional-leading-pipe sugar (§25, #171): a bare `|` immediately
            // before a keyword stage is consumed so `| where` / `| sort` and
            // single-line chains (`open … | where … | save …`) read naturally.
            // It is pure input sugar — the consumed `|` is not stored, so
            // `to_source` re-emits the canonical typed-pipe form (no second
            // canonical form). `| name` (named-flow §25.4) and `|> …` are
            // unaffected; a body-leading `|` (no `current` stage yet) is handled
            // by `parse_body_head` and still errors.
            if self.at(&Tok::Pipe) && self.peek_is_keyword_except_map() {
                self.bump();
            }
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
                // `|! { pred disp; pred disp … }` — a validation bundle
                // (§29.5-6 s4): each entry is its own contract, lowered to a
                // chain of `Op::Validate` nodes (order preserved, zero new IR —
                // a `halt` entry still stops at the first violation in order).
                Tok::PipeValidate => {
                    self.bump();
                    if self.eat(&Tok::LBrace) {
                        let mut any = false;
                        loop {
                            while self.eat(&Tok::Semicolon) {}
                            if self.eat(&Tok::RBrace) {
                                break;
                            }
                            let pred = self.parse_filter_preds()?;
                            let disposition = self.parse_disposition()?;
                            let n = self.g.add_node(Op::Validate { pred, disposition });
                            self.g.add_edge(current, n, EdgeKind::Stream);
                            current = n;
                            any = true;
                        }
                        if !any {
                            return Err(self.err(
                                "`|! { … }` needs at least one `pred warn|reject|halt` entry",
                            ));
                        }
                    } else {
                        let pred = self.parse_filter_preds()?;
                        let disposition = self.parse_disposition()?;
                        let n = self.g.add_node(Op::Validate { pred, disposition });
                        self.g.add_edge(current, n, EdgeKind::Stream);
                        current = n;
                    }
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
                    current = self.parse_group_tail(current)?;
                }
                // `group KEY… [func:col …]` — readable bare-word alias for `|#`
                // (§25, #171). Same tail as `|#`; `to_source` re-emits `|#` (the
                // canonical form — `group` is input-only sugar). A real column
                // named `group` is reached with `item("group")` (it is reserved
                // as a stage keyword here, like `sort`/`distinct`).
                Tok::Word(w) if w == "group" => {
                    self.bump();
                    current = self.parse_group_tail(current)?;
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
                        // `| map { ... }` — reserved but not implemented. It used
                        // to parse and silently drop the block (a no-op the user
                        // reads as a working transform, #203) — refuse with the
                        // working alternative instead (never-silent).
                        Tok::Word(w) if w == "map" => {
                            return Err(self.err(
                                "`| map { … }` is not yet implemented — write computed \
                                 columns as `|> (expr) as col` (e.g. `|> name (age * 2) \
                                 as doubled`)",
                            ));
                        }
                        // A bare word that names no defined flow is a clear error
                        // (not a silent skip), matching the merge/join diagnostic.
                        Tok::Word(n) => {
                            return Err(self.err(format!("`| {n}`: unknown flow '{n}'")));
                        }
                        // `| { ... }` — reserved but not implemented; same #203
                        // family as `map` (a block that parsed and vanished).
                        _ => {
                            return Err(self.err(
                                "a bare `| { … }` block is not yet implemented — write \
                                 computed columns as `|> (expr) as col`, filters as \
                                 `|? pred`",
                            ));
                        }
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
                    // A quoted path may be a route template (`{col}` derives the
                    // partition keys, §28.7 / #143); a bare word stays the v1
                    // fixed path (braces never lex into a word).
                    let path = match self.tok().clone() {
                        Tok::Str(p) => {
                            self.bump();
                            norm_path(p)
                        }
                        _ => norm_path(self.path_word()?),
                    };
                    let mut explicit: Option<String> = None;
                    let mut by: Vec<String> = Vec::new();
                    let mut flat = false;
                    loop {
                        if self.peek_is_word("as") {
                            self.bump();
                            let m = self.word()?;
                            if m == "flat" {
                                flat = true;
                            } else {
                                explicit = Some(m);
                            }
                        } else if self.peek_is_word("by") {
                            self.bump();
                            while matches!(self.tok(), Tok::Word(k) if k != "as" && k != "by") {
                                by.push(self.word()?);
                            }
                            if by.is_empty() {
                                return Err(self.err("`save … by` needs at least one key column"));
                            }
                        } else {
                            break;
                        }
                    }
                    // Template validation is declaration-time (never-silent).
                    let segs = parse_route_template(&path).map_err(|e| self.err(e))?;
                    let mut keys: Vec<String> = Vec::new();
                    let mut exprs: Vec<Expr> = Vec::new();
                    for seg in &segs {
                        match seg {
                            RouteSeg::Key(k) => {
                                if !keys.contains(k) {
                                    keys.push(k.clone());
                                }
                            }
                            // A computed placeholder (s4c, #143 ①): parse the
                            // snippet through the normal expression grammar by
                            // wrapping it in a one-item projection (the parens
                            // delimit it exactly), so the two grammars can
                            // never drift. Each is its own anonymous key.
                            RouteSeg::Raw(raw) => {
                                let wrapped = format!("__T:\n open __x\n |> ({raw}) as __k\n;");
                                let sub = parse(&wrapped).map_err(|e| {
                                    self.err(format!(
                                        "invalid expression placeholder {{{raw}}} in save \
                                         template: {e}"
                                    ))
                                })?;
                                let expr = sub
                                    .nodes
                                    .iter()
                                    .find_map(|n| match &n.op {
                                        Op::ProjectExpr { items, .. } => {
                                            items.first().map(|(e, _)| e.clone())
                                        }
                                        _ => None,
                                    })
                                    .ok_or_else(|| {
                                        self.err(format!(
                                            "invalid expression placeholder {{{raw}}}"
                                        ))
                                    })?;
                                exprs.push(expr);
                            }
                            RouteSeg::Lit(_) => {}
                        }
                    }
                    let templated = !keys.is_empty() || !exprs.is_empty();
                    if templated {
                        // A key that never reaches the path would silently add
                        // a directory level — refuse instead (#143 ①).
                        for k in &by {
                            if !keys.contains(k) {
                                return Err(self.err(format!(
                                    "`by {k}` does not appear in the save template —                                      add a {{{k}}} placeholder"
                                )));
                            }
                        }
                        if flat {
                            return Err(self.err(
                                "`as flat` applies to a plain path with `by`;                                  a template owns its naming",
                            ));
                        }
                    } else if flat && by.is_empty() {
                        return Err(self.err("`as flat` needs `by KEY…`"));
                    }
                    let delim = resolve_delim(&path, explicit.as_deref());
                    let fmt = resolve_format(&path, explicit.as_deref());
                    // Parquet is read-only in this slice — refuse the sink with
                    // guidance instead of a silent wrong format (never-silent).
                    if matches!(fmt, Some(Format::Parquet)) {
                        return Err(self.err(
                            "`save … .parquet` is not yet implemented (Parquet is read-only in this \
                             slice) — save as csv / jsonl / json",
                        ));
                    }
                    let n = if templated || !by.is_empty() {
                        // Partitioned default format: CSV (a Hive base like
                        // `out/` has no extension to infer from).
                        let codec = match fmt.unwrap_or(Format::Csv) {
                            Format::Csv => SinkCodec::Csv { delim },
                            Format::Jsonl => SinkCodec::Jsonl,
                            Format::Json => SinkCodec::Json,
                            Format::Parquet => unreachable!("refused above"),
                        };
                        self.g.add_node(Op::Sink {
                            route: Route::Template {
                                template: path,
                                by: if templated { keys } else { by },
                                flat,
                                exprs,
                            },
                            transport: Transport::Local,
                            codec,
                        })
                    } else {
                        let fmt = fmt.ok_or_else(|| {
                            self.err(format!(
                                "unknown format '{}'",
                                explicit.clone().unwrap_or_default()
                            ))
                        })?;
                        self.g.add_node(fmt.into_sink_op(path, delim))
                    };
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "writecsv" => {
                    self.bump();
                    let path = self.word()?;
                    let n = self.g.add_node(Op::sink(
                        path.clone(),
                        SinkCodec::Csv {
                            delim: rivus_ir::delim_for_path(&path),
                        },
                    ));
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "writejson" => {
                    self.bump();
                    let path = self.word()?;
                    let n = self.g.add_node(Op::sink(path, SinkCodec::Jsonl));
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
                // `read [as csv|tsv|jsonl] [with source|filename]` — open+decode
                // every handle in the upstream Resource column, union-by-name
                // (§28.3, slice 3c). `as FMT` forces a format; else per extension.
                Tok::Word(w) if w == "read" => {
                    self.bump();
                    let fmt = if self.peek_is_word("as") {
                        self.bump();
                        let f = self.word()?;
                        Some(match f.to_ascii_lowercase().as_str() {
                            "csv" => ReadFmt::Csv,
                            "tsv" => ReadFmt::Tsv,
                            "jsonl" | "ndjson" | "json" => ReadFmt::Jsonl,
                            _ => return Err(self.err(format!("read: unknown format '{f}'"))),
                        })
                    } else {
                        None
                    };
                    let provenance = self.parse_provenance()?;
                    let n = self.g.add_node(Op::Read { fmt, provenance });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
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
                    let keys = keys.into_iter().map(|(s, d)| (key_path(s), d)).collect();
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
                    let keys = keys.into_iter().map(key_path).collect();
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
                // `explode COL` / `unnest COL` — multiply rows over a List column
                // (§32 s4c).
                Tok::Word(w) if w == "explode" || w == "unnest" => {
                    self.bump();
                    let col = self.word()?;
                    let n = self.g.add_node(Op::Explode { col });
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
                // `sessionize TS gap "30m" [by COL ...]` — session windows
                // (§36.5 / #60): append a `session` column carrying the row's
                // session start (same "window start as key" shape as bucket/
                // hops, so `|# session …` aggregates per session).
                Tok::Word(w) if w == "sessionize" => {
                    self.bump();
                    let ts = self.word()?;
                    if !self.peek_is_word("gap") {
                        return Err(self.err(
                            "sessionize expects `gap \"DUR\"` after the timestamp column \
                             (e.g. `sessionize ts gap \"30m\"`)",
                        ));
                    }
                    self.bump(); // 'gap'
                    let gap = match self.bump() {
                        Tok::Str(s) => s,
                        other => {
                            return Err(self.err(format!(
                                "sessionize gap expects a duration string like \"30m\", \
                                 found {other:?}"
                            )))
                        }
                    };
                    let mut by = Vec::new();
                    if self.peek_is_word("by") {
                        self.bump();
                        while let Tok::Word(name) = self.tok().clone() {
                            if is_keyword(&name) {
                                break;
                            }
                            self.bump();
                            by.push(name);
                        }
                        if by.is_empty() {
                            return Err(self.err("sessionize `by` expects at least one column"));
                        }
                    }
                    let n = self.g.add_node(Op::Sessionize { ts, gap, by });
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
                        self.reject_expr_dt_format()?;
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
            // bare `datetime` auto-infers common formats at read time (design
            // 23) at `Sec`. An explicit format is validated here (§29 s3,
            // never-silent: unknown `[locale]` tag / bad `n…n` run = program
            // error) and its `n…n` sub-second run decides the column's tick
            // unit (none → Sec, 1-3 → Milli, 4-6 → Micro, 7-9 → Nano).
            let mut unit = TimeUnit::Sec;
            if self.eat(&Tok::LParen) {
                match self.bump() {
                    Tok::Str(fmt) => {
                        rivus_core::DateTime::validate_format(&fmt).map_err(|e| self.err(e))?;
                        unit = rivus_core::DateTime::unit_for_format(&fmt);
                        self.last_dt_fmt = Some(fmt);
                    }
                    other => {
                        return Err(self.err(format!(
                            "datetime(\"fmt\"): expected a quoted format string, found {other:?}"
                        )))
                    }
                }
                self.expect(&Tok::RParen)?;
            }
            return Ok(DataType::DateTime { unit });
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

    /// (BUG-D §23.6) A datetime parse format belongs to a **schema declaration**,
    /// not an expression / `cast`-verb cast (which has no format — it auto-parses).
    /// If `finish_type` just captured one in expression position, reject it
    /// never-silently instead of dropping it; point the user at the schema.
    fn reject_expr_dt_format(&mut self) -> Result<(), RivusError> {
        if self.last_dt_fmt.take().is_some() {
            return Err(self.err(
                "a datetime parse format is only valid in a schema declaration \
                 (e.g. `open f.csv (ts:datetime(\"yyMMddHHmmss\"))`), not in a cast — \
                 declare the format at the source, then cast bare here",
            ));
        }
        Ok(())
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

    /// Parse the tail of a group-by stage (the `|#` token or the `group` keyword
    /// is already consumed): one or more keys, then optional `func:col`
    /// aggregates. A leading word is an aggregate (not a key) only when it is a
    /// known agg func immediately followed by `:` (e.g. `sum:score`); every other
    /// leading word is a key, and at least one key is required. Shared by `|#`
    /// and the `group` alias so the two stay identical (§25, #171).
    fn parse_group_tail(&mut self, current: NodeId) -> Result<NodeId, RivusError> {
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
            return Err(self.err("group requires at least one key"));
        }
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
        let keys = keys.into_iter().map(key_path).collect();
        let n = self.g.add_node(Op::GroupBy { keys, aggs });
        self.g.add_edge(current, n, EdgeKind::Stream);
        Ok(n)
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
            // `watch "glob"` — the **unbounded** discovery (§28.12, ratified
            // #149): subscribe to OS file-change notification and emit a handle
            // row per changed file matching the glob (same bare-column shape as
            // `ls`; `read` consumes it). Parsing is always-std (IR reversible);
            // *evaluation* requires the off-by-default `unbounded` feature — a
            // feature-less run refuses pre-run (never-silent).
            Tok::Word(w) if w == "watch" => {
                self.bump();
                let pattern = match self.bump() {
                    Tok::Str(s) => s,
                    other => {
                        return Err(self.err(format!(
                            "watch expects a quoted glob pattern, found {other:?}"
                        )))
                    }
                };
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Watch(pattern),
                    transport: Transport::Local,
                    codec: Codec::discover(),
                    provenance: Provenance::Off,
                }))
            }
            // `subscribe "tcp://host:port" [as csv|tsv|json]` — the unbounded
            // network feed (§33, feature `net`): dial a TCP endpoint and stream
            // newline-delimited records. Parsing is always-std (IR reversible);
            // *evaluation* needs `net` (a feature-less run refuses pre-run). The
            // endpoint has no extension, so the codec is CSV unless `as` says.
            Tok::Word(w) if w == "subscribe" => {
                self.bump();
                let addr = match self.bump() {
                    Tok::Str(s) => s,
                    other => {
                        return Err(self.err(format!(
                            "subscribe expects a quoted tcp:// URL, found {other:?}"
                        )))
                    }
                };
                let explicit = if self.peek_is_word("as") {
                    self.bump();
                    Some(self.word()?)
                } else {
                    None
                };
                let codec = match explicit.as_deref() {
                    None | Some("csv") => Codec::csv(b','),
                    Some("tsv") | Some("tab") => Codec::csv(b'\t'),
                    Some("json") | Some("jsonl") | Some("ndjson") => Codec::Jsonl,
                    Some(other) => {
                        return Err(self.err(format!("subscribe: unknown format '{other}'")))
                    }
                };
                Ok(self.g.add_node(Op::Source {
                    discovery: Discovery::Subscribe(addr),
                    transport: Transport::Local,
                    codec,
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
                    // `char[N]` — a fixed-width text field (§29.4): N raw bytes
                    // decoded as UTF-8. Carries its byte width, so it is parsed
                    // here rather than via the word-keyed `BinType::parse`.
                    let bt = if ty == "char" {
                        if !self.eat(&Tok::LBracket) {
                            return Err(self.err(
                                "binary `char` needs a byte width: write `char[N]`, \
                                 e.g. `name:char[16]`",
                            ));
                        }
                        let n = match self.bump() {
                            Tok::Int(v) if v >= 0 => v as u32,
                            other => {
                                return Err(self.err(format!(
                                    "char[N]: N must be a non-negative integer, found {other:?}"
                                )))
                            }
                        };
                        self.expect(&Tok::RBracket)?;
                        BinType::Char(n)
                    } else {
                        BinType::parse(&ty)
                            .ok_or_else(|| self.err(format!("unknown binary type '{ty}'")))?
                    };
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
                    let left_keys = left_keys.into_iter().map(key_path).collect();
                    let right_keys = right_keys.into_iter().map(key_path).collect();
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

    /// Parse a `|>` projection list. Items are bare fields (`name`), `:`
    /// definition chains (`name [:alias] [:type]`, §29.2), renames
    /// (`name as alias`), or computed columns (`(expr) as alias`). When every
    /// item is a bare field this lowers to the pure-selection `Op::Project`
    /// (so existing fusion/pushdown are untouched); otherwise to `ProjectExpr`.
    fn parse_projection(&mut self) -> Result<Op, RivusError> {
        let mut items: Vec<(Expr, String)> = Vec::new();
        let mut views: Vec<ViewDef> = Vec::new();
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
                // `name`, `name as alias`, or a `:` definition chain.
                Tok::Word(w) if !is_keyword(&w) => {
                    self.bump();
                    if self.peek_is_word("as") {
                        self.bump();
                        let alias = self.word()?;
                        items.push((Expr::field(&w), alias));
                        all_bare = false;
                    } else if self.at(&Tok::Colon) {
                        items.push(self.parse_colon_chain(&w, &mut views)?);
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
            Ok(Op::ProjectExpr { items, views })
        }
    }

    /// Parse the tail of a `:` definition chain (§29.2) after its column word:
    /// `col :alias`, `col :type(arg)`, or `col :alias :type(arg)` — definitions
    /// stack left→right, light→heavy (rename before cast). After `:` a type
    /// word always means a cast (the disjointness rule that keeps `to_source`
    /// reversible); to rename a column *to* a type-word name, use the
    /// parenthesized escape hatch `(col) as int`. Lowers to a plain
    /// `ProjectExpr` item — rename is just the alias, cast is `Expr::Cast` —
    /// so the IR and byte-identity are exactly those of the parenthesized
    /// forms.
    fn parse_colon_chain(
        &mut self,
        col: &str,
        views: &mut Vec<ViewDef>,
    ) -> Result<(Expr, String), RivusError> {
        self.expect(&Tok::Colon)?;
        let first = self.word()?;
        let (alias, ty) = if is_type_word(&first) {
            (col.to_string(), Some(self.finish_type(&first)?))
        } else if self.eat(&Tok::Colon) {
            let tyword = self.word()?;
            if !is_type_word(&tyword) {
                return Err(self.err(format!(
                    "`{col} :{first} :{tyword}`: a `:` chain is `col [:alias] [:type]` — \
                     after the rename `:{first}`, `:{tyword}` must be a type \
                     (e.g. int, decimal(2), datetime)"
                )));
            }
            (first, Some(self.finish_type(&tyword)?))
        } else {
            (first, None)
        };
        if ty.is_some() {
            // A datetime parse format belongs to the source schema, not a cast
            // (§23.6) — same rule as the cast verb and expression casts.
            self.reject_expr_dt_format()?;
        }
        // Union sub-view definition (§29.3, s2): `col :string(W) :{ name@a..b … }`.
        // The optional `(W)` width is consumed only when a `:{ … }` block follows
        // (it is kept on the ViewDef — the `Str` lane has no width). The whole
        // view is the cast `col :str`; the sub-views are resolved lazily by the
        // `base.name` accessor, so the op materializes no extra columns.
        let width = self.parse_view_width(ty)?;
        if let Some(ty) = ty {
            if self.at(&Tok::Colon) && self.toks[self.pos + 1].0 == Tok::LBrace {
                self.bump(); // `:` before the `{ … }` block
                let subs = self.parse_view_block(&alias)?;
                self.view_defs.insert(alias.clone(), subs.clone());
                views.push(ViewDef {
                    col: alias.clone(),
                    width,
                    subs,
                });
                return Ok((
                    Expr::Cast {
                        expr: Box::new(Expr::field(col)),
                        ty,
                    },
                    alias,
                ));
            }
        }
        if width.is_some() {
            return Err(self.err(
                "string(N) width is only valid with a sub-view block, e.g. \
                 `id :string(N) :{ a@0..3 … }`",
            ));
        }
        if ty.is_some() && self.at(&Tok::Colon) {
            return Err(self.err(format!(
                "`{col} :…`: a `:` chain is at most `col :alias :type` \
                 (rename first, then cast — nothing follows the type)"
            )));
        }
        let mut expr = Expr::field(col);
        if let Some(ty) = ty {
            expr = Expr::Cast {
                expr: Box::new(expr),
                ty,
            };
        }
        Ok((expr, alias))
    }

    /// Parse the optional `(W)` width of a string union view — but only when it
    /// is immediately followed by a `:{ … }` block (`:string(W) :{ … }`), so a
    /// following `(expr) as alias` projection item is never mistaken for a width.
    /// Consumes `(W)` and returns the width, or consumes nothing.
    fn parse_view_width(&mut self, ty: Option<DataType>) -> Result<Option<u32>, RivusError> {
        if !matches!(ty, Some(DataType::Str)) || !self.at(&Tok::LParen) {
            return Ok(None);
        }
        // Only the exact `( Int ) : {` shape is a width.
        let is_width = matches!(self.toks.get(self.pos + 1).map(|t| &t.0), Some(Tok::Int(_)))
            && self.toks.get(self.pos + 2).map(|t| &t.0) == Some(&Tok::RParen)
            && self.toks.get(self.pos + 3).map(|t| &t.0) == Some(&Tok::Colon)
            && self.toks.get(self.pos + 4).map(|t| &t.0) == Some(&Tok::LBrace);
        if !is_width {
            return Ok(None);
        }
        self.bump(); // `(`
        let w = match self.bump() {
            Tok::Int(n) if n >= 0 => n as u32,
            other => {
                return Err(self.err(format!(
                    "string(N): width must be a non-negative integer, found {other:?}"
                )))
            }
        };
        self.expect(&Tok::RParen)?;
        Ok(Some(w))
    }

    /// Parse a union sub-view block `{ name@start..end … }` (§29.3, s2): one or
    /// more half-open **character** ranges. `start <= end`, offsets are
    /// non-negative integers, and duplicate sub-view names are an error
    /// (never-silent). The opening `{` is the current token.
    fn parse_view_block(&mut self, col: &str) -> Result<Vec<SubView>, RivusError> {
        self.expect(&Tok::LBrace)?;
        let mut subs: Vec<SubView> = Vec::new();
        while !self.at(&Tok::RBrace) {
            if self.at(&Tok::Eof) {
                return Err(self.err("unterminated sub-view block `:{ … }`"));
            }
            let name = self.word()?;
            if !self.eat(&Tok::At) {
                return Err(self.err(format!(
                    "sub-view `{name}` needs an offset range, e.g. `{name}@0..3`"
                )));
            }
            let start = self.view_offset(&name)?;
            if !self.eat(&Tok::DotDot) {
                return Err(self.err(format!(
                    "sub-view `{name}@{start}..end`: expected `..` (a half-open range `start..end`)"
                )));
            }
            let end = self.view_offset(&name)?;
            if start > end {
                return Err(self.err(format!(
                    "sub-view `{name}@{start}..{end}`: start must be <= end (half-open `[start, end)`)"
                )));
            }
            if subs.iter().any(|s| s.name == name) {
                return Err(self.err(format!("duplicate sub-view name `{name}` on `{col}`")));
            }
            subs.push(SubView { name, start, end });
        }
        self.expect(&Tok::RBrace)?;
        if subs.is_empty() {
            return Err(self.err(format!(
                "sub-view block on `{col}` is empty; define at least one `name@start..end`"
            )));
        }
        Ok(subs)
    }

    /// Read a non-negative integer sub-view offset (a character index).
    fn view_offset(&mut self, name: &str) -> Result<u32, RivusError> {
        match self.bump() {
            Tok::Int(n) if n >= 0 => Ok(n as u32),
            other => Err(self.err(format!(
                "sub-view `{name}` offset must be a non-negative integer, found {other:?}"
            ))),
        }
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

    /// The required `warn|reject|halt` word after a `|!` contract's predicate
    /// (no implicit default, so a silent policy is impossible).
    fn parse_disposition(&mut self) -> Result<Disposition, RivusError> {
        match self.tok().clone() {
            Tok::Word(w) if Disposition::parse(&w).is_some() => {
                self.bump();
                Ok(Disposition::parse(&w).unwrap())
            }
            other => Err(self.err(format!(
                "`|!` needs a disposition (warn|reject|halt), found {other:?}"
            ))),
        }
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
        } else if self.at(&Tok::Tilde) {
            // `EXPR ~ 'pat'` — regex infix (§29.5-6 s4), comparison-level.
            // Lowers to the existing `Func::Regexp` (zero new IR); the pattern
            // is usually a raw `'…'` regex literal but any expression works
            // (per-row pattern, like `regexp(col, expr)`).
            self.bump();
            let right = if let Tok::Regex(p) = self.tok().clone() {
                self.bump();
                Expr::Literal(Value::Str(p))
            } else {
                self.parse_add()?
            };
            Ok(Expr::Func {
                func: Func::Regexp,
                args: vec![left, right],
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
                // Any cast-type word turns `:` into a cast (incl. the temporal and
                // decimal lanes, which `finish_type` handles); otherwise leave `:`
                // for the caller. A temporal cast carries no format here — an
                // explicit `:datetime("fmt")` is rejected by `reject_expr_dt_format`
                // (BUG-D §23.6: the format belongs to a schema declaration).
                if is_cast_type_word(&w) {
                    self.bump(); // ':'
                    self.bump(); // type word
                    let ty = self.finish_type(&w)?;
                    self.reject_expr_dt_format()?;
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
            // Unary minus on a numeric literal — `split_part(s, "/", -1)`,
            // `(x * -1)` (#199). Restricted to literals: general negation has
            // no IR node yet, and `0 - expr` spells it explicitly.
            Tok::Minus => {
                self.bump();
                match self.tok().clone() {
                    Tok::Int(n) => {
                        self.bump();
                        Ok(Expr::Literal(Value::I64(-n)))
                    }
                    Tok::Float(_, d) => {
                        self.bump();
                        Ok(Expr::Literal(Value::Dec(rivus_core::Decimal::new(
                            -d.unscaled,
                            d.scale,
                        ))))
                    }
                    other => Err(self.err(format!(
                        "unary `-` needs a numeric literal, found {other:?} — write \
                         `0 - expr` for a general negation"
                    ))),
                }
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Literal(Value::Str(s)))
            }
            // A `'…'` regex literal is *only* a pattern: the right side of `~`
            // or the pattern argument of regexp(…) (both consume it before
            // reaching here). Anywhere else it is a declaration-time error —
            // never a silent plain string (§29.5-6 s4).
            Tok::Regex(_) => Err(self.err(
                "a '…' regex literal is only valid as the right side of `~` or as the \
                 pattern of regexp(…); for a plain string use \"…\"",
            )),
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
                // A `'…'` regex literal is accepted exactly in the pattern slot
                // of the regexp family — `regexp(col, '\d+')` (§29.5-6 s4).
                let arg = |p: &mut Self, idx: usize| -> Result<Expr, RivusError> {
                    if func == Func::Regexp && idx == 1 {
                        if let Tok::Regex(pat) = p.tok().clone() {
                            p.bump();
                            return Ok(Expr::Literal(Value::Str(pat)));
                        }
                    }
                    p.parse_expr()
                };
                let mut args = Vec::new();
                if !self.at(&Tok::RParen) {
                    args.push(arg(self, 0)?);
                    while self.eat(&Tok::Comma) {
                        args.push(arg(self, args.len())?);
                    }
                }
                self.expect(&Tok::RParen)?;
                Ok(Expr::Func { func, args })
            }
            // An UNKNOWN word followed by `(` is a function-call attempt with a
            // name we don't have (#192) — say so, with the nearest valid name,
            // instead of drifting into an unrelated "expected RParen" error.
            // Type-words are excluded (`datetime("fmt")` is a schema form
            // handled by the type parser), as are the keyword forms below.
            Tok::Word(ref w)
                if self.toks[self.pos + 1].0 == Tok::LParen
                    && Func::parse(w).is_none()
                    && !is_type_word(w)
                    && w != "case"
                    && w != "resource" =>
            {
                let hint = rivus_core::suggest::suggest_similar(w, Func::names().iter().copied())
                    .map(|s| format!(" — did you mean '{s}'?"))
                    .unwrap_or_default();
                Err(self.err(format!(
                    "unknown function '{w}'{hint} (available: {})",
                    Func::names().join(", ")
                )))
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
            // `base.name` — union sub-view accessor (§29.3, s2): `base` is a column
            // with sub-views defined earlier by a `:{ … }` block, so this lowers to
            // `Expr::SubView` with the char range inlined from `view_defs` (same
            // expression-context `.` mechanism as `source.uri`). An unknown sub-view
            // name is a never-silent error rather than a silently-empty slice.
            Tok::Word(ref w)
                if self.toks[self.pos + 1].0 == Tok::Dot && self.view_defs.contains_key(w) =>
            {
                let base = w.clone();
                self.bump(); // base
                self.expect(&Tok::Dot)?;
                let name = self.word()?;
                match self.view_defs[&base].iter().find(|s| s.name == name) {
                    Some(s) => Ok(Expr::SubView {
                        base,
                        name,
                        start: s.start,
                        end: s.end,
                    }),
                    None => Err(self.err(format!(
                        "`{base}.{name}`: `{base}` has no sub-view `{name}` \
                         (sub-views come from a `:{{ … }}` block)"
                    ))),
                }
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
                // §32 s4: a nested path into a Struct/List column.
                // - Flow mode folds `user.age` into one identifier (the lexer
                //   keeps `.` in a word at paren-depth 0): lower it to a path.
                if name.contains('.') {
                    return match PathExpr::parse(&name) {
                        // `source.<field>` is the provenance accessor (§28.6), not
                        // a data path — reserved, and reachable only inside a
                        // computed column `|> (…)` where the lexer splits it.
                        Some(p) if p.root == "source" && !p.is_bare() => Err(self.err(
                            "`source.<field>` (provenance, §28.6) is only valid inside a \
                             computed column `|> (…)`; a nested data path uses any other root",
                        )),
                        Some(p) if !p.is_bare() => Ok(Expr::Path(p)),
                        // A lone trailing/leading dot etc. — never-silent.
                        _ => Err(self.err(format!("malformed nested path `{name}`"))),
                    };
                }
                // - Expression mode (inside parens) splits `user . age` and
                //   `tags [ 0 ]`; a following `.` / `[` is a nested-path tail.
                //   (`source.field` / a sub-view base were dispatched above.)
                if matches!(self.tok(), Tok::Dot | Tok::LBracket) {
                    return self.parse_path_tail_from(name);
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

    /// Build a nested path (§32 s4) from an already-read `root`, consuming the
    /// expression-mode `.field` / `[i]` tail tokens. Called only when a `.` / `[`
    /// follows, so the result always has ≥ 1 segment (never the bare form).
    fn parse_path_tail_from(&mut self, root: String) -> Result<Expr, RivusError> {
        let mut segs = Vec::new();
        loop {
            if self.eat(&Tok::Dot) {
                segs.push(PathSeg::Field(self.word()?));
            } else if self.eat(&Tok::LBracket) {
                // 0-based list index; must fit u32 (the digits round-trip), never
                // silently truncated.
                let idx = match self.bump() {
                    Tok::Int(n) if (0..=u32::MAX as i64).contains(&n) => n as u32,
                    other => {
                        return Err(self.err(format!(
                            "a list index must be in 0..={}, found {other:?}",
                            u32::MAX
                        )))
                    }
                };
                self.expect(&Tok::RBracket)?;
                segs.push(PathSeg::Index(idx));
            } else {
                break;
            }
        }
        Ok(Expr::Path(PathExpr { root, segs }))
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
        } else if self.eat(&Tok::LBracket) {
            // `$_[i]` — positional column reference (§29.5-6 s4): the i-th
            // column, 0-based, in schema order (headerless/positional data).
            // The index must fit `u32` — a larger literal is a declaration-time
            // error, never silently truncated (the source digits must round-trip).
            let idx = match self.bump() {
                Tok::Int(n) if (0..=u32::MAX as i64).contains(&n) => n as u32,
                other => {
                    return Err(self.err(format!(
                        "`$_[i]` expects a column index in 0..={}, found {other:?}",
                        u32::MAX
                    )))
                }
            };
            self.expect(&Tok::RBracket)?;
            Ok(Expr::FieldAt(idx))
        } else {
            Err(self.err("expected '.field', '..field' or '[i]' after $_"))
        }
    }
}

/// Normalize a source/sink path: `stdin` / `stdout` / `-` all map to the `-`
/// sentinel (read stdin / write stdout, direction inferred from source vs sink).
/// Map a declared column type name to a `DataType` lane.
/// Does `w` name a type usable in an `expr:type` cast? Covers the plain lanes
/// (`decl_type`) plus the lanes `finish_type` parses with a suffix/format
/// (`decimal(N)`, `datetime`/`date`/`time`/`duration`). Gates `parse_cast` so an
/// inline temporal cast (`x:datetime`, `(ts:date) as d`) is recognized, not left
/// as a stray `:` (BUG-D §23.6).
fn is_cast_type_word(w: &str) -> bool {
    decl_type(w).is_some()
        || ["decimal", "datetime", "date", "time", "duration"]
            .iter()
            .any(|t| w.eq_ignore_ascii_case(t))
}

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
    /// Apache Parquet (read-only slice; runtime `parquet` feature).
    Parquet,
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
            Format::Parquet => Codec::Parquet,
        };
        Op::source(path, codec)
    }

    fn into_sink_op(self, path: String, delim: u8) -> Op {
        let codec = match self {
            Format::Csv => SinkCodec::Csv { delim },
            Format::Jsonl => SinkCodec::Jsonl,
            Format::Json => SinkCodec::Json,
            // Refused at the `save` parse site before reaching here.
            Format::Parquet => unreachable!("parquet sink refused at parse"),
        };
        Op::sink(path, codec)
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
            "parquet" => Some(Format::Parquet),
            _ => None,
        };
    }
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".parquet") {
        Some(Format::Parquet)
    } else if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
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
            | "sessionize"
            | "readbin"
            | "readcsv"
            | "readjson"
            | "read"
            | "ls"
            | "gci"
            | "dir"
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
            | "explode"
            | "unnest"
            | "fill"
            | "drop"
            | "cast"
            | "reorder"
            | "rename"
            | "where"
            | "group"
            | "on"
            | "map"
            | "mode"
            | "stop"
            | "monitor"
            | "watch"
            | "subscribe"
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
            Op::ProjectExpr { items, .. } => {
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
    fn colon_chain_lowers_to_plain_project_expr_items() {
        // §29.2: `col [:alias] [:type]` stacks definitions left→right and
        // lowers to ordinary ProjectExpr items — rename is just the alias,
        // cast is Expr::Cast — so the IR (and byte-identity) is exactly that
        // of the parenthesized forms.
        match nth_op(
            "F:\n open a.csv\n |> amount :amt :decimal(2) qty :int note :memo plain\n;",
            1,
        ) {
            Op::ProjectExpr { items, .. } => {
                assert_eq!(items.len(), 4);
                assert_eq!(items[0].1, "amt");
                assert!(matches!(
                    &items[0].0,
                    Expr::Cast {
                        ty: DataType::Decimal { scale: 2 },
                        ..
                    }
                ));
                // A bare cast keeps the column name as its alias.
                assert_eq!(items[1].1, "qty");
                assert!(matches!(
                    &items[1].0,
                    Expr::Cast {
                        ty: DataType::I64,
                        ..
                    }
                ));
                assert_eq!(items[2].1, "memo");
                assert!(matches!(&items[2].0, Expr::Field { .. }));
                assert_eq!(items[3].1, "plain");
            }
            other => panic!("expected ProjectExpr, got {other:?}"),
        }
    }

    #[test]
    fn colon_chain_is_the_canonical_form_and_round_trips() {
        // The chain re-parses to the same source, and the older parenthesized
        // spellings canonicalize to it (alias → one canonical form, §25.2a).
        let src = "F:\n open a.csv\n |> amount :amt :decimal(2) qty :int note :memo\n;";
        let s1 = parse(src).unwrap().to_source();
        // Type names normalize (`int` → `i64`), like the cast verb.
        assert!(
            s1.contains("|> amount :amt :decimal(2) qty :i64 note :memo"),
            "chain is not the canonical rendering: {s1}"
        );
        assert_eq!(
            s1,
            parse(&s1).unwrap().to_source(),
            "chain not reversible: {s1}"
        );
        let sugar =
            "F:\n open a.csv\n |> (amount:decimal(2)) as amt (qty:int) as qty (note) as memo\n;";
        assert_eq!(
            parse(sugar).unwrap().to_source(),
            s1,
            "parenthesized forms must canonicalize to the chain"
        );
    }

    #[test]
    fn union_view_defines_round_trips_and_resolves_subviews() {
        // `:string(W) :{ name@start..end … }` defines char sub-views on a
        // fixed-width column (§29.3, s2); `base.name` references them in
        // expression context (same `.` mechanism as `source.uri`).
        let src = "U:\n open ids.csv\n |> id :string(27) :{ cls@0..3 dept@3..11 }\n \
                   |> (id.cls) as cls (id.dept) as dept\n;";
        let g = parse(src).unwrap();
        // Node #1 (first projection): the whole-view cast item + a ViewDef that
        // keeps the declared width and the char ranges (the `Str` lane has none).
        match &g.nodes[1].op {
            Op::ProjectExpr { items, views } => {
                assert_eq!(items.len(), 1, "whole view is one materialized column");
                assert!(matches!(&items[0].0, Expr::Cast { .. }) && items[0].1 == "id");
                assert_eq!(views.len(), 1);
                assert_eq!(views[0].col, "id");
                assert_eq!(views[0].width, Some(27));
                assert_eq!(
                    views[0].subs,
                    vec![
                        rivus_ir::SubView {
                            name: "cls".into(),
                            start: 0,
                            end: 3
                        },
                        rivus_ir::SubView {
                            name: "dept".into(),
                            start: 3,
                            end: 11
                        },
                    ]
                );
            }
            other => panic!("expected ProjectExpr with views, got {other:?}"),
        }
        // Node #2: the references lower to `Expr::SubView` with inlined ranges.
        match &g.nodes[2].op {
            Op::ProjectExpr { items, .. } => {
                assert!(matches!(
                    &items[0].0,
                    Expr::SubView { base, name, start, end }
                        if base == "id" && name == "cls" && *start == 0 && *end == 3
                ));
            }
            other => panic!("expected ProjectExpr, got {other:?}"),
        }
        // Canonical rendering + reversibility (§29.7 invariant).
        let s1 = g.to_source();
        assert!(
            s1.contains("|> id :str(27) :{ cls@0..3 dept@3..11 }"),
            "definition not canonical: {s1}"
        );
        assert!(
            s1.contains("(id.cls) as cls (id.dept) as dept"),
            "references not canonical: {s1}"
        );
        assert_eq!(
            s1,
            parse(&s1).unwrap().to_source(),
            "union view not reversible: {s1}"
        );
    }

    #[test]
    fn union_view_malformed_definitions_and_references_are_explicit_errors() {
        // Never-silent: every malformed sub-view definition / reference is a
        // clear parse error rather than a silently-wrong or dropped view.
        let bad = [
            // unknown sub-view reference
            "U:\n open f.csv\n |> id :str(4) :{ a@0..2 } |> (id.zzz) as z\n;",
            // duplicate sub-view name
            "U:\n open f.csv\n |> id :str(4) :{ a@0..2 a@2..4 }\n;",
            // start > end
            "U:\n open f.csv\n |> id :str(4) :{ a@3..1 }\n;",
            // width without a sub-view block
            "U:\n open f.csv\n |> id :string(4)\n;",
            // empty sub-view block
            "U:\n open f.csv\n |> id :str(4) :{ }\n;",
            // missing offset range
            "U:\n open f.csv\n |> id :str(4) :{ a }\n;",
        ];
        for src in bad {
            assert!(parse(src).is_err(), "should have rejected: {src}");
        }
    }

    #[test]
    fn readbin_char_field_parses_and_round_trips() {
        // `char[N]` binary field (§29.4): parses to `BinType::Char(N)`, renders
        // back as `name:char[N]`, and re-parses to the same IR (reversible).
        let src = "R:\n readbin f.bin (id:i32 name:char[16])\n |> id name\n;";
        let g = parse(src).unwrap();
        match &g.nodes[0].op {
            Op::Source {
                codec: rivus_ir::Codec::Binary { fields, .. },
                ..
            } => {
                assert_eq!(fields[1].0, "name");
                assert_eq!(fields[1].1, rivus_ir::BinType::Char(16));
            }
            other => panic!("expected a binary Source, got {other:?}"),
        }
        let s = g.to_source();
        assert!(s.contains("name:char[16]"), "char[N] not in source: {s}");
        assert_eq!(
            s,
            parse(&s).unwrap().to_source(),
            "char[N] not reversible: {s}"
        );
        // A bare `char` with no width is a never-silent error.
        assert!(parse("R:\n readbin f.bin (id:i32 x:char)\n;").is_err());
    }

    #[test]
    fn alias_colliding_with_a_type_word_keeps_the_parenthesized_form() {
        // `|> (amount) as int` renames to a type-word name. `to_source` must
        // not emit `amount :int` — that re-parses as a cast — so the
        // parenthesized escape hatch is the stable spelling (§29.5-4).
        let src = "F:\n open a.csv\n |> (amount) as int\n;";
        let s = parse(src).unwrap().to_source();
        // (Expression fields render as `$_.name` inside parens.)
        assert!(
            s.contains("($_.amount) as int"),
            "expected escape hatch in {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        match nth_op(&s, 1) {
            Op::ProjectExpr { items, .. } => {
                assert_eq!(items[0].1, "int");
                assert!(
                    matches!(&items[0].0, Expr::Field { .. }),
                    "must stay a rename, not become a cast: {:?}",
                    items[0].0
                );
            }
            other => panic!("expected ProjectExpr, got {other:?}"),
        }
    }

    #[test]
    fn colon_chain_misordered_or_overlong_is_an_explicit_error() {
        // Definitions stack light→heavy: rename first, then cast, nothing
        // after the type (§29.2). Violations are explicit errors, never a
        // silent reinterpretation.
        for src in [
            "F:\n open a.csv\n |> amount :int :amt\n;",
            "F:\n open a.csv\n |> amount :amt :decimal(2) :int\n;",
        ] {
            let e = parse(src).unwrap_err();
            assert!(
                format!("{e:?}").contains("nothing follows the type"),
                "missing order hint for {src}: {e:?}"
            );
        }
        // A second definition that isn't a type gets the chain shape hint.
        let e = parse("F:\n open a.csv\n |> amount :amt :foo\n;").unwrap_err();
        assert!(
            format!("{e:?}").contains("must be a type"),
            "missing type hint: {e:?}"
        );
    }

    #[test]
    fn datetime_format_is_validated_and_derives_the_unit() {
        // §29 s3: a schema-declared format is validated at parse (never-silent
        // program errors) and its `n…n` run decides the column's tick unit.
        for bad in [
            "F:\n open a.csv (ts:datetime(\"[xx-yy]yyyy\"))\n;", // unknown locale
            "F:\n open a.csv (ts:datetime(\"[ja-jp\"))\n;",      // unclosed tag
            "F:\n open a.csv (ts:datetime(\"mm.nnn ss.nnn\"))\n;", // two runs
            "F:\n open a.csv (ts:datetime(\"ss.nnnnnnnnnn\"))\n;", // 10-digit run
        ] {
            assert!(parse(bad).is_err(), "should have rejected: {bad}");
        }
        // A `[ja-jp]` + sub-second format parses and round-trips verbatim
        // (multi-byte literals intact — the string lexer copies bytes, §29 s3).
        let src = "F:\n open a.csv (ts:datetime(\"[ja-jp]yyyy年MM月dd日(ddd) HH:mm:ss.nnnnnn\"))\n |> ts\n;";
        let s = parse(src).unwrap().to_source();
        assert!(
            s.contains("[ja-jp]yyyy年MM月dd日(ddd) HH:mm:ss.nnnnnn"),
            "format mangled: {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
    }

    #[test]
    fn colon_chain_datetime_format_is_rejected_like_other_casts() {
        // BUG-D §23.6: a datetime parse format belongs to the source schema;
        // chain casts follow the same never-silent rule as the cast verb.
        let e = parse("F:\n open a.csv\n |> ts :datetime(\"yyMMddHHmmss\")\n;").unwrap_err();
        assert!(
            format!("{e:?}").contains("schema declaration"),
            "missing schema hint: {e:?}"
        );
    }

    #[test]
    fn every_type_word_casts_in_a_colon_chain() {
        // Locks `rivus_ir::is_type_word` to the parser's type tables: every
        // word the predicate accepts must finish as a cast — never fall back
        // to a rename, never error as an unknown type.
        for w in [
            "int",
            "i64",
            "integer",
            "float",
            "f64",
            "double",
            "str",
            "string",
            "text",
            "bool",
            "boolean",
            "resource",
            "decimal(2)",
            "datetime",
            "duration",
            "date",
            "time",
        ] {
            let src = format!("F:\n open a.csv\n |> amount :{w}\n;");
            match nth_op(&src, 1) {
                Op::ProjectExpr { items, .. } => {
                    assert_eq!(items[0].1, "amount", "alias must stay the column for :{w}");
                    assert!(
                        matches!(items[0].0, Expr::Cast { .. }),
                        ":{w} must lower to a cast"
                    );
                }
                other => panic!("expected ProjectExpr for :{w}, got {other:?}"),
            }
        }
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
                Op::ProjectExpr { items, .. } => Some(items),
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
                Op::ProjectExpr { items, .. } => Some(items),
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
                Op::ProjectExpr { items, .. } => Some(items),
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
    fn watch_unbounded_discovery_parses_and_round_trips() {
        // `watch "glob"` (§28.12, ratified #149) → the unbounded discovery
        // source (Watch + Codec::Discover). Parse / to_source are always-std
        // (IR reversible); only evaluation is feature-gated.
        let src = "W:\n watch \"in/*.csv\"\n read as csv\n take 5\n;";
        let g = parse(src).unwrap();
        assert!(matches!(
            &g.nodes[0].op,
            Op::Source {
                discovery: Discovery::Watch(p),
                codec: Codec::Discover { .. },
                ..
            } if p == "in/*.csv"
        ));
        let s = g.to_source();
        assert!(s.contains("watch \"in/*.csv\""), "watch lost in {s}");
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");

        // The boundedness-derived determinism tag (§0.14): the watch source and
        // everything downstream carry it; a bounded sibling scope does not.
        assert!(g.uses_unbounded());
        let tag = g.unbounded_nodes();
        assert!(
            (0..g.nodes.len()).all(|i| tag[i]),
            "all nodes of a watch flow are downstream of the unbounded source"
        );
        let two = parse("B:\n open data.csv\n;\nW:\n watch \"in/*.csv\"\n;").unwrap();
        let tag2 = two.unbounded_nodes();
        let bounded = two
            .nodes
            .iter()
            .find(|n| {
                matches!(
                    &n.op,
                    Op::Source {
                        discovery: Discovery::Fixed(_),
                        ..
                    }
                )
            })
            .unwrap()
            .id;
        let unbounded = two
            .nodes
            .iter()
            .find(|n| {
                matches!(
                    &n.op,
                    Op::Source {
                        discovery: Discovery::Watch(_),
                        ..
                    }
                )
            })
            .unwrap()
            .id;
        assert!(!tag2[bounded], "bounded sibling scope must stay untagged");
        assert!(tag2[unbounded]);

        // A non-string pattern is an explicit parse error (never-silent).
        assert!(parse("W:\n watch in.csv\n;").is_err());
    }

    #[test]
    fn read_verb_parses_and_round_trips() {
        // `read [as FMT] [with …]` (§28.3, slice 3c) parses to `Op::Read` and
        // round-trips; an unknown format is an explicit error.
        for (src, want) in [
            ("R:\n ls \"*.csv\"\n read\n;", None),
            (
                "R:\n ls \"*.csv\"\n read as csv with source\n;",
                Some(ReadFmt::Csv),
            ),
            (
                "R:\n ls \"*.x\"\n read as jsonl with filename\n;",
                Some(ReadFmt::Jsonl),
            ),
        ] {
            let g = parse(src).unwrap();
            let fmt = g
                .nodes
                .iter()
                .find_map(|n| match &n.op {
                    Op::Read { fmt, .. } => Some(*fmt),
                    _ => None,
                })
                .expect("read op");
            assert_eq!(fmt, want, "fmt for {src}");
            let s = g.to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
        assert!(parse("R:\n ls \"*.csv\"\n read as toml\n;").is_err());
    }

    #[test]
    fn leading_bom_on_flow_script_is_stripped() {
        // BUG-E: a UTF-8 BOM (`\u{FEFF}`) at the start of a flow script must not
        // break parsing (Windows editors add it). The script is BOM-tolerant.
        let plain = "F:\n open a.csv\n |> id\n;";
        let g = parse(&format!("\u{feff}{plain}")).expect("BOM-prefixed flow must parse");
        assert_eq!(g.to_source(), parse(plain).unwrap().to_source());
    }

    #[test]
    fn provenance_accessor_is_reserved_but_dotted_data_path_resolves() {
        // `source.<field>` is the provenance accessor (§28.6): reserved, and
        // reachable only inside a computed column `|> (…)` — never as a bare
        // flow-mode predicate (never-silent, not a field literally "source.uri").
        assert!(parse("F:\n open a.csv with source\n |? source.uri == \"x\"\n;").is_err());
        // In a computed column it is the provenance accessor (works, slice 2).
        assert!(parse("F:\n open a.csv with source\n |> (source.uri) as u\n;").is_ok());
        // §32 s4: any *other* dotted field is now a nested data path (resolved
        // against the Struct/List lanes), not an error.
        assert!(parse("L:\n ls \"*.csv\"\n |? path.name == \"a\"\n;").is_ok());
        assert!(parse("D:\n open d.jsonl\n |? user.age >= 18\n;").is_ok());
        assert!(parse("D:\n open d.jsonl\n |> (tags[0]) as first\n;").is_ok());
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
    fn datetime_format_in_expr_cast_is_rejected_bug_d() {
        // BUG-D §23.6: a parse format belongs to a schema *declaration*, not an
        // expression / `cast`-verb cast — an explicit format in cast position is a
        // never-silent parse error (not silently dropped).
        assert!(parse("F:\n open log.csv\n cast ts:datetime(\"yyMMddHHmmss\")\n;").is_err());
        assert!(parse("F:\n open log.csv\n |> (ts:datetime(\"yyMMddHHmmss\")) as t\n;").is_err());
        // The reader-schema form stays valid (unchanged).
        assert!(parse("F:\n open log.csv (ts:datetime(\"yyMMddHHmmss\"))\n;").is_ok());
        // A bare expression cast (no format) is valid and round-trips — incl. the
        // temporal lanes that the expr cast now recognizes (date/time/datetime).
        for src in [
            "F:\n open log.csv\n cast ts:datetime\n;",
            "F:\n open log.csv\n |> (ts:date) as d\n;",
            "F:\n open log.csv\n |> (t:time) as tm\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
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
    fn regex_infix_and_literal_parse_and_are_reversible() {
        // `EXPR ~ '…'` (§29.5-6 s4) round-trips; the regexp()/regex()/matches()
        // call spellings converge to the `~` canonical form when the pattern is
        // a literal (like s1's `:` chain convergence).
        let src = "F:\n open d.csv\n |? code ~ '^JP-\\d{4}$'\n;";
        let s = parse(src).unwrap().to_source();
        assert!(s.contains("code ~ '^JP-\\d{4}$'"), "infix lost in {s}");
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        for old in [
            "F:\n open d.csv\n |? regexp(code, \"^JP\")\n;",
            "F:\n open d.csv\n |? regex(code, \"^JP\")\n;",
            "F:\n open d.csv\n |? matches(code, \"^JP\")\n;",
            // The '…' literal is also accepted in the call's pattern slot.
            "F:\n open d.csv\n |? regexp(code, '^JP')\n;",
        ] {
            let s = parse(old).unwrap().to_source();
            assert!(s.contains("code ~ '^JP"), "no canonical ~ in {s}");
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
        // A pattern containing `'` has no raw '…' spelling → the call form is
        // kept (and stays reversible).
        let quoted = parse("F:\n open d.csv\n |? regexp(code, \"a'b\")\n;")
            .unwrap()
            .to_source();
        assert!(
            quoted.contains("regexp($_.code, \"a'b\")"),
            "quoted pattern must keep the call form: {quoted}"
        );
        assert_eq!(quoted, parse(&quoted).unwrap().to_source());
        // A computed (non-literal) pattern has no infix spelling either.
        let dynpat = parse("F:\n open d.csv\n |? regexp(code, pat)\n;")
            .unwrap()
            .to_source();
        assert!(
            dynpat.contains("regexp($_.code, $_.pat)"),
            "computed pattern must keep the call form: {dynpat}"
        );
        // A '…' literal anywhere but a pattern slot is a declaration-time error
        // (never a silent plain string), as is an unterminated literal.
        assert!(parse("F:\n open d.csv\n |? code == 'x'\n;").is_err());
        assert!(parse("F:\n open d.csv\n |? upper('x') == code\n;").is_err());
        assert!(parse("F:\n open d.csv\n |? code ~ 'abc\n;").is_err());
    }

    #[test]
    fn positional_reference_parses_and_is_reversible() {
        // `$_[i]` — positional column reference (§29.5-6 s4): 0-based, schema
        // order; the index must be a non-negative integer.
        let src = "F:\n open d.csv\n |? $_[0] == \"x\"\n |> ($_[1]) as second\n;";
        let s = parse(src).unwrap().to_source();
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        assert!(
            s.contains("$_[0]") && s.contains("$_[1]"),
            "positional refs lost in {s}"
        );
        assert!(parse("F:\n open d.csv\n |? $_[x] == 1\n;").is_err());
        assert!(parse("F:\n open d.csv\n |? $_[] == 1\n;").is_err());
        // An index past u32::MAX is a declaration-time error, never silently
        // truncated into a different (re-parsing) column (never-silent).
        assert!(parse("F:\n open d.csv\n |? $_[4294967296] == 1\n;").is_err());
        // The largest valid index round-trips its exact digits.
        let big = parse("F:\n open d.csv\n |? $_[4294967295] == 1\n;")
            .unwrap()
            .to_source();
        assert!(big.contains("$_[4294967295]"), "max index lost in {big}");
    }

    #[test]
    fn regex_infix_is_parenthesized_in_operator_context() {
        // The bare `~` infix is not an atom: a `~`/compare/cast/regex parent
        // must parenthesize it so `to_source` re-parses (IR reversibility).
        for src in [
            "F:\n open d.csv\n |? (code ~ '^JP') == true\n;",
            "F:\n open d.csv\n |? (code ~ '^JP') ~ 'true'\n;",
            "F:\n open d.csv\n |> ((code ~ '^JP'):int) as m\n;",
            // A cast over an arithmetic group also self-parenthesizes its
            // operand — the same projection-wrap fix makes it reversible
            // (was a pre-existing leading-paren regression, not s4-specific).
            "F:\n open d.csv\n |> ((a + b):int) as m\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        }
    }

    #[test]
    fn validate_bundle_round_trips_and_is_canonical_for_runs() {
        // `|! { pred disp; … }` (§29.5-6 s4) lowers to one `Op::Validate` per
        // entry (zero new IR, order preserved) and is the canonical spelling
        // for ≥2 consecutive contracts; a single contract stays flat.
        let src = "F:\n open d.csv\n |! { age >= 0 warn; age <= 120 reject }\n;";
        let g = parse(src).unwrap();
        let validates = g
            .nodes
            .iter()
            .filter(|n| matches!(n.op, Op::Validate { .. }))
            .count();
        assert_eq!(validates, 2, "bundle must lower to one Validate per entry");
        let s = g.to_source();
        assert!(
            s.contains("|! { $_.age >= 0 warn; $_.age <= 120 reject }"),
            "bundle spelling lost in {s}"
        );
        assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
        // Two flat consecutive contracts converge to the bundle…
        let flat = parse("F:\n open d.csv\n |! age >= 0 warn\n |! age <= 120 reject\n;")
            .unwrap()
            .to_source();
        assert_eq!(flat, s, "consecutive |! must converge to the bundle");
        // …a single one stays flat…
        let single = parse("F:\n open d.csv\n |! age >= 0 warn\n;")
            .unwrap()
            .to_source();
        assert!(
            single.contains("|! $_.age >= 0 warn") && !single.contains("|! {"),
            "single |! must stay flat: {single}"
        );
        // …and a comment pinned to the second contract breaks the run (trivia
        // is never repositioned or lost).
        let commented =
            parse("F:\n open d.csv\n |! age >= 0 warn\n # upper bound\n |! age <= 120 reject\n;")
                .unwrap()
                .to_source();
        assert!(
            commented.contains("# upper bound") && !commented.contains("|! {"),
            "a comment must break the bundle: {commented}"
        );
        assert_eq!(commented, parse(&commented).unwrap().to_source());
        // Empty bundles / missing dispositions are declaration-time errors.
        assert!(parse("F:\n open d.csv\n |! { }\n;").is_err());
        assert!(parse("F:\n open d.csv\n |! { age >= 0 }\n;").is_err());
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
                Op::Sink { route, .. } => route.path().map(str::to_string),
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
                Op::Sink { route, .. } => route.path().map(str::to_string),
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
            Op::Sink {
                codec: SinkCodec::Csv { .. },
                ..
            }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.jsonl\n;", 1),
            Op::Sink {
                codec: SinkCodec::Jsonl,
                ..
            }
        ));
        // `as json` (and a `.json` extension) is a JSON *array*; `.jsonl` /
        // `.ndjson` / `as jsonl` stay one-object-per-line.
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.dat as json\n;", 1),
            Op::Sink {
                codec: SinkCodec::Json,
                ..
            }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.json\n;", 1),
            Op::Sink {
                codec: SinkCodec::Json,
                ..
            }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.dat as jsonl\n;", 1),
            Op::Sink {
                codec: SinkCodec::Jsonl,
                ..
            }
        ));
        // `writejson` keeps emitting NDJSON (backward-compatible).
        assert!(matches!(
            nth_op("F:\n open a.csv\n writejson o.x\n;", 1),
            Op::Sink {
                codec: SinkCodec::Jsonl,
                ..
            }
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
            Op::Sink {
                codec: SinkCodec::Csv { .. },
                ..
            }
        ));
    }

    #[test]
    fn route_save_parses_validates_and_round_trips() {
        // §28.7 route (#143 ①): template placeholders derive the partition
        // keys (§27.3 is the degenerate form of §27.4); a plain path with
        // `by` is the Hive layout; `as flat` flattens names; `{{`/`}}` escape.
        for src in [
            "F:\n open a.csv\n save \"out/{country}.csv\"\n;",
            "F:\n open a.csv\n save \"out/\" by country region\n;",
            "F:\n open a.csv\n save \"out/\" by country as flat\n;",
            "F:\n open a.csv\n save \"out/\" as jsonl by country\n;",
            "F:\n open a.csv\n save \"out/{{lit}}_{k}.csv\"\n;",
            // A literal-only {{x}} template keeps its `by` (review #145: fmt
            // must never turn a partitioned save into a fixed one).
            "F:\n open a.csv\n save \"out/o_{{x}}/\" by k\n;",
            // Computed placeholders (s4c, #143 ①): the raw expression text
            // round-trips verbatim.
            "F:\n open a.csv\n save \"out/{substr(country,1,2)}_{id}.csv\"\n;",
        ] {
            let s = parse(src).unwrap().to_source();
            assert_eq!(s, parse(&s).unwrap().to_source(), "not reversible: {s}");
            assert!(s.contains("save \""), "route must stay quoted: {s}");
        }
        // Declaration-time errors (never-silent): a `by` key outside the
        // template, `as flat` on a template, empty/unclosed placeholders,
        // flat or by without keys.
        assert!(parse("F:\n open a.csv\n save \"out/{a}.csv\" by b\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/{a}.csv\" as flat\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/{}.csv\" by a\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/{a.csv\"\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/\" as flat\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/\" by\n;").is_err());
        // A computed placeholder is an anonymous key: `by` can only name {col}
        // placeholders, and a broken snippet is a declaration-time error.
        assert!(parse("F:\n open a.csv\n save \"out/{substr(a,1)}.csv\" by a\n;").is_err());
        assert!(parse("F:\n open a.csv\n save \"out/{substr(}.csv\"\n;").is_err());
        // A redundant `by` matching the template canonicalizes away (the
        // placeholders are the keys), and the derived keys land on the IR.
        let g = parse("F:\n open a.csv\n save \"out/{country}.csv\" by country\n;").unwrap();
        match &g.nodes[1].op {
            Op::Sink {
                route: Route::Template { by, flat, .. },
                ..
            } => {
                assert_eq!(by, &vec!["country".to_string()]);
                assert!(!flat);
            }
            o => panic!("expected a template sink, got {o:?}"),
        }
        let s = g.to_source();
        assert!(
            s.contains("save \"out/{country}.csv\"") && !s.contains(" by "),
            "derived keys must not re-emit `by`: {s}"
        );
        // A quoted plain path (no placeholders, no `by`) stays a fixed sink.
        assert!(matches!(
            nth_op("F:\n open a.csv\n save \"o.csv\"\n;", 1),
            Op::Sink {
                route: Route::Fixed(_),
                ..
            }
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
    fn to_source_round_trips_merge_and_join_scopes_with_downstream_stages() {
        // #186: a merge/join scope with downstream stages (`M: A + B |# c ;`)
        // used to render as an inline `-> M: + merge` branch — headless syntax
        // that does not re-parse, with the second input orphaned. It must render
        // as an independent scope with the binary head (`M:\n A + B\n |# c ;`)
        // and round-trip to the same DAG.
        for src in [
            "A: open a.csv (c:str) ;\nB: open b.csv (c:str) ;\nM: A + B |# c ;",
            "A: open a.csv (c:str) ;\nB: open b.csv (c:str) ;\nM: A + B |? c == \"x\" ;",
            "U: open u.csv (id:i64 a:i64) ;\nO: open o.csv (id:i64) ;\nJ: U & O on id |? a >= 1 ;",
            "U: open u.csv (id:i64 a:i64) ;\nO: open o.csv (id:i64) ;\nJ: U &left O on uid:oid sort a desc take 5 ;",
            // The tail-position special case must keep working unchanged.
            "A: open a.csv ;\nB: open b.csv ;\nM: A + B ;",
            "U: open u.csv ;\nO: open o.csv ;\nJ: U & O on id ;",
            // Three-way merge with a downstream stage.
            "A: open a.csv ;\nB: open b.csv ;\nC: open c.csv ;\nM: A + B + C take 3 ;",
        ] {
            let g1 = parse(src).unwrap();
            let s = g1.to_source();
            let g2 = parse(&s).unwrap_or_else(|e| {
                panic!("regenerated source does not re-parse for {src:?}:\n{s}\nerror: {e:?}")
            });
            assert_eq!(
                fingerprint(&g1),
                fingerprint(&g2),
                "round-trip changed the DAG for {src:?}; regenerated:\n{s}"
            );
            // Idempotence: rendering the re-parsed graph gives the same text.
            assert_eq!(s, g2.to_source(), "to_source not idempotent for {src:?}");
        }
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
            Op::Sink {
                codec: SinkCodec::Csv { delim: b'\t' },
                ..
            }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.csv as tsv\n;", 1),
            Op::Sink {
                codec: SinkCodec::Csv { delim: b'\t' },
                ..
            }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.csv\n;", 1),
            Op::Sink {
                codec: SinkCodec::Csv { delim: b',' },
                ..
            }
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
                let names =
                    |ks: Vec<PathExpr>| ks.iter().map(|k| k.to_string()).collect::<Vec<_>>();
                assert_eq!(names(lk), vec!["uid"]);
                assert_eq!(names(rk), vec!["oid"]);
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
        let names = |ks: Vec<PathExpr>| ks.iter().map(|k| k.to_string()).collect::<Vec<_>>();
        assert_eq!(names(lk), vec!["country", "region"]);
        assert_eq!(names(rk), vec!["country", "region"]);

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

    // §32 s2: group / sort / distinct / join keys are `PathExpr`. A **bare** key
    // round-trips as its plain name (the degenerate pin — byte-identical), and a
    // **nested** key (`user.age`) round-trips as `user.age`.
    #[test]
    fn path_expr_keys_round_trip_bare_and_nested() {
        // Bare keys stay plain across group / sort / distinct.
        let g =
            parse("G:\n open g.csv\n |# city sum:amount\n sort sum_amount desc\n distinct city\n;")
                .unwrap();
        let src = g.to_source();
        assert!(
            src.contains("|# city sum:amount"),
            "bare group key changed: {src}"
        );
        assert!(
            src.contains("sort sum_amount desc"),
            "bare sort key changed: {src}"
        );
        assert!(
            src.contains("distinct city"),
            "bare distinct key changed: {src}"
        );
        assert_eq!(
            src,
            parse(&src).unwrap().to_source(),
            "bare not idempotent: {src}"
        );

        // The group key is the degenerate (bare) PathExpr form.
        let key = g.nodes.iter().find_map(|n| match &n.op {
            Op::GroupBy { keys, .. } => keys.first().cloned(),
            _ => None,
        });
        assert_eq!(key.as_ref().and_then(|k| k.as_bare()), Some("city"));

        // A nested key parses to a path and round-trips as `user.age`.
        let d = parse("D:\n open d.csv\n |# user.age count\n;").unwrap();
        let dsrc = d.to_source();
        assert!(
            dsrc.contains("|# user.age"),
            "nested key not round-tripped: {dsrc}"
        );
        assert_eq!(
            dsrc,
            parse(&dsrc).unwrap().to_source(),
            "nested not idempotent: {dsrc}"
        );
        let nested = d.nodes.iter().find_map(|n| match &n.op {
            Op::GroupBy { keys, .. } => keys.first().cloned(),
            _ => None,
        });
        assert!(
            nested.is_some_and(|k| !k.is_bare()),
            "nested key should not be bare"
        );
    }

    // §32 s4: a nested path in *expression* context (predicate / computed column)
    // lowers to `Expr::Path` and round-trips as `user.age` / `tags[0]`. Covers the
    // s2 follow-up "`tags[0]` flow round-trip".
    #[test]
    fn path_expr_in_predicate_and_projection_round_trips() {
        // Flow-mode predicate: `user.age` folds to one token, lowers to a path.
        let g = parse("D:\n open d.jsonl\n |? user.age >= 18\n;").unwrap();
        let src = g.to_source();
        assert!(src.contains("user.age >= 18"), "path predicate lost: {src}");
        assert_eq!(
            src,
            parse(&src).unwrap().to_source(),
            "not idempotent: {src}"
        );

        // The predicate lowered to an `Expr::Path` (not a flat field).
        let is_path = g.nodes.iter().any(|n| match &n.op {
            Op::Filter { pred, .. } => pred.any(&|e| matches!(e, Expr::Path(_))),
            _ => false,
        });
        assert!(is_path, "predicate should contain an Expr::Path");

        // Computed column with a list index `tags[0]` (the s2 follow-up):
        // expression mode splits `tags [ 0 ]`; it round-trips as `tags[0]`.
        let p = parse("D:\n open d.jsonl\n |> (tags[0]) as first\n;").unwrap();
        let psrc = p.to_source();
        assert!(psrc.contains("tags[0]"), "list-index path lost: {psrc}");
        assert_eq!(
            psrc,
            parse(&psrc).unwrap().to_source(),
            "not idempotent: {psrc}"
        );

        // Deeper struct path in a computed column.
        let q = parse("D:\n open d.jsonl\n |> (user.address.city) as c\n;").unwrap();
        let qsrc = q.to_source();
        assert!(qsrc.contains("user.address.city"), "deep path lost: {qsrc}");
        assert_eq!(
            qsrc,
            parse(&qsrc).unwrap().to_source(),
            "not idempotent: {qsrc}"
        );
    }

    #[test]
    fn optional_leading_pipe_and_single_line_chaining() {
        // §25 / #171: a bare `|` before a keyword stage is optional input sugar.
        // It is NOT stored — `to_source` re-emits the canonical typed-pipe form
        // (no second canonical form), so a leading-pipe flow and its bare
        // equivalent produce byte-identical source.
        let with_pipes =
            parse("F:\n open a.csv\n | where age >= 30\n | sort score desc\n | save out.csv\n;")
                .unwrap();
        let plain =
            parse("F:\n open a.csv\n |? age >= 30\n sort score desc\n save out.csv\n;").unwrap();
        assert_eq!(
            with_pipes.to_source(),
            plain.to_source(),
            "leading-pipe sugar must normalize to the canonical form"
        );
        // The canonical form is the typed pipe `|?` (a bare field renders as
        // `$_.age`), with no leading bare `|` before the stage.
        let src = with_pipes.to_source();
        assert!(src.contains("|? $_.age >= 30"), "where→|? canonical: {src}");
        assert!(
            !src.lines().any(|l| l.trim_start().starts_with("| ")),
            "no leading bare pipe in canonical source: {src}"
        );
        assert_eq!(src, parse(&src).unwrap().to_source(), "idempotent: {src}");

        // Single-line chaining: stages separated by `|` on one line parse to the
        // same graph as the multi-line form.
        let one_line =
            parse("F:\n open a.csv | where age >= 30 | sort score desc | save out.csv\n;").unwrap();
        assert_eq!(
            one_line.to_source(),
            with_pipes.to_source(),
            "single-line == multi-line"
        );

        // A body-leading `|` (before any source) is still a parse error.
        assert!(parse("F:\n | open a.csv\n;").is_err());
        // `| name` stays named-flow reuse (§25.4), not leading-pipe sugar.
        assert!(
            parse("A:\n open a.csv\n |> x\n;\nB:\n open b.csv\n | A\n;").is_ok(),
            "named-flow apply must still parse"
        );
    }

    #[test]
    fn sugar_works_inside_literate_flow_fence_no_31_conflict() {
        // #171 sugar must not conflict with the §31 literate form (#162/#163): a
        // ```flow fence body is ordinary flow syntax, so leading pipes / single
        // line / `group` parse there too, and lower to the same graph as the
        // canonical `.riv`.
        let md = "# demo\n\nprose\n\n```flow\nG:\n open a.csv | where age >= 30 | group country sum:score\n;\n```\n";
        let from_md = parse_md(md).unwrap();
        let canonical = parse("G:\n open a.csv\n |? age >= 30\n |# country sum:score\n;").unwrap();
        assert_eq!(
            from_md.to_source(),
            canonical.to_source(),
            ".riv.md sugar must lower to the canonical graph (§31 stage-1 zero-semantic-change)"
        );
    }

    #[test]
    fn subscribe_parses_and_round_trips() {
        // §33 `net`: `subscribe "tcp://…"` is an unbounded source; `to_source`
        // re-quotes the endpoint with the `subscribe` verb (reversible), `as json`
        // round-trips. Parsing is always-std (no `net` feature); the plan is
        // flagged networked (gated `net`) and unbounded (no parallel/reorder).
        for src in [
            "F:\n subscribe \"tcp://127.0.0.1:9000\"\n |> name\n;",
            "J:\n subscribe \"tcp://host:9000\" as json\n |> name\n;",
        ] {
            let g = parse(src).unwrap();
            let s = g.to_source();
            assert!(s.contains("subscribe \"tcp://"), "subscribe quoted: {s}");
            assert_eq!(s, parse(&s).unwrap().to_source(), "not idempotent: {s}");
            assert!(g.uses_net(), "subscribe must set uses_net");
            assert!(g.uses_unbounded(), "subscribe is unbounded");
            assert!(!g.uses_watch(), "subscribe is not watch (gated apart)");
        }
    }

    #[test]
    fn http_open_parses_and_round_trips() {
        // §33 `net`: `open "http://…"` reads the URL as one quoted string token;
        // `to_source` re-quotes it (reversible), and `as json` selects the codec
        // for a URL with no extension. Parsing is always-std (no `net` feature).
        for src in [
            "A:\n open \"http://127.0.0.1:8080/data.csv\"\n |> name\n;",
            "J:\n open \"http://host/feed\" as json\n |> name\n;",
        ] {
            let g = parse(src).unwrap();
            let s = g.to_source();
            assert!(s.contains("open \"http://"), "URL quoted in to_source: {s}");
            assert_eq!(s, parse(&s).unwrap().to_source(), "not idempotent: {s}");
        }
        // The plan is flagged as networked (the runtime gates `net` pre-run).
        assert!(
            parse("A:\n open \"http://h/a.csv\"\n;").unwrap().uses_net(),
            "http source must set uses_net"
        );
    }

    #[test]
    fn array_agg_aliases_parse_and_canonicalize() {
        use rivus_ir::AggFunc;
        // §32 / #172: `array_agg`, `list_agg`, `arr` all lower to `ArrayAgg`; the
        // canonical `to_source` name is `array_agg` (one-way alias normalization).
        let op = nth_op(
            "G:\n open a.csv\n |# team array_agg:score list_agg:p arr:name\n;",
            1,
        );
        let Op::GroupBy { aggs, .. } = op else {
            panic!("expected GroupBy, got {op:?}");
        };
        assert!(
            aggs.iter().all(|(f, _)| *f == AggFunc::ArrayAgg),
            "all three aliases → ArrayAgg"
        );
        let g = parse("G:\n open a.csv\n |# t arr:score\n;").unwrap();
        let s = g.to_source();
        assert!(s.contains("array_agg:score"), "canonical array_agg: {s}");
        assert_eq!(s, parse(&s).unwrap().to_source(), "idempotent: {s}");
    }

    #[test]
    fn group_keyword_is_sugar_for_pipe_hash() {
        // §25 / #171: `group KEY… func:col…` is a readable alias for `|#`, and
        // `to_source` re-emits `|#` (the canonical form — `group` is input sugar).
        let kw = parse("G:\n open a.csv\n group country sum:score max:score\n;").unwrap();
        let sym = parse("G:\n open a.csv\n |# country sum:score max:score\n;").unwrap();
        assert_eq!(kw.to_source(), sym.to_source(), "group ≡ |#");
        assert!(kw.to_source().contains("|# country"), "canonical is |#");
        assert_eq!(kw.to_source(), parse(&kw.to_source()).unwrap().to_source());
        // The `group` alias and `| group` (with the optional leading pipe) agree.
        let piped = parse("G:\n open a.csv\n | group country sum:score max:score\n;").unwrap();
        assert_eq!(piped.to_source(), sym.to_source(), "| group ≡ |#");
        // A real column literally named `group` is still reachable via item("…").
        assert!(
            parse("G:\n open a.csv\n |> (item(\"group\")) as g\n;").is_ok(),
            "item(\"group\") escape must work"
        );
    }

    #[test]
    fn explode_parses_and_round_trips() {
        // §32 s4c: `explode COL` (and the `unnest` alias) lower to `Op::Explode`
        // and round-trip as `explode COL` (idempotent through to_source).
        let g = parse("E:\n open d.jsonl\n explode tags\n |> id tags\n;").unwrap();
        match nth_op("E:\n open d.jsonl\n explode tags\n;", 1) {
            Op::Explode { col } => assert_eq!(col, "tags"),
            other => panic!("expected Explode, got {other:?}"),
        }
        let src = g.to_source();
        assert!(src.contains("explode tags"), "explode lost: {src}");
        assert_eq!(
            src,
            parse(&src).unwrap().to_source(),
            "not idempotent: {src}"
        );

        // `unnest` is an alias and normalizes to `explode` in the source form.
        let u = parse("U:\n open d.jsonl\n unnest items\n;").unwrap();
        let usrc = u.to_source();
        assert!(usrc.contains("explode items"), "unnest alias: {usrc}");
        assert_eq!(
            usrc,
            parse(&usrc).unwrap().to_source(),
            "alias not idempotent"
        );
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
            Op::ProjectExpr { items, .. } => {
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
    fn unknown_function_errors_with_suggestion_and_list() {
        // #192: `squareroot(x)` used to parse as a bare field named `squareroot`
        // and evaluate to null in silence. Now it refuses at parse, teaches the
        // available list, and a near-miss gets a did-you-mean.
        let e = parse("F:\n open a.csv\n |? squareroot(age) > 2\n;")
            .expect_err("unknown function must not parse")
            .to_string();
        assert!(e.contains("unknown function 'squareroot'"), "named: {e}");
        assert!(e.contains("available:"), "lists the catalog: {e}");
        let e = parse("F:\n open a.csv\n |> (uper(name)) as u\n;")
            .expect_err("typo'd function must not parse")
            .to_string();
        assert!(e.contains("did you mean 'upper'"), "did-you-mean: {e}");
        // Known calls, type-word casts, and `case`/`resource` stay untouched.
        assert!(parse("F:\n open a.csv\n |> (upper(name)) as u\n;").is_ok());
        assert!(parse("F:\n open a.csv\n |? (age:int) >= 2\n;").is_ok());
    }

    #[test]
    fn parquet_source_parses_and_round_trips() {
        // SUPPLY-CHAIN adapter slice: `.parquet` resolves the codec from the
        // extension; a non-.parquet path spells `as parquet`. Parse/to_source
        // are std-only (any build); only *running* needs the feature.
        let g = parse("P:\n open data.parquet\n;").unwrap();
        assert!(g.uses_parquet(), "codec must be Parquet");
        let src = g.to_source();
        assert!(src.contains("open data.parquet\n"), "{src}");
        assert_eq!(src, parse(&src).unwrap().to_source(), "idempotent");
        let g = parse("P:\n open blob.bin as parquet\n;").unwrap();
        assert!(g.uses_parquet());
        assert!(g.to_source().contains("as parquet"), "{}", g.to_source());
        // The write side is a later slice — refused with guidance.
        let e = parse("P:\n open a.csv\n save out.parquet\n;")
            .expect_err("parquet sink must not parse")
            .to_string();
        assert!(e.contains("read-only in this slice"), "{e}");
    }

    #[test]
    fn hops_parses_and_round_trips() {
        // §36: sliding-window derived keys — hops + explode + group.
        let g = parse(
            "W:\n open t.csv (ts:datetime v:int)\n |> (hops(ts, \"2m\", \"1m\")) as w v\n explode w\n |# w avg:v\n;",
        )
        .unwrap();
        let src = g.to_source();
        assert!(src.contains("hops($_.ts, \"2m\", \"1m\")"), "{src}");
        assert_eq!(src, parse(&src).unwrap().to_source(), "idempotent");
    }

    #[test]
    fn map_and_bare_blocks_refuse_with_guidance() {
        // #203: `| map { … }` / `| { … }` used to parse and silently drop the
        // block (a no-op the user reads as a working transform).
        let e = parse("M:\n open a.csv\n | map { x }\n |> name\n;")
            .expect_err("map must not parse")
            .to_string();
        assert!(e.contains("`| map { … }` is not yet implemented"), "{e}");
        assert!(
            e.contains("|> (expr) as col"),
            "teaches the alternative: {e}"
        );
        let e = parse("M:\n open a.csv\n | { anything }\n;")
            .expect_err("bare block must not parse")
            .to_string();
        assert!(e.contains("bare `| { … }` block"), "{e}");
    }

    #[test]
    fn unary_minus_literals_parse_and_round_trip() {
        // #199: `split_part(p, "/", -1)` and `(x * -1)` forms.
        let g = parse("N:\n open a.csv\n |> ((age * -1)) as neg\n;").unwrap();
        assert_eq!(g.to_source(), parse(&g.to_source()).unwrap().to_source());
        let g = parse("N:\n open a.csv\n |> ((price * -2.5)) as neg\n;").unwrap();
        assert!(g.to_source().contains("-2.5"), "{}", g.to_source());
        // General negation of a column stays a clear error, with the spelled fix.
        let e = parse("N:\n open a.csv\n |> ((-age)) as neg\n;")
            .expect_err("unary minus on a column must not parse")
            .to_string();
        assert!(e.contains("0 - expr"), "teaches the general form: {e}");
    }

    #[test]
    fn path_funcs_parse_and_round_trip() {
        // #199: basename / stem / dirname.
        let g =
            parse("P:\n open a.csv\n |> (basename(p)) as b (stem(p)) as s (dirname(p)) as d\n;")
                .unwrap();
        let src = g.to_source();
        for f in ["basename(", "stem(", "dirname("] {
            assert!(src.contains(f), "{f} lost in round-trip: {src}");
        }
        assert_eq!(src, parse(&src).unwrap().to_source(), "idempotent");
    }

    #[test]
    fn sessionize_parses_and_round_trips() {
        // §36.5: session windows — ts, gap duration, optional by columns.
        let g =
            parse("S:\n open e.csv (ts:datetime user:str)\n sessionize ts gap \"30m\" by user\n;")
                .unwrap();
        let src = g.to_source();
        assert!(src.contains("sessionize ts gap \"30m\" by user"), "{src}");
        assert_eq!(src, parse(&src).unwrap().to_source(), "idempotent");
        // Without `by` (single implicit group).
        let g = parse("S:\n open e.csv (ts:datetime)\n sessionize ts gap \"1h\"\n;").unwrap();
        assert!(
            g.to_source().contains("sessionize ts gap \"1h\"\n"),
            "{}",
            g.to_source()
        );
        // Malformed forms teach the shape.
        let e = parse("S:\n open e.csv\n sessionize ts \"30m\"\n;")
            .expect_err("gap keyword required")
            .to_string();
        assert!(e.contains("gap \"DUR\""), "{e}");
        let e = parse("S:\n open e.csv\n sessionize ts gap 30\n;")
            .expect_err("gap must be a duration string")
            .to_string();
        assert!(e.contains("duration string"), "{e}");
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
                Op::Sink { route, .. } => route.path().map(str::to_string),
                _ => None,
            })
            .unwrap();
        assert_eq!(sink, "-");
    }
}
