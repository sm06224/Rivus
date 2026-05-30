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
//! transform  := '|?' expr | '|>' field+ | '|#' field | '|' 'map' block
//! branch     := '->' IDENT ':' body ';'
//! sink       := 'save' PATH | 'print'
//! hook       := 'on' EVENT ('severity' '>=' SEV)? ':' action ';'
//! ```

mod lexer;

use lexer::{Lexer, Tok};
use rivus_core::{Mode, RivusError, Severity, Value};
use rivus_ir::{
    Access, BinType, CmpOp, EdgeKind, Endian, Expr, Hook, HookAction, HookEvent, NodeId, Op,
    PlanGraph,
};

pub fn parse(src: &str) -> Result<PlanGraph, RivusError> {
    let toks = Lexer::new(src).tokenize().map_err(RivusError::Parse)?;
    let mut p = Parser {
        toks,
        pos: 0,
        g: PlanGraph::new(),
    };
    p.parse_program()?;
    Ok(p.g)
}

struct Parser {
    toks: Vec<(Tok, u32)>,
    pos: usize,
    g: PlanGraph,
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

    fn peek_is_word(&self, w: &str) -> bool {
        matches!(self.tok(), Tok::Word(x) if x == w)
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
        let mut current = self.parse_body_head(input)?;

        loop {
            match self.tok().clone() {
                Tok::PipeFilter => {
                    self.bump();
                    let pred = self.parse_expr()?;
                    let n = self.g.add_node(Op::Filter { pred });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::PipeMap => {
                    self.bump();
                    let fields = self.parse_field_list()?;
                    let n = self.g.add_node(Op::Project { fields });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::PipeGroup => {
                    self.bump();
                    let key = self.word()?;
                    let n = self.g.add_node(Op::GroupBy { key });
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Pipe => {
                    // `| map { ... }` — MVP: parse and skip the block (no-op).
                    self.bump();
                    if self.peek_is_word("map") {
                        self.bump();
                    }
                    self.skip_block()?;
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
                    let path = self.word()?;
                    let explicit = if self.peek_is_word("as") {
                        self.bump();
                        Some(self.word()?)
                    } else {
                        None
                    };
                    let fmt = resolve_format(&path, explicit.as_deref()).ok_or_else(|| {
                        self.err(format!("unknown format '{}'", explicit.unwrap_or_default()))
                    })?;
                    let n = self.g.add_node(fmt.into_sink_op(path));
                    self.g.add_edge(current, n, EdgeKind::Stream);
                    current = n;
                }
                Tok::Word(w) if w == "writecsv" => {
                    self.bump();
                    let path = self.word()?;
                    let n = self.g.add_node(Op::SinkCsv { path });
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
        }
        Ok(current)
    }

    /// Parse the first element of a body: a source, a stream replay, a
    /// merge/join over named scopes, or (for branch children) the inherited
    /// upstream node.
    fn parse_body_head(&mut self, input: Option<NodeId>) -> Result<NodeId, RivusError> {
        match self.tok().clone() {
            // `open PATH [as FMT]` — extension is only the default; an explicit
            // `as csv|tsv|json|jsonl|ndjson` overrides it (and works when the
            // path has no/odd extension). `readcsv`/`readjson`/`readbin` are
            // equivalent explicit aliases (lower cognitive load, fewer surprises).
            Tok::Word(w) if w == "open" => {
                self.bump();
                let path = self.word()?;
                let explicit = if self.peek_is_word("as") {
                    self.bump();
                    Some(self.word()?)
                } else {
                    None
                };
                let fmt = resolve_format(&path, explicit.as_deref()).ok_or_else(|| {
                    self.err(format!("unknown format '{}'", explicit.unwrap_or_default()))
                })?;
                Ok(self.g.add_node(fmt.into_op(path)))
            }
            Tok::Word(w) if w == "readcsv" => {
                self.bump();
                let path = self.word()?;
                Ok(self.g.add_node(Op::OpenCsv {
                    path,
                    projection: None,
                }))
            }
            Tok::Word(w) if w == "readjson" => {
                self.bump();
                let path = self.word()?;
                Ok(self.g.add_node(Op::OpenJsonl { path }))
            }
            Tok::Word(w) if w == "stream" => {
                self.bump();
                let name = self.word()?;
                Ok(self.g.add_node(Op::StreamRef { name }))
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
                Ok(self.g.add_node(Op::OpenBinary {
                    path,
                    fields,
                    endian,
                    c_align,
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
                    let rhs = self.word()?;
                    let rid = *self
                        .g
                        .labels
                        .get(&rhs)
                        .ok_or_else(|| self.err(format!("unknown flow '{rhs}'")))?;
                    // MVP: join on a key named after the right scope, refined later.
                    let join = self.g.add_node(Op::Join {
                        left_key: "id".into(),
                        right_key: "id".into(),
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

    fn parse_field_list(&mut self) -> Result<Vec<String>, RivusError> {
        let mut fields = Vec::new();
        while let Tok::Word(w) = self.tok() {
            if is_keyword(w) {
                break;
            }
            fields.push(w.clone());
            self.bump();
        }
        if fields.is_empty() {
            return Err(self.err("`|>` requires at least one field name"));
        }
        Ok(fields)
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
        let left = self.parse_primary()?;
        if let Tok::Cmp(op) = self.tok().clone() {
            self.bump();
            let right = self.parse_primary()?;
            Ok(Expr::Compare {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, RivusError> {
        match self.tok().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(Expr::Literal(Value::I64(n)))
            }
            Tok::Float(f) => {
                self.bump();
                Ok(Expr::Literal(Value::F64(f)))
            }
            Tok::Str(s) => {
                self.bump();
                Ok(Expr::Literal(Value::Str(s)))
            }
            Tok::DollarCur | Tok::DollarStack(_) => {
                self.bump();
                self.parse_field_tail()
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
            // Bare field of the current object: `age`.
            Tok::Word(name) => {
                self.bump();
                Ok(Expr::Field {
                    name,
                    access: Access::Fast,
                })
            }
            other => Err(self.err(format!("unexpected token in expression: {other:?}"))),
        }
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

/// A text source format selectable on `open` (binary goes via `readbin`).
enum Format {
    Csv,
    Jsonl,
}

impl Format {
    fn into_op(self, path: String) -> Op {
        match self {
            Format::Csv => Op::OpenCsv {
                path,
                projection: None,
            },
            Format::Jsonl => Op::OpenJsonl { path },
        }
    }

    fn into_sink_op(self, path: String) -> Op {
        match self {
            Format::Csv => Op::SinkCsv { path },
            Format::Jsonl => Op::SinkJsonl { path },
        }
    }
}

/// Resolve the format for `open`: an explicit `as FMT` wins; otherwise fall back
/// to the file extension; otherwise default to CSV. Returns `None` only for an
/// unrecognized explicit format name.
fn resolve_format(path: &str, explicit: Option<&str>) -> Option<Format> {
    if let Some(f) = explicit {
        return match f.to_ascii_lowercase().as_str() {
            "csv" | "tsv" => Some(Format::Csv),
            "json" | "jsonl" | "ndjson" => Some(Format::Jsonl),
            _ => None,
        };
    }
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".jsonl") || lower.ends_with(".ndjson") {
        Some(Format::Jsonl)
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
            | "writecsv"
            | "writejson"
            | "stream"
            | "save"
            | "print"
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
        assert!(matches!(
            nth_op("F:\n open a.csv\n save o.dat as json\n;", 1),
            Op::SinkJsonl { .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n writejson o.x\n;", 1),
            Op::SinkJsonl { .. }
        ));
        assert!(matches!(
            nth_op("F:\n open a.csv\n writecsv o.x\n;", 1),
            Op::SinkCsv { .. }
        ));
    }

    #[test]
    fn format_selection_extension_alias_and_override() {
        // Default: extension picks the format.
        assert!(matches!(first_op("F:\n open d.csv\n;"), Op::OpenCsv { .. }));
        assert!(matches!(
            first_op("F:\n open d.jsonl\n;"),
            Op::OpenJsonl { .. }
        ));
        assert!(matches!(
            first_op("F:\n open d.ndjson\n;"),
            Op::OpenJsonl { .. }
        ));
        // Odd/absent extension defaults to CSV...
        assert!(matches!(first_op("F:\n open d.dat\n;"), Op::OpenCsv { .. }));
        // ...but `as FMT` overrides the extension entirely.
        assert!(matches!(
            first_op("F:\n open d.dat as json\n;"),
            Op::OpenJsonl { .. }
        ));
        assert!(matches!(
            first_op("F:\n open d.jsonl as csv\n;"),
            Op::OpenCsv { .. }
        ));
        // Explicit aliases ignore the extension.
        assert!(matches!(
            first_op("F:\n readjson d.weird\n;"),
            Op::OpenJsonl { .. }
        ));
        assert!(matches!(
            first_op("F:\n readcsv d.weird\n;"),
            Op::OpenCsv { .. }
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
}
