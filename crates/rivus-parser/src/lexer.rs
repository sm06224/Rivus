//! Hand-written lexer for the Unified Flow Syntax.
//!
//! Whitespace and newlines are insignificant (statements are delimited
//! structurally by their leading token and by `;`). `#` begins a line comment,
//! except inside the `|#` operator.

use rivus_ir::CmpOp;

/// A comment tagged with the index of the token it immediately precedes.
pub type Comment = (usize, String);

/// The lexer's output: the token stream (token + source line) and the comment
/// trivia gathered alongside it (§25.7).
pub type Tokens = (Vec<(Tok, u32)>, Vec<Comment>);

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Colon,            // :
    Semicolon,        // ;
    Comma,            // ,
    Assign,           // =   (value-hole binding, e.g. `| clean min=0`)
    Bang,             // !
    Plus,             // +
    Minus,            // -   (expression mode only)
    Star,             // *   (expression mode only)
    Slash,            // /   (expression mode only)
    Percent,          // %   (expression mode only)
    Amp,              // &
    At,               // @   (union sub-view offset, e.g. `cls@0..3` — §29.3 s2)
    Arrow,            // ->
    Dot,              // .
    DotDot,           // ..
    LParen,           // (
    RParen,           // )
    LBrace,           // {
    RBrace,           // }
    LBracket,         // [   (binary `char[N]` width — §29.4)
    RBracket,         // ]
    PipeFilter,       // |?
    PipeMap,          // |>
    PipeGroup,        // |#
    PipeValidate,     // |!
    Pipe,             // |
    Cmp(CmpOp),       // == != < <= > >=
    DollarCur,        // $_
    DollarStack(u32), // $_:N
    Hole(String),     // $name — a value hole (§25.3)
    Int(i64),
    /// A float literal: its `f64` value (for the f64/i64 lanes) **and** the exact
    /// decimal it was written as (natural scale = fractional-digit count). The
    /// decimal is kept so a comparison against a `decimal` column never has to
    /// round the literal (accounting contract; design 21 §21.4).
    Float(f64, rivus_core::Decimal),
    Str(String),
    Word(String),
    Eof,
}

#[derive(Debug, Clone)]
pub struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
    pub line: u32,
    /// Parenthesis nesting depth. Inside `( … )` the lexer switches to
    /// *expression mode*: `- * / %` become operator tokens and identifiers no
    /// longer absorb `- / .`. Outside parens the path-friendly tokenization is
    /// unchanged, so `open /tmp/a-b.csv` still lexes as one word.
    depth: u32,
    /// Comment trivia gathered while skipping whitespace, not yet attached to a
    /// token. Drained in `tokenize` onto the index of the token they precede
    /// (`#86`/§25.7: comments are inert trivia preserved through the IR so
    /// `rivus fmt` round-trips them). Each entry is already canonicalized to its
    /// re-emittable form (`# line` or `#{ block }#`).
    pending: Vec<String>,
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Lexer {
            src: src.as_bytes(),
            pos: 0,
            line: 1,
            depth: 0,
            pending: Vec::new(),
        }
    }

    fn peek(&self) -> u8 {
        *self.src.get(self.pos).unwrap_or(&0)
    }

    fn peek2(&self) -> u8 {
        *self.src.get(self.pos + 1).unwrap_or(&0)
    }

    fn bump(&mut self) -> u8 {
        let c = self.peek();
        self.pos += 1;
        if c == b'\n' {
            self.line += 1;
        }
        c
    }

    fn skip_trivia(&mut self) {
        loop {
            let c = self.peek();
            if c == b' ' || c == b'\t' || c == b'\r' || c == b'\n' {
                self.bump();
            } else if c == b'#' && self.peek2() == b'{' {
                // Block comment `#{ ... }#` — inert trivia, preserved (§25.7).
                // Spans lines; terminated by the first `}#` (lenient: an
                // unterminated block runs to EOF rather than erroring).
                self.bump(); // '#'
                self.bump(); // '{'
                let start = self.pos;
                loop {
                    let d = self.peek();
                    if d == 0 {
                        self.push_comment(start, self.pos, true);
                        break;
                    }
                    if d == b'}' && self.peek2() == b'#' {
                        let end = self.pos;
                        self.bump(); // '}'
                        self.bump(); // '#'
                        self.push_comment(start, end, true);
                        break;
                    }
                    self.bump();
                }
            } else if c == b'#' {
                // Line comment `# ...` (v1 form) — also preserved as trivia.
                self.bump(); // '#'
                let start = self.pos;
                while self.peek() != b'\n' && self.peek() != 0 {
                    self.bump();
                }
                self.push_comment(start, self.pos, false);
            } else {
                break;
            }
        }
    }

    /// Canonicalize a comment's inner bytes (`src[start..end]`) into its
    /// re-emittable trivia form and buffer it. `block` chooses `#{ … }#` vs the
    /// line form `# …`; the inner text is trimmed so `fmt` is idempotent.
    fn push_comment(&mut self, start: usize, end: usize, block: bool) {
        let text = std::str::from_utf8(&self.src[start..end])
            .unwrap_or("")
            .trim();
        self.pending.push(if block {
            format!("#{{ {text} }}#")
        } else {
            format!("# {text}")
        });
    }

    /// Tokenize the whole input. On a lexical error returns the message and the
    /// line it occurred on. The second result holds comment trivia, each tagged
    /// with the index of the token it immediately precedes (so the parser can
    /// attach it to the node that follows — §25.7 inert-trivia preservation).
    pub fn tokenize(mut self) -> Result<Tokens, String> {
        let mut out = Vec::new();
        let mut comments: Vec<Comment> = Vec::new();
        loop {
            self.skip_trivia();
            // The comments gathered during this skip precede the token we are
            // about to push (its index is `out.len()`).
            for c in self.pending.drain(..) {
                comments.push((out.len(), c));
            }
            let line = self.line;
            let c = self.peek();
            if c == 0 {
                out.push((Tok::Eof, line));
                return Ok((out, comments));
            }
            let tok = match c {
                b':' => {
                    self.bump();
                    Tok::Colon
                }
                b';' => {
                    self.bump();
                    Tok::Semicolon
                }
                b',' => {
                    self.bump();
                    Tok::Comma
                }
                b'+' => {
                    self.bump();
                    Tok::Plus
                }
                b'&' => {
                    self.bump();
                    Tok::Amp
                }
                b'(' => {
                    self.bump();
                    self.depth += 1;
                    Tok::LParen
                }
                b')' => {
                    self.bump();
                    self.depth = self.depth.saturating_sub(1);
                    Tok::RParen
                }
                b'{' => {
                    self.bump();
                    Tok::LBrace
                }
                b'}' => {
                    self.bump();
                    Tok::RBrace
                }
                b'[' => {
                    self.bump();
                    Tok::LBracket
                }
                b']' => {
                    self.bump();
                    Tok::RBracket
                }
                b'!' => {
                    self.bump();
                    if self.peek() == b'=' {
                        self.bump();
                        Tok::Cmp(CmpOp::Ne)
                    } else {
                        Tok::Bang
                    }
                }
                b'=' => {
                    self.bump();
                    if self.peek() == b'=' {
                        self.bump();
                        Tok::Cmp(CmpOp::Eq)
                    } else {
                        // A lone `=` is the binding assignment (`| clean min=0`,
                        // §25.3). In a predicate the parser still rejects it,
                        // pointing at `==`.
                        Tok::Assign
                    }
                }
                b'<' => {
                    self.bump();
                    if self.peek() == b'=' {
                        self.bump();
                        Tok::Cmp(CmpOp::Le)
                    } else {
                        Tok::Cmp(CmpOp::Lt)
                    }
                }
                b'>' => {
                    self.bump();
                    if self.peek() == b'=' {
                        self.bump();
                        Tok::Cmp(CmpOp::Ge)
                    } else {
                        Tok::Cmp(CmpOp::Gt)
                    }
                }
                b'-' if self.peek2() == b'>' => {
                    self.bump();
                    self.bump();
                    Tok::Arrow
                }
                b'.' if self.peek2() == b'.' => {
                    self.bump();
                    self.bump();
                    Tok::DotDot
                }
                b'.' => {
                    self.bump();
                    Tok::Dot
                }
                // `@` — union sub-view offset separator (`cls@0..3`, §29.3 s2).
                b'@' => {
                    self.bump();
                    Tok::At
                }
                // Expression-mode arithmetic operators (only inside parens, so
                // they never shadow `->`, merge `+`, or path words like `a/b`).
                b'-' if self.depth > 0 => {
                    self.bump();
                    Tok::Minus
                }
                // A lone `-` outside parens is the stdin/stdout sentinel word
                // (`open -`, `save -`). `->` and expression `-` are handled
                // above, so this only fires for a dash not followed by `>`.
                b'-' => {
                    self.bump();
                    Tok::Word("-".to_string())
                }
                b'*' if self.depth > 0 => {
                    self.bump();
                    Tok::Star
                }
                b'/' if self.depth > 0 => {
                    self.bump();
                    Tok::Slash
                }
                b'%' if self.depth > 0 => {
                    self.bump();
                    Tok::Percent
                }
                b'|' => {
                    self.bump();
                    match self.peek() {
                        b'?' => {
                            self.bump();
                            Tok::PipeFilter
                        }
                        b'>' => {
                            self.bump();
                            Tok::PipeMap
                        }
                        b'#' => {
                            self.bump();
                            Tok::PipeGroup
                        }
                        b'!' => {
                            self.bump();
                            Tok::PipeValidate
                        }
                        _ => Tok::Pipe,
                    }
                }
                b'$' => self.lex_dollar(line)?,
                b'"' => self.lex_string(line)?,
                c if c.is_ascii_digit() => self.lex_number(),
                c if is_word_start(c) => self.lex_word(),
                other => {
                    return Err(format!(
                        "unexpected character {:?} at line {line}",
                        other as char
                    ))
                }
            };
            out.push((tok, line));
        }
    }

    fn lex_dollar(&mut self, line: u32) -> Result<Tok, String> {
        self.bump(); // '$'
                     // `$_` / `$_:N` — current / parent-scope object accessors.
        if self.peek() == b'_' {
            self.bump(); // '_'
            if self.peek() == b':' && self.peek2().is_ascii_digit() {
                self.bump(); // ':'
                let mut n = 0u32;
                while self.peek().is_ascii_digit() {
                    n = n * 10 + (self.bump() - b'0') as u32;
                }
                return Ok(Tok::DollarStack(n));
            }
            return Ok(Tok::DollarCur);
        }
        // `$name` — a value hole (§25.3). Name is `[A-Za-z][A-Za-z0-9_]*`.
        if self.peek().is_ascii_alphabetic() {
            let start = self.pos;
            while self.peek().is_ascii_alphanumeric() || self.peek() == b'_' {
                self.bump();
            }
            let name = std::str::from_utf8(&self.src[start..self.pos])
                .unwrap()
                .to_string();
            return Ok(Tok::Hole(name));
        }
        Err(format!("expected '$_' or '$name' at line {line}"))
    }

    fn lex_string(&mut self, line: u32) -> Result<Tok, String> {
        self.bump(); // opening quote
                     // Accumulate bytes, not byte-as-`char`: a multi-byte UTF-8 literal in a
                     // string (e.g. the `[ja-jp]` format `"yyyy年MM月dd日"`, §29 s3) must be
                     // copied verbatim — pushing each byte as a char re-encodes it into
                     // mojibake. The source is valid UTF-8 and escapes are ASCII, so the
                     // collected bytes are valid too; the check is belt-and-braces.
        let mut s: Vec<u8> = Vec::new();
        loop {
            let c = self.peek();
            match c {
                0 => return Err(format!("unterminated string at line {line}")),
                b'"' => {
                    self.bump();
                    return String::from_utf8(s)
                        .map(Tok::Str)
                        .map_err(|_| format!("invalid UTF-8 in string at line {line}"));
                }
                b'\\' => {
                    self.bump();
                    let e = self.bump();
                    s.push(match e {
                        b'n' => b'\n',
                        b't' => b'\t',
                        b'"' => b'"',
                        b'\\' => b'\\',
                        other => other,
                    });
                }
                _ => s.push(self.bump()),
            }
        }
    }

    fn lex_number(&mut self) -> Tok {
        let start = self.pos;
        while self.peek().is_ascii_digit() {
            self.bump();
        }
        let mut is_float = false;
        if self.peek() == b'.' && self.peek2().is_ascii_digit() {
            is_float = true;
            self.bump();
            while self.peek().is_ascii_digit() {
                self.bump();
            }
        }
        let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        if is_float {
            // Natural scale = digits after the dot; build the exact decimal from
            // the same text (no f64 round-trip), keeping the f64 value too.
            let frac = text
                .split_once('.')
                .map(|(_, f)| f.len() as u8)
                .unwrap_or(0);
            let dec = rivus_core::Decimal::parse_scaled(text, frac)
                .unwrap_or_else(|| rivus_core::Decimal::new(0, frac));
            Tok::Float(text.parse().unwrap(), dec)
        } else {
            Tok::Int(text.parse().unwrap())
        }
    }

    fn lex_word(&mut self) -> Tok {
        let start = self.pos;
        while self.word_part(self.peek()) {
            self.bump();
        }
        let text = std::str::from_utf8(&self.src[start..self.pos]).unwrap();
        Tok::Word(text.to_string())
    }

    /// Word-continuation rule, depth-aware: inside parens an identifier is a
    /// plain `[A-Za-z0-9_]+` so it splits cleanly from `- / .` operators;
    /// outside parens it stays path-friendly (`users.csv`, `data/out`, `a-b`).
    fn word_part(&self, c: u8) -> bool {
        if self.depth > 0 {
            c.is_ascii_alphanumeric() || c == b'_'
        } else {
            is_word_part(c)
        }
    }
}

fn is_word_start(c: u8) -> bool {
    // `/` is allowed so absolute/relative file paths (`/tmp/x.csv`, `./out`)
    // lex as a single word in source/sink positions.
    c.is_ascii_alphabetic() || c == b'_' || c == b'/'
}

/// Words may contain dots, slashes and dashes so that file paths
/// (`users.csv`, `data/out.parquet`) lex as a single token. A leading `.` is
/// never absorbed because `lex_word` only starts on an alphabetic/underscore.
fn is_word_part(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'.' || c == b'/' || c == b'-'
}
