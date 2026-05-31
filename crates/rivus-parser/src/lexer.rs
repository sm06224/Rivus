//! Hand-written lexer for the Unified Flow Syntax.
//!
//! Whitespace and newlines are insignificant (statements are delimited
//! structurally by their leading token and by `;`). `#` begins a line comment,
//! except inside the `|#` operator.

use rivus_ir::CmpOp;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Colon,            // :
    Semicolon,        // ;
    Comma,            // ,
    Bang,             // !
    Plus,             // +
    Minus,            // -   (expression mode only)
    Star,             // *   (expression mode only)
    Slash,            // /   (expression mode only)
    Percent,          // %   (expression mode only)
    Amp,              // &
    Arrow,            // ->
    Dot,              // .
    DotDot,           // ..
    LParen,           // (
    RParen,           // )
    LBrace,           // {
    RBrace,           // }
    PipeFilter,       // |?
    PipeMap,          // |>
    PipeGroup,        // |#
    Pipe,             // |
    Cmp(CmpOp),       // == != < <= > >=
    DollarCur,        // $_
    DollarStack(u32), // $_:N
    Int(i64),
    Float(f64),
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
}

impl<'a> Lexer<'a> {
    pub fn new(src: &'a str) -> Self {
        Lexer {
            src: src.as_bytes(),
            pos: 0,
            line: 1,
            depth: 0,
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
            } else if c == b'#' {
                // line comment
                while self.peek() != b'\n' && self.peek() != 0 {
                    self.bump();
                }
            } else {
                break;
            }
        }
    }

    /// Tokenize the whole input. On a lexical error returns the message and the
    /// line it occurred on.
    pub fn tokenize(mut self) -> Result<Vec<(Tok, u32)>, String> {
        let mut out = Vec::new();
        loop {
            self.skip_trivia();
            let line = self.line;
            let c = self.peek();
            if c == 0 {
                out.push((Tok::Eof, line));
                return Ok(out);
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
                        return Err(format!("unexpected '=' (did you mean '==') at line {line}"));
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
        if self.peek() != b'_' {
            return Err(format!("expected '$_' at line {line}"));
        }
        self.bump(); // '_'
        if self.peek() == b':' && self.peek2().is_ascii_digit() {
            self.bump(); // ':'
            let mut n = 0u32;
            while self.peek().is_ascii_digit() {
                n = n * 10 + (self.bump() - b'0') as u32;
            }
            Ok(Tok::DollarStack(n))
        } else {
            Ok(Tok::DollarCur)
        }
    }

    fn lex_string(&mut self, line: u32) -> Result<Tok, String> {
        self.bump(); // opening quote
        let mut s = String::new();
        loop {
            let c = self.peek();
            match c {
                0 => return Err(format!("unterminated string at line {line}")),
                b'"' => {
                    self.bump();
                    return Ok(Tok::Str(s));
                }
                b'\\' => {
                    self.bump();
                    let e = self.bump();
                    s.push(match e {
                        b'n' => '\n',
                        b't' => '\t',
                        b'"' => '"',
                        b'\\' => '\\',
                        other => other as char,
                    });
                }
                _ => s.push(self.bump() as char),
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
            Tok::Float(text.parse().unwrap())
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
