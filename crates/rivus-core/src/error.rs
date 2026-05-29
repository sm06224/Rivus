//! The Error Stream.
//!
//! Continue-first (Master principle #2): errors are *observable events on a
//! side-channel stream*, not control-flow that unwinds the stack. Only
//! `Severity::Fatal` is permitted to halt the graph. Everything else flows down
//! a parallel error edge and the main flow keeps going.

use std::fmt;

/// Error severity ladder. `Ord` is derived so runtime conditions such as
/// `on error severity >= warning:` are a simple comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warn,
    Recoverable,
    Critical,
    /// The only severity that terminates the graph.
    Fatal,
}

impl Severity {
    pub fn parse(s: &str) -> Option<Severity> {
        Some(match s.to_ascii_lowercase().as_str() {
            "info" => Severity::Info,
            "warn" | "warning" => Severity::Warn,
            "recoverable" => Severity::Recoverable,
            "critical" => Severity::Critical,
            "fatal" => Severity::Fatal,
            _ => return None,
        })
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Severity::Info => "info",
            Severity::Warn => "warn",
            Severity::Recoverable => "recoverable",
            Severity::Critical => "critical",
            Severity::Fatal => "fatal",
        };
        f.write_str(s)
    }
}

/// At what granularity an error was raised (Observability spec §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorScope {
    Item,
    Chunk,
    Branch,
    Graph,
}

/// A single event on the error stream.
#[derive(Debug, Clone)]
pub struct ErrorEvent {
    pub severity: Severity,
    pub scope: ErrorScope,
    pub message: String,
    /// Label of the flow node that raised it, if known.
    pub node: Option<String>,
    pub chunk_id: Option<u64>,
}

impl ErrorEvent {
    pub fn new(severity: Severity, scope: ErrorScope, message: impl Into<String>) -> Self {
        ErrorEvent {
            severity,
            scope,
            message: message.into(),
            node: None,
            chunk_id: None,
        }
    }

    pub fn at_node(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    pub fn at_chunk(mut self, id: u64) -> Self {
        self.chunk_id = Some(id);
        self
    }

    pub fn is_fatal(&self) -> bool {
        self.severity == Severity::Fatal
    }
}

impl fmt::Display for ErrorEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}]", self.severity)?;
        if let Some(node) = &self.node {
            write!(f, " {node}")?;
        }
        if let Some(id) = self.chunk_id {
            write!(f, " chunk {id}")?;
        }
        write!(f, ": {}", self.message)
    }
}

/// Fatal, non-recoverable errors that abort graph construction or execution.
/// (Distinct from the error *stream*, which carries non-fatal events.)
#[derive(Debug, Clone)]
pub enum RivusError {
    Parse(String),
    Build(String),
    Io(String),
    Runtime(String),
}

impl fmt::Display for RivusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RivusError::Parse(m) => write!(f, "parse error: {m}"),
            RivusError::Build(m) => write!(f, "build error: {m}"),
            RivusError::Io(m) => write!(f, "io error: {m}"),
            RivusError::Runtime(m) => write!(f, "runtime error: {m}"),
        }
    }
}

impl std::error::Error for RivusError {}

pub type Result<T> = std::result::Result<T, RivusError>;
