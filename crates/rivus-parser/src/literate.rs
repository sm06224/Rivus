//! `.riv.md` — Rivus Literate authoring form (§31 stage 1).
//!
//! A `.riv.md` document layers three roles (§31.1), never mixed:
//!
//! * **YAML frontmatter** (`---` fenced, at the very top) — declarations:
//!   (R) resource hints, (C) `needs` capability *declarations*, and meta
//!   (`title`). (S) semantic config never lives here (§31.3) — it stays
//!   in-script, which is what keeps stage 1 a *zero semantic change*.
//! * **Markdown prose** — an "enhanced comment": inert, carries no meaning,
//!   and round-trips verbatim (§31.1). The parser only needs to find the
//!   frontmatter and the ```` ```flow ```` fences; everything else is trivia.
//! * **```` ```flow ```` fences** — the executed pipeline (ordinary flow
//!   syntax). Untagged / other-tag fences are inert display (ruling ⑤).
//!
//! This module is the authoring layer: it splits a document into ordered
//! [`Segment`]s, exposes the concatenated executable [`LiterateDoc::flow_source`]
//! (one `.riv.md` lowers to one `PlanGraph`, §31.2), and round-trips the
//! document so `rivus fmt` can reformat only the flow bodies while preserving
//! prose and frontmatter byte-for-byte (§31.5). std-only, zero-dependency.

use rivus_core::RivusError;

/// A parsed `.riv.md` document: optional frontmatter plus an ordered list of
/// prose / flow segments. The segment order and raw text are preserved so the
/// document round-trips (§31.5); only flow-fence *bodies* are subject to
/// reformatting by `fmt`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiterateDoc {
    /// Raw frontmatter block *between* the `---` delimiters (without them), if
    /// present. Kept verbatim for lossless round-trip; parsed lazily by
    /// [`LiterateDoc::frontmatter_pairs`] for the config cascade.
    pub frontmatter: Option<String>,
    pub segments: Vec<Segment>,
}

/// One ordered piece of a `.riv.md` document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// Inert markdown: headings, paragraphs, blank lines, and non-`flow`
    /// fenced blocks (e.g. ```` ```mermaid ````). Preserved verbatim.
    Prose(String),
    /// A ```` ```flow ```` fenced block — the executed pipeline.
    Flow(FlowCell),
}

/// A single ```` ```flow ```` fence: the executed pipeline plus its per-cell
/// `#|` options (§31.3). The opening/closing fence lines are preserved verbatim
/// so indentation and backtick count round-trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowCell {
    /// The opening fence line verbatim, e.g. `` ```flow `` (may carry leading
    /// indentation, which is non-semantic — §31.1).
    pub open: String,
    /// Raw `#|` option lines at the head of the cell (Quarto-style), verbatim.
    pub options: Vec<String>,
    /// The executable flow source (the cell body with `#|` options removed).
    pub body: String,
    /// The closing fence line verbatim, e.g. `` ``` ``.
    pub close: String,
}

impl LiterateDoc {
    /// The concatenated executable program: every ```` ```flow ```` body in
    /// document order, joined by blank lines. One `.riv.md` → one program →
    /// one `PlanGraph` (§31.2). Prose, frontmatter and `#|` options are not
    /// part of the executable text.
    pub fn flow_source(&self) -> String {
        let mut out = String::new();
        for seg in &self.segments {
            if let Segment::Flow(cell) = seg {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(cell.body.trim_end_matches('\n'));
                out.push('\n');
            }
        }
        out
    }

    /// Does the document contain at least one executable ```` ```flow ```` cell?
    pub fn has_flow(&self) -> bool {
        self.segments.iter().any(|s| matches!(s, Segment::Flow(_)))
    }

    /// Frontmatter parsed into ordered `(key, value)` pairs for the config
    /// cascade (§31.3). Lenient strict-subset YAML: top-level `key: value`
    /// lines, trailing `# …` comments stripped, blank lines ignored. The raw
    /// block (used for round-trip) is independent of this, so leniency here is
    /// safe. Nested / block YAML is out of scope for stage 1.
    pub fn frontmatter_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        let Some(fm) = &self.frontmatter else {
            return pairs;
        };
        for line in fm.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                let key = k.trim();
                if key.is_empty() {
                    continue;
                }
                pairs.push((key.to_string(), strip_inline_comment(v).trim().to_string()));
            }
        }
        pairs
    }

    /// Render the document back to `.riv.md` text. Round-trips the frontmatter,
    /// prose and fences verbatim; callers that reformat (e.g. `fmt`) mutate the
    /// flow `body` first (§31.5).
    pub fn render(&self) -> String {
        let mut out = String::new();
        if let Some(fm) = &self.frontmatter {
            out.push_str("---\n");
            out.push_str(fm);
            if !fm.ends_with('\n') {
                out.push('\n');
            }
            out.push_str("---\n");
        }
        for seg in &self.segments {
            match seg {
                Segment::Prose(text) => out.push_str(text),
                Segment::Flow(cell) => {
                    out.push_str(&cell.open);
                    out.push('\n');
                    for opt in &cell.options {
                        out.push_str(opt);
                        out.push('\n');
                    }
                    out.push_str(&cell.body);
                    if !cell.body.is_empty() && !cell.body.ends_with('\n') {
                        out.push('\n');
                    }
                    out.push_str(&cell.close);
                    out.push('\n');
                }
            }
        }
        out
    }
}

/// Strip a trailing ` # …` inline comment from a frontmatter value, ignoring
/// `#` inside `[...]` (lists) or quotes so `needs: [a#b]` / `t: "a # b"` keep
/// their text. Minimal heuristic for the stage-1 strict subset.
fn strip_inline_comment(v: &str) -> &str {
    let bytes = v.as_bytes();
    let mut depth: i32 = 0;
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                b'"' | b'\'' => quote = Some(c),
                b'[' => depth += 1,
                b']' => depth = depth.saturating_sub(1),
                b'#' if depth == 0 && (i == 0 || bytes[i - 1] == b' ' || bytes[i - 1] == b'\t') => {
                    return &v[..i];
                }
                _ => {}
            },
        }
        i += 1;
    }
    v
}

/// Is this trimmed line a code-fence delimiter? Returns the backtick run length
/// and the info string (text after the backticks) when so. Only backtick fences
/// are recognized (strict subset, §31.1); `~~~` is treated as prose.
fn fence_marker(trimmed: &str) -> Option<(usize, &str)> {
    let ticks = trimmed.chars().take_while(|&c| c == '`').count();
    if ticks < 3 {
        return None;
    }
    let info = trimmed[ticks..].trim();
    // An info string containing a backtick is not a valid fence (CommonMark).
    if info.contains('`') {
        return None;
    }
    Some((ticks, info))
}

/// Parse `.riv.md` source into a [`LiterateDoc`]. Recognizes a leading `---`
/// frontmatter block and ```` ```flow ```` fences; all other text is inert
/// prose (§31.1). Errors (never-silent) on an unterminated frontmatter or
/// flow fence.
pub fn parse_literate(src: &str) -> Result<LiterateDoc, RivusError> {
    // Tolerate a leading UTF-8 BOM like the flow lexer does.
    let src = src.strip_prefix('\u{feff}').unwrap_or(src);
    let lines: Vec<&str> = src.split('\n').collect();
    let mut idx = 0;

    // --- Frontmatter: only when the very first line is exactly `---`. ---
    let mut frontmatter = None;
    if lines.first().map(|l| l.trim_end()) == Some("---") {
        let mut end = None;
        for (j, line) in lines.iter().enumerate().skip(1) {
            let t = line.trim_end();
            if t == "---" || t == "..." {
                end = Some(j);
                break;
            }
        }
        match end {
            Some(j) => {
                frontmatter = Some(lines[1..j].join("\n"));
                idx = j + 1;
            }
            None => {
                return Err(RivusError::Parse(
                    "unterminated frontmatter: opened with `---` but never closed (expected a \
                     closing `---` on its own line)"
                        .to_string(),
                ));
            }
        }
    }

    // --- Body: prose, with ```flow fences carved out. ---
    let mut segments: Vec<Segment> = Vec::new();
    let mut prose = String::new();
    let flush_prose = |prose: &mut String, segments: &mut Vec<Segment>| {
        if !prose.is_empty() {
            segments.push(Segment::Prose(std::mem::take(prose)));
        }
    };

    while idx < lines.len() {
        let line = lines[idx];
        // The final element of `split('\n')` on a trailing newline is "" — don't
        // emit a phantom blank line for it.
        if idx == lines.len() - 1 && line.is_empty() {
            break;
        }
        if let Some((ticks, info)) = fence_marker(line.trim()) {
            let tag = info.split_whitespace().next().unwrap_or("");
            if tag == "flow" {
                // Executable cell: scan to the closing fence.
                let open = line.to_string();
                let mut body_lines: Vec<&str> = Vec::new();
                let mut close = None;
                let mut k = idx + 1;
                while k < lines.len() {
                    let t = lines[k].trim();
                    if let Some((cticks, cinfo)) = fence_marker(t) {
                        if cticks >= ticks && cinfo.is_empty() {
                            close = Some(lines[k].to_string());
                            break;
                        }
                    }
                    body_lines.push(lines[k]);
                    k += 1;
                }
                let Some(close) = close else {
                    return Err(RivusError::Parse(format!(
                        "unterminated ```flow fence opened at line {} (expected a closing ``` on \
                         its own line)",
                        idx + 1
                    )));
                };
                flush_prose(&mut prose, &mut segments);
                // Split leading `#|` option lines (Quarto-style cell options).
                let mut options = Vec::new();
                let mut bi = 0;
                while bi < body_lines.len() && body_lines[bi].trim_start().starts_with("#|") {
                    options.push(body_lines[bi].to_string());
                    bi += 1;
                }
                let body = body_lines[bi..].join("\n");
                segments.push(Segment::Flow(FlowCell {
                    open,
                    options,
                    body,
                    close,
                }));
                idx = k + 1;
                continue;
            } else {
                // Inert fence (```mermaid, ```rust, untagged display, …): copy
                // the whole block — including its delimiters — into prose so it
                // round-trips and is never executed (ruling ⑤).
                prose.push_str(line);
                prose.push('\n');
                let mut k = idx + 1;
                while k < lines.len() {
                    let t = lines[k].trim();
                    let closed = fence_marker(t)
                        .is_some_and(|(cticks, cinfo)| cticks >= ticks && cinfo.is_empty());
                    prose.push_str(lines[k]);
                    prose.push('\n');
                    if closed {
                        k += 1;
                        break;
                    }
                    k += 1;
                }
                idx = k;
                continue;
            }
        }
        prose.push_str(line);
        prose.push('\n');
        idx += 1;
    }
    flush_prose(&mut prose, &mut segments);

    Ok(LiterateDoc {
        frontmatter,
        segments,
    })
}

/// Wrap a bare `.riv` flow program as a minimal `.riv.md` document: the whole
/// program in a single ```` ```flow ```` fence, no prose, no frontmatter
/// (§31.5 pairing, `.riv` → `.riv.md`).
pub fn wrap_flow(flow_src: &str) -> LiterateDoc {
    LiterateDoc {
        frontmatter: None,
        segments: vec![Segment::Flow(FlowCell {
            open: "```flow".to_string(),
            options: Vec::new(),
            body: flow_src.trim_end_matches('\n').to_string(),
            close: "```".to_string(),
        })],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontmatter_and_flow_split() {
        let src = "---\ntitle: demo\nchunk_size: 65536  # (R)\n---\n\n# Heading\n\nprose\n\n\
                   ```flow\nU: open users.csv |? age >= 20 ;\n```\n";
        let doc = parse_literate(src).unwrap();
        assert_eq!(
            doc.frontmatter.as_deref(),
            Some("title: demo\nchunk_size: 65536  # (R)")
        );
        let pairs = doc.frontmatter_pairs();
        assert_eq!(pairs[0], ("title".into(), "demo".into()));
        assert_eq!(pairs[1], ("chunk_size".into(), "65536".into()));
        assert!(doc.has_flow());
        assert_eq!(doc.flow_source(), "U: open users.csv |? age >= 20 ;\n");
    }

    #[test]
    fn multiple_flow_cells_concatenate() {
        let src = "```flow\nA: open a.csv ;\n```\n\ntext\n\n```flow\nB: open b.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        assert_eq!(doc.flow_source(), "A: open a.csv ;\n\nB: open b.csv ;\n");
    }

    #[test]
    fn cell_options_are_split_from_body() {
        let src = "```flow\n#| name: daily\n#| cache: true\nopen x.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        let Segment::Flow(cell) = &doc.segments[0] else {
            panic!("expected flow cell");
        };
        assert_eq!(cell.options, vec!["#| name: daily", "#| cache: true"]);
        assert_eq!(cell.body, "open x.csv ;");
        assert_eq!(doc.flow_source(), "open x.csv ;\n");
    }

    #[test]
    fn non_flow_fence_is_inert_prose() {
        let src = "```mermaid\ngraph TD\n```\n\n```flow\nopen x.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        // The mermaid block is prose, not executed.
        assert_eq!(doc.flow_source(), "open x.csv ;\n");
        assert!(matches!(doc.segments[0], Segment::Prose(_)));
    }

    #[test]
    fn untagged_fence_is_not_executed() {
        let src = "```\nopen NOT_RUN.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        assert!(!doc.has_flow());
        assert_eq!(doc.flow_source(), "");
    }

    #[test]
    fn render_round_trips_verbatim() {
        let src = "---\ntitle: demo\n---\n\n# H\n\nprose line\n\n```flow\nopen x.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        assert_eq!(doc.render(), src);
    }

    #[test]
    fn render_round_trips_cell_options_and_inert_fence() {
        let src = "```mermaid\ngraph\n```\n\n```flow\n#| name: c\nopen x.csv ;\n```\n";
        let doc = parse_literate(src).unwrap();
        assert_eq!(doc.render(), src);
    }

    #[test]
    fn unterminated_frontmatter_errors() {
        let err = parse_literate("---\ntitle: demo\n").unwrap_err();
        assert!(matches!(err, RivusError::Parse(_)));
    }

    #[test]
    fn unterminated_flow_fence_errors() {
        let err = parse_literate("```flow\nopen x.csv ;\n").unwrap_err();
        assert!(matches!(err, RivusError::Parse(_)));
    }

    #[test]
    fn wrap_flow_pairs_back() {
        let doc = wrap_flow("U: open users.csv ;\n");
        assert_eq!(doc.flow_source(), "U: open users.csv ;\n");
        assert_eq!(doc.render(), "```flow\nU: open users.csv ;\n```\n");
    }

    #[test]
    fn no_frontmatter_plain_prose() {
        let src = "just prose\nno fences\n";
        let doc = parse_literate(src).unwrap();
        assert!(doc.frontmatter.is_none());
        assert!(!doc.has_flow());
        assert_eq!(doc.render(), src);
    }

    #[test]
    fn strip_inline_comment_respects_brackets_and_quotes() {
        assert_eq!(strip_inline_comment("65536  # hint"), "65536  ");
        assert_eq!(
            strip_inline_comment("[read:a/*.csv]  # c"),
            "[read:a/*.csv]  "
        );
        assert_eq!(strip_inline_comment("\"a # b\""), "\"a # b\"");
        assert_eq!(strip_inline_comment("plain"), "plain");
    }
}
