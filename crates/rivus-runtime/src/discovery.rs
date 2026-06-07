//! Discovery (design §28.3): enumerate a glob pattern into a deterministic list
//! of resource paths. **std-only** — no third-party glob crate (zero-dep);
//! `**` recursion and the `*` / `?` / `[…]` segment matcher are implemented here.
//!
//! Symlinks are not followed (loop-safe). Results are sorted by uri (byte
//! ascending) so the discovered stream is deterministic (§28.3) and chunk-size
//! independent downstream.

use std::path::Path;

/// Enumerate files matching `pattern` into their path strings, sorted byte
/// ascending and de-duplicated. A non-matching / empty result is an empty vec
/// (the caller surfaces a "0 matches" warning — continue-first).
///
/// Glob vocabulary (shared with `like`/`glob` predicates): `*` matches any run
/// within a path segment, `?` one char, `[…]` a char class (`[a-z]`, `[!…]`
/// negation), and `**` matches zero or more whole path segments (recursion).
pub(crate) fn glob_paths(pattern: &str) -> Vec<String> {
    let segs: Vec<&str> = pattern.split('/').collect();
    let mut out = Vec::new();
    if pattern.starts_with('/') {
        // Absolute: the leading split element is "" — start at the fs root.
        walk(Path::new("/"), "/", &segs[1..], &mut out);
    } else {
        walk(Path::new("."), "", &segs, &mut out);
    }
    out.sort();
    out.dedup();
    out
}

/// Join a uri base and a path segment without doubling separators.
fn join_uri(base: &str, seg: &str) -> String {
    if base.is_empty() {
        seg.to_string()
    } else if base.ends_with('/') {
        format!("{base}{seg}")
    } else {
        format!("{base}/{seg}")
    }
}

/// Recursively match `segs` against the filesystem, tracking the on-disk path
/// (`fs`) and the clean uri (`uri`, the form the user wrote). Matched **files**
/// (not directories) are pushed to `out`.
fn walk(fs: &Path, uri: &str, segs: &[&str], out: &mut Vec<String>) {
    let Some((seg, rest)) = segs.split_first() else {
        // All segments consumed: this path is a match iff it's a real file.
        if fs.is_file() {
            out.push(uri.to_string());
        }
        return;
    };
    // The directory to enumerate (the cwd when we're still at the relative root).
    let read_root = if uri.is_empty() { Path::new(".") } else { fs };

    if *seg == "**" {
        // `**` matches zero segments (try `rest` here) …
        walk(fs, uri, rest, out);
        // … or one-or-more: recurse into each subdir with `**` still pending.
        if let Ok(rd) = std::fs::read_dir(read_root) {
            for e in rd.flatten() {
                let child = e.path();
                // `symlink_metadata` + is_dir avoids following symlinked dirs.
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let name = e.file_name().to_string_lossy().into_owned();
                    walk(&child, &join_uri(uri, &name), segs, out);
                }
            }
        }
    } else if seg.contains(['*', '?', '[']) {
        if let Ok(rd) = std::fs::read_dir(read_root) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().into_owned();
                if glob_match(seg, &name) {
                    walk(&e.path(), &join_uri(uri, &name), rest, out);
                }
            }
        }
    } else {
        // Literal segment: descend only if it exists (no directory scan).
        let child = read_root.join(seg);
        if child.exists() {
            walk(&child, &join_uri(uri, seg), rest, out);
        }
    }
}

/// Match a single path segment against one glob segment (`*` / `?` / `[…]`).
fn glob_match(pat: &str, name: &str) -> bool {
    let p: Vec<char> = pat.chars().collect();
    let n: Vec<char> = name.chars().collect();
    glob_rec(&p, &n)
}

fn glob_rec(p: &[char], n: &[char]) -> bool {
    match p.split_first() {
        None => n.is_empty(),
        // `*` matches zero chars (consume the star) or one more (keep the star).
        Some((&'*', rest)) => glob_rec(rest, n) || (!n.is_empty() && glob_rec(p, &n[1..])),
        Some((&'?', rest)) => !n.is_empty() && glob_rec(rest, &n[1..]),
        Some((&'[', _)) => match find_class_end(p) {
            Some(close) => {
                !n.is_empty()
                    && class_match(&p[1..close], n[0])
                    && glob_rec(&p[close + 1..], &n[1..])
            }
            // No closing `]` → treat `[` as a literal.
            None => !n.is_empty() && n[0] == '[' && glob_rec(&p[1..], &n[1..]),
        },
        Some((&c, rest)) => !n.is_empty() && n[0] == c && glob_rec(rest, &n[1..]),
    }
}

/// Index of the `]` closing a `[…]` class (a `]` right after `[` / `[!` is a
/// literal member, per the usual glob rule). `None` if there is no close.
fn find_class_end(p: &[char]) -> Option<usize> {
    let mut i = 1;
    if i < p.len() && p[i] == '!' {
        i += 1;
    }
    if i < p.len() && p[i] == ']' {
        i += 1; // a leading ']' is a literal member, not the close
    }
    while i < p.len() {
        if p[i] == ']' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Does char `c` match the class body (between `[` and `]`)? Leading `!` negates;
/// `a-z` is a range; everything else is a literal member.
fn class_match(class: &[char], c: char) -> bool {
    let (neg, body) = match class.first() {
        Some('!') => (true, &class[1..]),
        _ => (false, class),
    };
    let mut matched = false;
    let mut i = 0;
    while i < body.len() {
        if i + 2 < body.len() && body[i + 1] == '-' {
            if body[i] <= c && c <= body[i + 2] {
                matched = true;
            }
            i += 3;
        } else {
            if body[i] == c {
                matched = true;
            }
            i += 1;
        }
    }
    matched != neg
}

#[cfg(test)]
mod tests {
    use super::glob_match;

    #[test]
    fn star_question_and_literal() {
        assert!(glob_match("*.csv", "a.csv"));
        assert!(glob_match("*.csv", ".csv"));
        assert!(!glob_match("*.csv", "a.tsv"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("data", "data"));
        assert!(!glob_match("data", "data2"));
        assert!(glob_match("log*2026*", "log_app_2026_01"));
    }

    #[test]
    fn char_classes() {
        assert!(glob_match("[abc].txt", "a.txt"));
        assert!(!glob_match("[abc].txt", "d.txt"));
        assert!(glob_match("file[0-9].csv", "file7.csv"));
        assert!(!glob_match("file[0-9].csv", "fileX.csv"));
        assert!(glob_match("[!0-9]*", "abc")); // negation: not a digit first
        assert!(!glob_match("[!0-9]*", "1bc"));
    }

    #[test]
    fn star_matches_empty_and_greedy() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a*b*c", "axxbyyc"));
        assert!(!glob_match("a*b*c", "axxbyy"));
    }
}
