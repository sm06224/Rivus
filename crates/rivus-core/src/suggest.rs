//! "Did you mean …?" suggestions — a tiny, dependency-free edit-distance
//! helper shared by every guided error (unknown column / function / scope,
//! #191/#192). Errors that *teach* are the literate-programming contract:
//! never just "unknown", always the nearest valid spelling when one is close.

/// Damerau-Levenshtein (optimal string alignment) distance with an early-exit
/// `cap` (distances above `cap` all report `cap + 1`, which is enough for
/// thresholding). Adjacent transposition counts as ONE edit — `aeg`→`age` is
/// the most common real typo shape. O(len_a × len_b).
fn osa_distance_capped(a: &str, b: &str, cap: usize) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > cap {
        return cap + 1;
    }
    let w = b.len() + 1;
    // Three rolling rows: i-2, i-1, i (transposition looks two rows back).
    let mut prev2: Vec<usize> = vec![0; w];
    let mut prev: Vec<usize> = (0..w).collect();
    let mut cur = vec![0usize; w];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i + 1;
        let mut row_min = cur[0];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            let mut d = (prev[j + 1] + 1).min(cur[j] + 1).min(prev[j] + cost);
            if i > 0 && j > 0 && *ca == b[j - 1] && a[i - 1] == *cb {
                d = d.min(prev2[j - 1] + 1); // adjacent transposition
            }
            cur[j + 1] = d;
            row_min = row_min.min(d);
        }
        if row_min > cap {
            return cap + 1;
        }
        std::mem::swap(&mut prev2, &mut prev);
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// The candidate closest to `unknown` within a small edit distance, for a
/// "did you mean '…'?" hint. The threshold scales gently with length (1 edit
/// for short names, up to 2 for longer ones) so `aeg`→`age` and
/// `contry`→`country` both hit while unrelated names stay silent. Case is
/// compared case-insensitively (a typo often includes a case slip); the
/// returned suggestion keeps the candidate's spelling. Ties break to the
/// first candidate in iteration order (deterministic).
pub fn suggest_similar<'a, I>(unknown: &str, candidates: I) -> Option<&'a str>
where
    I: IntoIterator<Item = &'a str>,
{
    let cap = if unknown.chars().count() <= 4 { 1 } else { 2 };
    let low = unknown.to_lowercase();
    let mut best: Option<(&'a str, usize)> = None;
    for c in candidates {
        if c == unknown {
            continue; // an exact match is not a suggestion problem
        }
        let d = osa_distance_capped(&low, &c.to_lowercase(), cap);
        if d <= cap && best.is_none_or(|(_, bd)| d < bd) {
            best = Some((c, d));
        }
    }
    best.map(|(c, _)| c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suggests_close_names_and_stays_silent_on_far_ones() {
        let cols = ["id", "name", "age", "score", "country", "active"];
        assert_eq!(suggest_similar("aeg", cols), Some("age"));
        assert_eq!(suggest_similar("nmae", cols), Some("name"));
        assert_eq!(suggest_similar("contry", cols), Some("country"));
        assert_eq!(suggest_similar("Score", cols), Some("score")); // case slip
        assert_eq!(suggest_similar("zzz", cols), None); // nothing close
        assert_eq!(suggest_similar("q", cols), None);
    }

    #[test]
    fn short_names_use_the_tight_threshold() {
        // 1 edit max for ≤4 chars: "agee"→age ok, "aggge" (2 edits) not.
        assert_eq!(suggest_similar("agee", ["age"]), Some("age"));
        assert_eq!(suggest_similar("agge", ["age"]), Some("age"));
    }
}
