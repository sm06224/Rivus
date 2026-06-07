//! Codec layer (design §28.5): bytes ⇄ chunks of typed columns, format-independent.
//!
//! **§28.10 slice 1a steps ②③ — pure move-only.** Formalizes the chunk-pull
//! (decode) boundary the source operators already drive, so csv / jsonl / binary
//! present one uniform Codec face — a future format (parquet, …) implements
//! [`Decoder`] and slots in behind the source operator without a new `Source*`
//! variant, and provenance (slice 2) can wrap any decoder's chunks.
//!
//! Move-only: the §06 two-pass inference is unchanged — pass 1 (infer the global
//! schema) still runs at each format's `open` / `plan_parallel`; this trait is
//! the pass-2 streaming decode plus the diagnostics the source surfaces. Every
//! method delegates to the existing per-format reader, so behavior and
//! byte-identity are exactly preserved (fixed by the stress suite). Dispatch is
//! per *chunk*, never per row, so the tens-of-millions-of-lines parse path stays
//! monomorphic inside the reader.

use rivus_core::{Column, DataType};

/// A streaming decoder: pull one chunk of decoded columns at a time, plus the
/// per-format diagnostics the source operator reports. The three readers
/// (`CsvChunker`, `CompressedCsvReader`, `JsonlChunker`, `BinChunker`) implement
/// it by delegating to their inherent methods/fields; formats without a given
/// diagnostic use the default.
pub(crate) trait Decoder {
    /// The next batch of typed columns, or `None` at end of stream / byte range.
    /// (Pass 2 of the two-pass readers; the whole decode for binary.)
    fn decode_chunk(&mut self) -> Option<Vec<Column>>;

    /// Per-column inference outcome `(name, type, widened)` for A4 telemetry;
    /// empty for declared / sample-inferred schemas and formats that don't infer
    /// column-by-column.
    fn inferred(&self) -> &[(String, DataType, bool)] {
        &[]
    }

    /// Rows the pushed-down prefilter skipped *building* (definitely-out rows) —
    /// pure accounting, surfaced once on exhaustion (the result is unchanged).
    fn rows_prefiltered(&self) -> u64 {
        0
    }

    /// Per-output-column count of non-empty cells that failed to parse into the
    /// column's lane and were set to null, surfaced once on exhaustion. Aligned
    /// to the output schema.
    fn parse_failures(&self) -> &[u64] {
        &[]
    }
}
