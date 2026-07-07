//! Apache Parquet reader (SUPPLY-CHAIN selected adapter, read-only slice).
//!
//! Compiled only with the off-by-default `parquet` feature; a feature-less
//! build refuses a Parquet plan **pre-run** (`PlanGraph::uses_parquet`,
//! never-silent — the same shape as `regex`/`gzip`). The adapter stays behind
//! the source-operator boundary: the engine never references the crate.
//!
//! Slice scope (documented in SUPPLY-CHAIN.md / the PR):
//! - **Row-group streaming**: one row group is decoded at a time and emitted
//!   as `chunk_size` chunks — peak memory is bounded by the file's row-group
//!   size, not the file size.
//! - **Flat schemas** on the typed lanes: BOOLEAN→bool, INT32/INT64→i64
//!   (DATE→date, TIMESTAMP millis/micros→datetime, DECIMAL→decimal),
//!   FLOAT/DOUBLE→f64, BYTE_ARRAY UTF8→str, DECIMAL byte arrays→decimal.
//!   A **nested** column (group/list/map) is a Fatal open error naming the
//!   column (never-silent; nested lanes are a later slice on §32 s3).
//! - Serial only: `plan_parallel_source` returns `None` for Parquet (the
//!   byte-range split does not apply to a columnar container); downstream
//!   transforms still parallelize on the chunk-partition path.

use super::operators::{OpCtx, Operator};
use rivus_core::{
    Chunk, Column, ColumnData, DecColumn, DtColumn, ErrorEvent, ErrorScope, Field as SchemaField,
    Schema, Severity, StrColumn, TimeUnit, Validity,
};
use rivus_ir::NodeId;
use std::sync::Arc;

use parquet::basic::{ConvertedType, LogicalType, TimeUnit as PqTimeUnit, Type as PhysicalType};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field as PqField;
use parquet::schema::types::Type as PqType;

/// The Rivus lane one Parquet leaf column decodes to.
#[derive(Clone, Copy, PartialEq)]
enum Lane {
    Bool,
    I64,
    F64,
    Str,
    Date,
    /// Epoch ticks at the given unit (TIMESTAMP millis/micros/nanos).
    DateTime(TimeUnit),
    /// Scaled integer (DECIMAL(p, s) with p ≤ 38).
    Dec(u8),
}

/// `open x.parquet` — a pull source decoding one row group at a time.
pub(crate) struct SourceParquet {
    path: String,
    chunk_size: usize,
    reader: Option<SerializedFileReader<std::fs::File>>,
    schema: Arc<Schema>,
    lanes: Vec<Lane>,
    /// Current row group (decoded columns + cursor), sliced into chunks.
    group: Vec<Column>,
    group_len: usize,
    cursor: usize,
    next_group: usize,
    opened: bool,
    failed: bool,
    /// Non-UTF8 BYTE_ARRAY cells rendered lossily — warn once (continue-first).
    lossy_warned: bool,
    source: Option<rivus_core::Resource>,
    filename: bool,
}

impl SourceParquet {
    pub(crate) fn new(path: String, chunk_size: usize) -> Self {
        SourceParquet {
            path,
            chunk_size: chunk_size.max(1),
            reader: None,
            schema: Schema::empty(),
            lanes: Vec::new(),
            group: Vec::new(),
            group_len: 0,
            cursor: 0,
            next_group: 0,
            opened: false,
            failed: false,
            lossy_warned: false,
            source: None,
            filename: false,
        }
    }

    pub(crate) fn with_provenance(mut self, prov: rivus_ir::Provenance, path: &str) -> Self {
        self.source = prov.source(path);
        self.filename = prov.materializes_filename();
        self
    }

    fn fatal(&mut self, ctx: &mut OpCtx, msg: String) {
        self.failed = true;
        ctx.raise(
            ErrorEvent::new(Severity::Fatal, ErrorScope::Graph, msg).at_node(ctx.label.clone()),
        );
    }

    /// Open the file, validate the schema is flat, and derive the lanes.
    fn open(&mut self, ctx: &mut OpCtx) {
        self.opened = true;
        let file = match std::fs::File::open(&self.path) {
            Ok(f) => f,
            Err(e) => return self.fatal(ctx, format!("cannot open '{}': {e}", self.path)),
        };
        let reader = match SerializedFileReader::new(file) {
            Ok(r) => r,
            Err(e) => return self.fatal(ctx, format!("cannot read parquet '{}': {e}", self.path)),
        };
        let root = reader.metadata().file_metadata().schema();
        let mut fields: Vec<SchemaField> = Vec::new();
        let mut lanes: Vec<Lane> = Vec::new();
        for f in root.get_fields() {
            let (name, lane) = match self.classify(f) {
                Ok(x) => x,
                Err(msg) => return self.fatal(ctx, msg),
            };
            let dtype = match lane {
                Lane::Bool => rivus_core::DataType::Bool,
                Lane::I64 => rivus_core::DataType::I64,
                Lane::F64 => rivus_core::DataType::F64,
                Lane::Str => rivus_core::DataType::Str,
                Lane::Date => rivus_core::DataType::Date,
                Lane::DateTime(unit) => rivus_core::DataType::DateTime { unit },
                Lane::Dec(scale) => rivus_core::DataType::Decimal { scale },
            };
            fields.push(SchemaField::new(name, dtype));
            lanes.push(lane);
        }
        self.schema = Arc::new(Schema::new(fields));
        self.lanes = lanes;
        self.reader = Some(reader);
    }

    /// The lane for one root field, or a teach-the-limit error for shapes this
    /// slice does not decode (nested, INT96, unannotated fixed binaries).
    fn classify(&self, f: &Arc<PqType>) -> Result<(String, Lane), String> {
        let name = f.name().to_string();
        let PqType::PrimitiveType {
            basic_info,
            physical_type,
            scale,
            ..
        } = f.as_ref()
        else {
            return Err(format!(
                "parquet '{}': column '{name}' is nested (group/list/map) — nested lanes \
                 are a later slice; select flat columns when writing, or convert upstream",
                self.path
            ));
        };
        let logical = basic_info.logical_type_ref();
        let converted = basic_info.converted_type();
        let lane = match physical_type {
            PhysicalType::BOOLEAN => Lane::Bool,
            PhysicalType::INT32 => {
                if converted == ConvertedType::DATE {
                    Lane::Date
                } else if converted == ConvertedType::DECIMAL {
                    Lane::Dec(u8::try_from(*scale).unwrap_or(0))
                } else {
                    Lane::I64
                }
            }
            PhysicalType::INT64 => match logical {
                Some(LogicalType::Timestamp(t)) => Lane::DateTime(match t.unit {
                    PqTimeUnit::MILLIS => TimeUnit::Milli,
                    PqTimeUnit::MICROS => TimeUnit::Micro,
                    PqTimeUnit::NANOS => TimeUnit::Nano,
                }),
                _ if converted == ConvertedType::TIMESTAMP_MILLIS => {
                    Lane::DateTime(TimeUnit::Milli)
                }
                _ if converted == ConvertedType::TIMESTAMP_MICROS => {
                    Lane::DateTime(TimeUnit::Micro)
                }
                _ if converted == ConvertedType::DECIMAL => {
                    Lane::Dec(u8::try_from(*scale).unwrap_or(0))
                }
                _ => Lane::I64,
            },
            PhysicalType::FLOAT | PhysicalType::DOUBLE => Lane::F64,
            PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                if converted == ConvertedType::DECIMAL {
                    Lane::Dec(u8::try_from(*scale).unwrap_or(0))
                } else {
                    // UTF8/ENUM/JSON and unannotated binaries all ride the text
                    // lane; a non-UTF8 cell renders lossily with a one-time warn.
                    Lane::Str
                }
            }
            PhysicalType::INT96 => {
                return Err(format!(
                    "parquet '{}': column '{name}' uses the deprecated INT96 timestamp — \
                     rewrite the file with TIMESTAMP_MILLIS/MICROS (int64)",
                    self.path
                ));
            }
        };
        Ok((name, lane))
    }

    /// Decode the next row group into typed columns; false when exhausted.
    fn load_group(&mut self, ctx: &mut OpCtx) -> bool {
        let lanes = self.lanes.clone();
        let path = self.path.clone();
        let ncols = lanes.len();
        let mut i64s: Vec<Vec<i64>> = vec![Vec::new(); ncols];
        let mut f64s: Vec<Vec<f64>> = vec![Vec::new(); ncols];
        let mut bools: Vec<Vec<bool>> = vec![Vec::new(); ncols];
        let mut strs: Vec<StrColumn> = (0..ncols).map(|_| StrColumn::default()).collect();
        let mut i32s: Vec<Vec<i32>> = vec![Vec::new(); ncols];
        let mut i128s: Vec<Vec<i128>> = vec![Vec::new(); ncols];
        let mut bits: Vec<Vec<bool>> = vec![Vec::new(); ncols];
        let mut nrows = 0usize;
        let mut lossy = false;
        // The row-group borrow of `reader` stays inside this block; any error
        // is deferred so `self.fatal` never overlaps it.
        let err: Option<String> = 'decode: {
            let Some(reader) = self.reader.as_ref() else {
                return false;
            };
            if self.next_group >= reader.metadata().num_row_groups() {
                return false;
            }
            let rg = match reader.get_row_group(self.next_group) {
                Ok(r) => r,
                Err(e) => {
                    break 'decode Some(format!(
                        "parquet '{path}': cannot read row group {}: {e}",
                        self.next_group
                    ))
                }
            };
            self.next_group += 1;
            let rows = match rg.get_row_iter(None) {
                Ok(it) => it,
                Err(e) => break 'decode Some(format!("parquet '{path}': row decode failed: {e}")),
            };
            for row in rows {
                let row = match row {
                    Ok(r) => r,
                    Err(e) => {
                        break 'decode Some(format!("parquet '{path}': row decode failed: {e}"))
                    }
                };
                nrows += 1;
                for (ci, (_, field)) in row.get_column_iter().enumerate() {
                    if ci >= ncols {
                        break;
                    }
                    let lane = lanes[ci];
                    let mut valid = true;
                    match (lane, field) {
                        (_, PqField::Null) => valid = false,
                        (Lane::Bool, PqField::Bool(b)) => bools[ci].push(*b),
                        (Lane::I64, f) => match i64_of(f) {
                            Some(v) => i64s[ci].push(v),
                            None => valid = false,
                        },
                        (Lane::F64, PqField::Float(v)) => f64s[ci].push(*v as f64),
                        (Lane::F64, PqField::Double(v)) => f64s[ci].push(*v),
                        (Lane::Str, PqField::Str(sv)) => strs[ci].push(sv),
                        (Lane::Str, PqField::Bytes(b)) => {
                            lossy = true;
                            strs[ci].push(&String::from_utf8_lossy(b.data()));
                        }
                        (Lane::Date, PqField::Date(d)) => i32s[ci].push(*d),
                        (Lane::DateTime(_), PqField::TimestampMillis(t))
                        | (Lane::DateTime(_), PqField::TimestampMicros(t)) => i64s[ci].push(*t),
                        (Lane::Dec(_), PqField::Decimal(d)) => match dec_i128(d) {
                            Some(v) => i128s[ci].push(v),
                            None => valid = false,
                        },
                        // A cell whose runtime shape contradicts the schema-derived
                        // lane → null (continue-first; the schema is authoritative).
                        _ => valid = false,
                    }
                    if !valid {
                        // Keep the backing store aligned with a lane-default cell.
                        match lane {
                            Lane::Bool => bools[ci].push(false),
                            Lane::I64 => i64s[ci].push(0),
                            Lane::F64 => f64s[ci].push(0.0),
                            Lane::Str => strs[ci].push(""),
                            Lane::Date => i32s[ci].push(0),
                            Lane::DateTime(_) => i64s[ci].push(0),
                            Lane::Dec(_) => i128s[ci].push(0),
                        }
                    }
                    bits[ci].push(valid);
                }
            }
            None
        };
        if let Some(msg) = err {
            self.fatal(ctx, msg);
            return false;
        }
        if lossy && !self.lossy_warned {
            self.lossy_warned = true;
            ctx.raise(
                ErrorEvent::new(
                    Severity::Warn,
                    ErrorScope::Chunk,
                    format!("parquet '{path}': binary (non-UTF8) cells rendered lossily as text"),
                )
                .at_node(ctx.label.clone()),
            );
        }
        let mut cols: Vec<Column> = Vec::with_capacity(ncols);
        for ci in 0..ncols {
            let validity = Validity::from_bits(&bits[ci]);
            let data = match lanes[ci] {
                Lane::Bool => ColumnData::Bool(std::mem::take(&mut bools[ci])),
                Lane::I64 => ColumnData::I64(std::mem::take(&mut i64s[ci])),
                Lane::F64 => ColumnData::F64(std::mem::take(&mut f64s[ci])),
                Lane::Str => ColumnData::Str(std::mem::take(&mut strs[ci])),
                Lane::Date => ColumnData::Date(std::mem::take(&mut i32s[ci])),
                Lane::DateTime(unit) => ColumnData::DateTime(DtColumn {
                    ticks: std::mem::take(&mut i64s[ci]),
                    unit,
                }),
                Lane::Dec(scale) => ColumnData::Dec(DecColumn {
                    unscaled: std::mem::take(&mut i128s[ci]),
                    scale,
                }),
            };
            cols.push(Column::new(data, validity));
        }
        self.group = cols;
        self.group_len = nrows;
        self.cursor = 0;
        true
    }
}

/// Integer-family record field → i64 (unsigned 64-bit that overflows → None).
fn i64_of(f: &PqField) -> Option<i64> {
    match f {
        PqField::Byte(v) => Some(*v as i64),
        PqField::Short(v) => Some(*v as i64),
        PqField::Int(v) => Some(*v as i64),
        PqField::Long(v) => Some(*v),
        PqField::UByte(v) => Some(*v as i64),
        PqField::UShort(v) => Some(*v as i64),
        PqField::UInt(v) => Some(*v as i64),
        PqField::ULong(v) => i64::try_from(*v).ok(),
        _ => None,
    }
}

/// DECIMAL bytes (two's-complement big-endian, p ≤ 38) → unscaled i128.
fn dec_i128(d: &parquet::data_type::Decimal) -> Option<i128> {
    let bytes = d.data();
    if bytes.is_empty() || bytes.len() > 16 {
        return None;
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut buf = [if negative { 0xFFu8 } else { 0 }; 16];
    buf[16 - bytes.len()..].copy_from_slice(bytes);
    Some(i128::from_be_bytes(buf))
}

impl Operator for SourceParquet {
    fn is_source(&self) -> bool {
        true
    }

    fn pull(&mut self, ctx: &mut OpCtx) -> Option<Chunk> {
        if !self.opened {
            self.open(ctx);
        }
        if self.failed {
            return None;
        }
        loop {
            if self.cursor < self.group_len {
                let end = (self.cursor + self.chunk_size).min(self.group_len);
                let idx: Vec<usize> = (self.cursor..end).collect();
                self.cursor = end;
                let columns: Vec<Column> = self.group.iter().map(|c| c.gather(&idx)).collect();
                let id = ctx.fresh_id();
                return Some(super::operators::attach_provenance(
                    Chunk::new(id, self.schema.clone(), columns),
                    &self.source,
                    self.filename,
                ));
            }
            if !self.load_group(ctx) || self.failed {
                return None;
            }
        }
    }

    fn process(&mut self, _from: NodeId, _chunk: Chunk, _ctx: &mut OpCtx) -> Vec<Chunk> {
        Vec::new()
    }
}
