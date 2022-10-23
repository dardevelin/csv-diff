pub trait CsvParseResult<P, T> {
    fn new(payload_inner: T) -> Self;
    fn into_payload(self) -> P;
}

pub struct CsvParseResultLeft<R> {
    pub(crate) csv_left_right_parse_result: CsvLeftRightParseResult<R>,
}

pub struct CsvParseResultRight<R> {
    pub(crate) csv_left_right_parse_result: CsvLeftRightParseResult<R>,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CsvLeftRightParseResult<R> {
    Left(R),
    Right(R),
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) struct RecordHash {
    pub(crate) key: u128,
    pub(crate) record_hash: u128,
}

impl RecordHash {
    #[inline]
    pub(crate) fn new(key: u128, record_hash: u128) -> Self {
        Self { key, record_hash }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct RecordHashWithPosition {
    pub(crate) record_hash: RecordHash,
    pub(crate) pos: Position,
}

impl RecordHashWithPosition {
    #[inline]
    pub(crate) fn new(key: u128, record_hash: u128, pos: Position) -> Self {
        Self {
            record_hash: RecordHash::new(key, record_hash),
            pos,
        }
    }
    #[inline]
    pub(crate) fn key(&self) -> u128 {
        self.record_hash.key
    }
    #[inline]
    pub(crate) fn record_hash_num(&self) -> u128 {
        self.record_hash.record_hash
    }
}

pub struct CsvByteRecordWithHash {
    pub(crate) byte_record: csv::Result<csv::ByteRecord>,
    pub(crate) record_hash: RecordHash,
}

impl CsvByteRecordWithHash {
    #[inline]
    pub(crate) fn new(byte_record: csv::Result<csv::ByteRecord>, record_hash: RecordHash) -> Self {
        Self {
            byte_record,
            record_hash,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) struct Position {
    pub byte_offset: u64,
    pub line: u64,
}

impl Position {
    #[inline]
    pub fn new(byte_offset: u64, line: u64) -> Self {
        Self { byte_offset, line }
    }
}

#[allow(clippy::from_over_into)]
impl Into<csv::Position> for Position {
    #[inline]
    fn into(self) -> csv::Position {
        let mut csv_pos = csv::Position::new();
        std::mem::replace(
            &mut csv_pos
                .set_byte(self.byte_offset)
                .set_line(self.line)
                .set_record(self.line - 1),
            csv::Position::new(),
        )
    }
}
