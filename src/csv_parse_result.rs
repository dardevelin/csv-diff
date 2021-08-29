pub trait CsvParseResult<P, T> {
    fn new(payload_inner: T) -> Self;
    fn into_payload(self) -> P;
}

pub struct CsvParseResultLeft {
    pub(crate) csv_left_right_parse_result: CsvLeftRightParseResult,
}

pub struct CsvParseResultRight {
    pub(crate) csv_left_right_parse_result: CsvLeftRightParseResult,
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CsvLeftRightParseResult {
    Left(RecordHash),
    Right(RecordHash),
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub struct RecordHash {
    pub(crate) key: u64,
    pub(crate) record_hash: u64,
    pub(crate) pos: Position,
}

impl RecordHash {
    pub(crate) fn new(key: u64, record_hash: u64, pos: Position) -> Self {
        Self {
            key,
            record_hash,
            pos,
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) struct Position {
    pub byte_offset: u64,
    pub line: u64,
}

impl Position {
    pub fn new(byte_offset: u64, line: u64) -> Self {
        Self { byte_offset, line }
    }
}

#[allow(clippy::from_over_into)]
impl Into<csv::Position> for Position {
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
