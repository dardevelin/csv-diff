use ahash::AHasher;
use std::collections::HashSet;
use std::hash::Hasher;
use std::io::{Read, Seek};
use std::sync::mpsc::Sender;

pub(crate) trait CsvParseResult<P, T> {
    fn new(payload_inner: T) -> Self;
    fn into_payload(self) -> P;
}

pub(crate) struct CsvParseResultLeft {
    csv_left_right_parse_result: CsvLeftRightParseResult,
}

impl CsvParseResult<CsvLeftRightParseResult, RecordHash> for CsvParseResultLeft {
    #[inline]
    fn new(record_hash: RecordHash) -> Self {
        Self {
            csv_left_right_parse_result: CsvLeftRightParseResult::Left(record_hash),
        }
    }
    #[inline]
    fn into_payload(self) -> CsvLeftRightParseResult {
        self.csv_left_right_parse_result
    }
}

pub(crate) struct CsvParseResultRight {
    csv_left_right_parse_result: CsvLeftRightParseResult,
}

impl CsvParseResult<CsvLeftRightParseResult, RecordHash> for CsvParseResultRight {
    #[inline]
    fn new(record_hash: RecordHash) -> Self {
        Self {
            csv_left_right_parse_result: CsvLeftRightParseResult::Right(record_hash),
        }
    }
    #[inline]
    fn into_payload(self) -> CsvLeftRightParseResult {
        self.csv_left_right_parse_result
    }
}

// TODO: there will probably also one without a sender
pub(crate) struct CsvParserHasherSender<T> {
    sender: Sender<T>,
    sender_total_lines: Sender<u64>,
}

impl<CsvLeftRightParseResult> CsvParserHasherSender<CsvLeftRightParseResult> {
    pub fn new(sender: Sender<CsvLeftRightParseResult>, sender_total_lines: Sender<u64>) -> Self {
        Self {
            sender,
            sender_total_lines,
        }
    }
    pub fn parse_and_hash<R: Read, T: CsvParseResult<CsvLeftRightParseResult, RecordHash>>(
        &mut self,
        csv: R,
        primary_key_columns: &HashSet<usize>,
    ) {
        let mut csv_reader_right = csv::Reader::from_reader(csv);
        let mut csv_record_right = csv::ByteRecord::new();
        // read first record in order to get the number of fields
        if let Ok(true) = csv_reader_right.read_byte_record(&mut csv_record_right) {
            let csv_record_right_first = std::mem::take(&mut csv_record_right);
            let num_of_fields = csv_record_right_first.len();
            let fields_as_key: Vec<_> = primary_key_columns.iter().collect();
            let fields_as_value: Vec<_> = (0..num_of_fields)
                .filter(|x| !primary_key_columns.contains(x))
                .collect();

            let mut hasher = AHasher::default();
            let record = csv_record_right_first;
            let key_fields: Vec<_> = fields_as_key
                .iter()
                .filter_map(|k_idx| record.get(**k_idx))
                .collect();
            if !key_fields.is_empty() {
                for key_field in key_fields {
                    hasher.write(key_field);
                }
                let key = hasher.finish();

                for i in fields_as_value.iter() {
                    hasher.write(record.get(*i).unwrap());
                }
                let pos = record.position().expect("a record position");
                self.sender
                    .send(
                        T::new(RecordHash::new(
                            key,
                            hasher.finish(),
                            Position::new(pos.byte(), pos.line()),
                        ))
                        .into_payload(),
                    )
                    .unwrap();
                let mut line = 2;
                while csv_reader_right
                    .read_byte_record(&mut csv_record_right)
                    .unwrap_or(false)
                {
                    let mut hasher = AHasher::default();
                    let key_fields = fields_as_key
                        .iter()
                        .filter_map(|k_idx| csv_record_right.get(**k_idx));
                    for key_field in key_fields {
                        hasher.write(key_field);
                    }
                    let key = hasher.finish();
                    for i in fields_as_value.iter() {
                        hasher.write(csv_record_right.get(*i).unwrap());
                    }
                    {
                        let pos = csv_record_right.position().expect("a record position");
                        self.sender
                            .send(
                                T::new(RecordHash::new(
                                    key,
                                    hasher.finish(),
                                    Position::new(pos.byte(), pos.line()),
                                ))
                                .into_payload(),
                            )
                            .unwrap();
                    }
                    line += 1;
                }
                self.sender_total_lines.send(line).unwrap();
                //sender_total_lines.send(csv_reader_right).unwrap();
            }
        } else {
            self.sender_total_lines.send(0).unwrap();
        }
    }
}

pub(crate) enum CsvLeftRightParseResult {
    Left(RecordHash),
    Right(RecordHash),
}

#[derive(Debug, PartialEq)]
pub(crate) struct RecordHash {
    pub key: u64,
    pub record_hash: u64,
    pub pos: Position,
}

impl RecordHash {
    pub fn new(key: u64, record_hash: u64, pos: Position) -> Self {
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

pub(crate) enum HashMapValue {
    Initial(u64, Position),
    Equal,
    Modified(Position, Position),
}
