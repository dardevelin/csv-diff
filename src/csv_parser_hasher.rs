use ahash::AHasher;
use crossbeam_channel::Sender;
use std::collections::HashSet;
use std::hash::Hasher;
use std::io::Cursor;
use std::io::{Read, Seek};

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

pub(crate) const STACK_SIZE_VEC: usize = 32;
pub(crate) type StackVec<T> = smallvec::SmallVec<[T; STACK_SIZE_VEC]>;

// TODO: there will probably also one without a sender
pub(crate) struct CsvParserHasherSender<T> {
    sender: Sender<StackVec<T>>,
    sender_total_lines: Sender<u64>,
}

impl CsvParserHasherSender<CsvLeftRightParseResult> {
    pub fn new(
        sender: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines: Sender<u64>,
    ) -> Self {
        Self {
            sender,
            sender_total_lines,
        }
    }
    pub fn parse_and_hash<
        R: Read + Seek,
        T: CsvParseResult<CsvLeftRightParseResult, RecordHash>,
    >(
        &mut self,
        csv: R,
        primary_key_columns: &HashSet<usize>,
    ) -> csv::Reader<R> {
        let mut csv_reader = csv::Reader::from_reader(csv);
        let mut csv_record = csv::ByteRecord::new();
        // read first record in order to get the number of fields
        if let Ok(true) = csv_reader.read_byte_record(&mut csv_record) {
            let csv_record_right_first = std::mem::take(&mut csv_record);
            let num_of_fields = csv_record_right_first.len();
            let fields_as_key: Vec<_> = primary_key_columns.iter().collect();
            let fields_as_value: Vec<_> = (0..num_of_fields)
                .filter(|x| !primary_key_columns.contains(x))
                .collect();

            let mut records_buff: StackVec<CsvLeftRightParseResult> = smallvec::SmallVec::new();

            let mut hasher = AHasher::default();
            let record = csv_record_right_first;
            let key_fields: Vec<_> = fields_as_key
                .iter()
                .filter_map(|k_idx| record.get(**k_idx))
                .collect();
            if !key_fields.is_empty() {
                // TODO: try to do it with as few calls to `write` as possible (see below)
                for key_field in key_fields {
                    hasher.write(key_field);
                }
                let key = hasher.finish();
                // TODO: don't hash all of it -> exclude the key fields (see below)
                hasher.write(record.as_slice());
                let pos = record.position().expect("a record position");
                records_buff.push(
                    T::new(RecordHash::new(
                        key,
                        hasher.finish(),
                        Position::new(pos.byte(), pos.line()),
                    ))
                    .into_payload(),
                );
                let mut line = 2;
                while csv_reader
                    .read_byte_record(&mut csv_record)
                    .unwrap_or(false)
                {
                    let mut hasher = AHasher::default();
                    let key_fields = fields_as_key
                        .iter()
                        .filter_map(|k_idx| csv_record.get(**k_idx));
                    // TODO: try to do it with as few calls to `write` as possible (see below)
                    for key_field in key_fields {
                        hasher.write(key_field);
                    }
                    let key = hasher.finish();
                    // TODO: don't hash all of it -> exclude the key fields
                    // in order to still be efficient and do as few `write` calls as possible
                    // consider using `csv_record.range(...)` method
                    hasher.write(csv_record.as_slice());
                    {
                        let pos = csv_record.position().expect("a record position");
                        records_buff.push(
                            T::new(RecordHash::new(
                                key,
                                hasher.finish(),
                                Position::new(pos.byte(), pos.line()),
                            ))
                            .into_payload(),
                        );
                        if line % STACK_SIZE_VEC as u64 == 0 {
                            let records_buff_full: StackVec<CsvLeftRightParseResult> =
                                smallvec::SmallVec::from_slice(records_buff.as_slice());
                            self.sender.send(records_buff_full).unwrap();
                            records_buff.clear();
                        }
                    }
                    line += 1;
                }
                // if our buffer has elements that have not been send over yet (our buffer is only partly filled),
                // we send them over now
                if !records_buff.is_empty() {
                    self.sender.send(records_buff).unwrap();
                }
                self.sender_total_lines.send(line).unwrap();
            }
        } else {
            self.sender_total_lines.send(0).unwrap();
        }
        csv_reader
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub(crate) enum CsvLeftRightParseResult {
    Left(RecordHash),
    Right(RecordHash),
}

#[derive(Debug, PartialEq, Copy, Clone)]
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
