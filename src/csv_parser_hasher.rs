use crossbeam_channel::Sender;
use csv::Reader;
use std::collections::HashSet;
use std::hash::Hasher;
use std::io::{Read, Seek};
use xxhash_rust::xxh3::{xxh3_128, Xxh3};

use crate::csv::Csv;
use crate::csv_hasher::CsvHasherExt;
use crate::csv_parse_result::{
    CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight, Position,
    RecordHash,
};

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

pub(crate) struct CsvParserHasherSender<T> {
    sender: Sender<T>,
    sender_total_lines: Sender<u64>,
}

impl CsvParserHasherSender<CsvLeftRightParseResult> {
    pub fn new(sender: Sender<CsvLeftRightParseResult>, sender_total_lines: Sender<u64>) -> Self {
        Self {
            sender,
            sender_total_lines,
        }
    }
    pub fn parse_and_hash<
        R: Read + Seek + Send,
        T: CsvParseResult<CsvLeftRightParseResult, RecordHash>,
    >(
        &mut self,
        csv: Csv<R>,
        primary_key_columns: &HashSet<usize>,
    ) -> csv::Result<csv::Reader<R>> {
        let mut csv_reader: Reader<R> = csv.into();
        let mut csv_record = csv::ByteRecord::new();
        // read first record in order to get the number of fields
        if csv_reader.read_byte_record(&mut csv_record)? {
            let csv_record_first = std::mem::take(&mut csv_record);
            let fields_as_key: Vec<_> = primary_key_columns.iter().copied().collect();
            // TODO: maybe use this in order to only hash fields that are values and not act
            // as primary keys. We should probably only do this, if primary key field indices are
            // contiguous, because otherwise we will have multiple calls to our hashing function,
            // which could hurt performance.
            // let num_of_fields = csv_record_first.len();
            // let fields_as_value: Vec<_> = (0..num_of_fields)
            //     .filter(|x| !primary_key_columns.contains(x))
            //     .collect();

            let mut records_buff: StackVec<CsvLeftRightParseResult> = smallvec::SmallVec::new();

            let record = csv_record_first;
            let key_fields: Vec<_> = fields_as_key
                .iter()
                .filter_map(|k_idx| record.get(*k_idx))
                .collect();
            if !key_fields.is_empty() {
                let key = record.hash_key_fields(fields_as_key.as_slice());
                // TODO: don't hash all of it -> exclude the key fields (see below)
                let hash_record = record.hash_record();
                let pos = record.position().expect("a record position");
                self.sender
                    .send(
                        T::new(RecordHash::new(
                            key,
                            hash_record,
                            Position::new(pos.byte(), pos.line()),
                        ))
                        .into_payload(),
                    )
                    .unwrap();
                let mut line = 2;
                while csv_reader.read_byte_record(&mut csv_record)? {
                    let key = csv_record.hash_key_fields(fields_as_key.as_slice());
                    let hash_record = csv_record.hash_record();
                    {
                        let pos = csv_record.position().expect("a record position");
                        self.sender
                            .send(
                                T::new(RecordHash::new(
                                    key,
                                    hash_record,
                                    Position::new(pos.byte(), pos.line()),
                                ))
                                .into_payload(),
                            )
                            .unwrap();
                    }
                    line += 1;
                }
                self.sender_total_lines.send(line).unwrap();
            }
        } else {
            self.sender_total_lines.send(0).unwrap();
        }
        Ok(csv_reader)
    }
}

#[derive(Debug)]
pub(crate) enum HashMapValue {
    Initial(u128, Position),
    Equal,
    Modified(Position, Position),
}
