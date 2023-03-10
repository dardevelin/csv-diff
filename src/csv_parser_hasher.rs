use crossbeam_channel::{Receiver, Sender};
use csv::Reader;
use std::collections::HashSet;
use std::hash::Hasher;
use std::io::{Read, Seek};
use xxhash_rust::xxh3::{xxh3_128, Xxh3};

use crate::csv::Csv;
use crate::csv_hasher::CsvHasherExt;
use crate::csv_parse_result::{
    CsvByteRecordWithHash, CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft,
    CsvParseResultRight, Position, RecordHash, RecordHashWithPosition,
};

impl<R> CsvParseResult<CsvLeftRightParseResult<R>, R> for CsvParseResultLeft<R> {
    #[inline]
    fn new(record_hash: R) -> Self {
        Self {
            csv_left_right_parse_result: CsvLeftRightParseResult::Left(record_hash),
        }
    }
    #[inline]
    fn into_payload(self) -> CsvLeftRightParseResult<R> {
        self.csv_left_right_parse_result
    }
}

impl<R> CsvParseResult<CsvLeftRightParseResult<R>, R> for CsvParseResultRight<R> {
    #[inline]
    fn new(record_hash: R) -> Self {
        Self {
            csv_left_right_parse_result: CsvLeftRightParseResult::Right(record_hash),
        }
    }
    #[inline]
    fn into_payload(self) -> CsvLeftRightParseResult<R> {
        self.csv_left_right_parse_result
    }
}

pub(crate) struct CsvParserHasherLinesSender<T> {
    sender: Sender<T>,
    sender_total_lines: Sender<u64>,
}

impl CsvParserHasherLinesSender<CsvLeftRightParseResult<RecordHashWithPosition>> {
    pub fn new(
        sender: Sender<CsvLeftRightParseResult<RecordHashWithPosition>>,
        sender_total_lines: Sender<u64>,
    ) -> Self {
        Self {
            sender,
            sender_total_lines,
        }
    }
    pub fn parse_and_hash<
        R: Read + Seek + Send,
        T: CsvParseResult<CsvLeftRightParseResult<RecordHashWithPosition>, RecordHashWithPosition>,
    >(
        &mut self,
        csv: Csv<R>,
        primary_key_columns: &HashSet<usize>,
    ) -> csv::Result<csv::Reader<R>> {
        let mut csv_reader: Reader<R> = csv.into_csv_reader();
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

            let record = csv_record_first;
            let key_fields_iter = fields_as_key.iter().filter_map(|k_idx| record.get(*k_idx));
            if key_fields_iter.peekable().peek().is_some() {
                let key = record.hash_key_fields(fields_as_key.as_slice());
                // TODO: don't hash all of it -> exclude the key fields (see below)
                let hash_record = record.hash_record();
                let pos = record.position().expect("a record position");
                self.sender
                    .send(
                        T::new(RecordHashWithPosition::new(
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
                                T::new(RecordHashWithPosition::new(
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

pub(crate) struct CsvParserHasherSender<T> {
    sender: Sender<T>,
}

impl CsvParserHasherSender<CsvLeftRightParseResult<CsvByteRecordWithHash>> {
    pub fn new(sender: Sender<CsvLeftRightParseResult<CsvByteRecordWithHash>>) -> Self {
        Self { sender }
    }
    pub fn parse_and_hash<
        R: Read + Send,
        T: CsvParseResult<CsvLeftRightParseResult<CsvByteRecordWithHash>, CsvByteRecordWithHash>,
    >(
        &mut self,
        csv: Csv<R>,
        primary_key_columns: &HashSet<usize>,
        receiver_csv_recycle: Receiver<csv::ByteRecord>,
    ) {
        let mut csv_reader: Reader<R> = csv.into_csv_reader();
        let mut csv_record = csv::ByteRecord::new();
        // read first record in order to get the number of fields
        match csv_reader.read_byte_record(&mut csv_record) {
            Ok(true) => {
                let record = std::mem::take(&mut csv_record);
                let fields_as_key: Vec<_> = primary_key_columns.iter().copied().collect();
                // TODO: maybe use this in order to only hash fields that are values and not act
                // as primary keys. We should probably only do this, if primary key field indices are
                // contiguous, because otherwise we will have multiple calls to our hashing function,
                // which could hurt performance.
                // let num_of_fields = record.len();
                // let fields_as_value: Vec<_> = (0..num_of_fields)
                //     .filter(|x| !primary_key_columns.contains(x))
                //     .collect();

                let mut hasher = Xxh3::new();
                let mut key_fields_iter = fields_as_key
                    .iter()
                    .filter_map(|k_idx| record.get(*k_idx))
                    .peekable();
                if key_fields_iter.peek().is_some() {
                    // TODO: try to do it with as few calls to `write` as possible (see below)
                    for key_field in key_fields_iter {
                        hasher.write(key_field);
                    }
                    let key = hasher.digest128();
                    // TODO: don't hash all of it -> exclude the key fields (see below)
                    let hash_record = xxh3_128(record.as_slice());
                    // we ignore any sending errors
                    let _ = self.sender.send(
                        T::new(CsvByteRecordWithHash::new(
                            Ok(record),
                            RecordHash::new(key, hash_record),
                        ))
                        .into_payload(),
                    );

                    loop {
                        let mut csv_record = receiver_csv_recycle
                            .try_recv()
                            .unwrap_or_else(|_| csv::ByteRecord::new());

                        match csv_reader.read_byte_record(&mut csv_record) {
                            Ok(true) => {
                                hasher.reset();
                                let key_fields = fields_as_key
                                    .iter()
                                    .filter_map(|k_idx| csv_record.get(*k_idx));
                                // TODO: try to do it with as few calls to `write` as possible (see below)
                                for key_field in key_fields {
                                    hasher.write(key_field);
                                }
                                let key = hasher.digest128();
                                // TODO: don't hash all of it -> exclude the key fields
                                // in order to still be efficient and do as few `write` calls as possible
                                // consider using `csv_record.range(...)` method
                                let hash_record = xxh3_128(csv_record.as_slice());
                                if self
                                    .sender
                                    .send(
                                        T::new(CsvByteRecordWithHash::new(
                                            Ok(csv_record),
                                            RecordHash::new(key, hash_record),
                                        ))
                                        .into_payload(),
                                    )
                                    .is_err()
                                {
                                    // when the receiver is gone, it doesn't make sense to continue here
                                    break;
                                }
                            }
                            Ok(false) => break,
                            Err(e) => {
                                if self
                                    .sender
                                    .send(
                                        T::new(CsvByteRecordWithHash::new(
                                            Err(e),
                                            RecordHash::new(0, 0),
                                        ))
                                        .into_payload(),
                                    )
                                    .is_err()
                                {
                                    // when the receiver is gone, it doesn't make sense to continue here
                                    break;
                                }
                                break;
                            }
                        }
                    }
                }
            }
            Ok(false) => { /* Do nothing, we have reached EOF */ }
            Err(e) => self
                .sender
                .send(
                    T::new(CsvByteRecordWithHash::new(Err(e), RecordHash::new(0, 0)))
                        .into_payload(),
                )
                .unwrap(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum HashMapValue<T, TEq = T> {
    Initial(u128, T),
    Equal(TEq, TEq),
    Modified(T, T),
}
