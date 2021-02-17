use crate::csv_parser_hasher::CsvLeftRightParseResult;
use crate::csv_parser_hasher::HashMapValue;
use crate::csv_parser_hasher::StackVec;
use crate::diff_result::*;
use crate::diff_row::*;
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};

type CsvHashValueMap = HashMap<u64, HashMapValue>;

pub(crate) struct CsvHashComparer<R: Read + std::io::Seek> {
    csv_records_left_map: CsvHashValueMap,
    csv_records_right_map: CsvHashValueMap,
    intermediate_left_map: CsvHashValueMap,
    intermediate_right_map: CsvHashValueMap,
    max_capacity_left_map: usize,
    max_capacity_right_map: usize,
    csv_seek_left_reader: csv::Reader<R>,
    csv_seek_right_reader: csv::Reader<R>,
    diff_records: Vec<DiffRow>,
}

impl<R: Read + std::io::Seek> CsvHashComparer<R> {
    // TODO: maybe we can simplify this to only take one capacity and use it for both?
    // But keep in mind, we would loose on flexibility (one csv is very small and one very big?)
    // TODO: we have to see, whether `convert::AsRef` is a problem here
    // maybe we instead need `Seek`
    pub fn with_capacity_and_reader(
        left_capacity: usize,
        right_capacity: usize,
        left_reader: csv::Reader<R>,
        right_reader: csv::Reader<R>,
    ) -> Self {
        Self {
            csv_records_left_map: HashMap::with_capacity(left_capacity),
            csv_records_right_map: HashMap::with_capacity(right_capacity),
            intermediate_left_map: HashMap::new(),
            intermediate_right_map: HashMap::new(),
            max_capacity_left_map: left_capacity,
            max_capacity_right_map: right_capacity,
            csv_seek_left_reader: left_reader,
            csv_seek_right_reader: right_reader,
            diff_records: Vec::new(),
        }
    }

    pub fn compare_csv_left_right_parse_result(
        &mut self,
        csv_left_right_parse_results: impl IntoIterator<Item = StackVec<CsvLeftRightParseResult>>,
    ) -> csv::Result<DiffResult> {
        for csv_left_right_parse_result in csv_left_right_parse_results.into_iter().flatten() {
            match csv_left_right_parse_result {
                CsvLeftRightParseResult::Left(left_record_res) => {
                    let pos_left = left_record_res.pos;
                    let key = left_record_res.key;
                    let record_hash_left = left_record_res.record_hash;
                    match self.csv_records_right_map.get_mut(&key) {
                        Some(hash_map_val) => match hash_map_val {
                            HashMapValue::Initial(ref record_hash_right, ref pos_right) => {
                                if record_hash_left != *record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(pos_left, *pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal;
                                }
                            }
                            _ => {}
                        },
                        None => {
                            self.csv_records_left_map
                                .insert(key, HashMapValue::Initial(record_hash_left, pos_left));
                        }
                    }
                    if self.max_capacity_right_map > 0
                        && pos_left.line % self.max_capacity_right_map as u64 == 0
                    {
                        // in order to circumvent borrowing issues in closure
                        let mut csv_records_right_map =
                            std::mem::take(&mut self.csv_records_right_map);
                        csv_records_right_map.drain().for_each(|(k, v)| {
                            match v {
                                HashMapValue::Equal => {
                                    // nothing to do - will be removed
                                }
                                HashMapValue::Initial(_hash, _pos) => {
                                    // put it back, because we don't know what to do with this value yet
                                    self.intermediate_right_map.insert(k, v);
                                }
                                HashMapValue::Modified(pos_left, pos_right) => {
                                    self.csv_seek_left_reader
                                        .seek(pos_left.into())
                                        .expect("must find the given position");
                                    self.csv_seek_right_reader
                                        .seek(pos_right.into())
                                        .expect("must find the given position");
                                    let mut left_byte_record = csv::ByteRecord::new();
                                    // TODO: proper error handling (although we are safe here)
                                    self.csv_seek_left_reader
                                        .read_byte_record(&mut left_byte_record)
                                        .expect("can be read");
                                    let mut right_byte_record = csv::ByteRecord::new();
                                    // TODO: proper error handling (although we are safe here)
                                    self.csv_seek_right_reader
                                        .read_byte_record(&mut right_byte_record)
                                        .expect("can be read");
                                    let fields_modified = left_byte_record
                                        .iter()
                                        .enumerate()
                                        .zip(right_byte_record.iter())
                                        .fold(
                                            HashSet::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.insert(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.diff_records.push(DiffRow::Modified {
                                        added: RecordLineInfo::new(
                                            right_byte_record,
                                            pos_right.line,
                                        ),
                                        deleted: RecordLineInfo::new(
                                            left_byte_record,
                                            pos_left.line,
                                        ),
                                        field_indices: fields_modified,
                                    });
                                }
                            }
                        });
                        self.csv_records_right_map = csv_records_right_map;
                        std::mem::swap(
                            &mut self.intermediate_right_map,
                            &mut self.csv_records_right_map,
                        );
                    }
                }
                CsvLeftRightParseResult::Right(right_record_res) => {
                    let pos_right = right_record_res.pos;
                    let key = right_record_res.key;
                    let record_hash_right = right_record_res.record_hash;
                    match self.csv_records_left_map.get_mut(&key) {
                        Some(hash_map_val) => match hash_map_val {
                            HashMapValue::Initial(ref record_hash_left, ref pos_left) => {
                                if *record_hash_left != record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(*pos_left, pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal;
                                }
                            }
                            _ => {}
                        },
                        None => {
                            self.csv_records_right_map
                                .insert(key, HashMapValue::Initial(record_hash_right, pos_right));
                        }
                    }
                    if self.max_capacity_left_map > 0
                        && pos_right.line % self.max_capacity_left_map as u64 == 0
                    {
                        // in order to circumvent borrowing issues in closure
                        let mut csv_records_left_map =
                            std::mem::take(&mut self.csv_records_left_map);
                        csv_records_left_map.drain().for_each(|(k, v)| {
                            match v {
                                HashMapValue::Equal => {
                                    // nothing to do - will be removed
                                }
                                HashMapValue::Initial(_hash, _pos) => {
                                    // put it back, because we don't know what to do with this value yet
                                    self.intermediate_left_map.insert(k, v);
                                }
                                HashMapValue::Modified(pos_left, pos_right) => {
                                    self.csv_seek_left_reader
                                        .seek(pos_left.into())
                                        .expect("must find the given position");
                                    self.csv_seek_right_reader
                                        .seek(pos_right.into())
                                        .expect("must find the given position");
                                    let mut left_byte_record = csv::ByteRecord::new();
                                    // TODO: proper error handling (although we are safe here)
                                    self.csv_seek_left_reader
                                        .read_byte_record(&mut left_byte_record)
                                        .expect("can be read");
                                    let mut right_byte_record = csv::ByteRecord::new();
                                    // TODO: proper error handling (although we are safe here)
                                    self.csv_seek_right_reader
                                        .read_byte_record(&mut right_byte_record)
                                        .expect("can be read");
                                    let fields_modified = left_byte_record
                                        .iter()
                                        .enumerate()
                                        .zip(right_byte_record.iter())
                                        .fold(
                                            HashSet::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.insert(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.diff_records.push(DiffRow::Modified {
                                        added: RecordLineInfo::new(
                                            right_byte_record,
                                            pos_right.line,
                                        ),
                                        deleted: RecordLineInfo::new(
                                            left_byte_record,
                                            pos_left.line,
                                        ),
                                        field_indices: fields_modified,
                                    });
                                }
                            }
                        });
                        self.csv_records_left_map = csv_records_left_map;
                        std::mem::swap(
                            &mut self.intermediate_left_map,
                            &mut self.csv_records_left_map,
                        );
                    }
                }
            }
        }
        //diff_records.reserve(csv_records_left_map.map.len() + csv_records_right_map.map.len());

        let mut diff_records = std::mem::take(&mut self.diff_records);
        diff_records.extend(
            std::mem::take(&mut self.csv_records_left_map)
                .into_iter()
                .filter_map(|(_, v)| match v {
                    HashMapValue::Initial(_hash, pos) => {
                        self.csv_seek_left_reader
                            .seek(pos.into())
                            .expect("must be found");
                        let mut byte_record = csv::ByteRecord::new();
                        self.csv_seek_left_reader
                            .read_byte_record(&mut byte_record)
                            .expect("can be read");
                        Some(DiffRow::Deleted(RecordLineInfo::new(byte_record, pos.line)))
                    }
                    _ => None,
                }),
        );

        diff_records.extend(
            std::mem::take(&mut self.csv_records_right_map)
                .into_iter()
                .filter_map(|(_, v)| match v {
                    HashMapValue::Initial(_hash, pos) => {
                        self.csv_seek_right_reader
                            .seek(pos.into())
                            .expect("must be found");
                        let mut byte_record = csv::ByteRecord::new();
                        self.csv_seek_right_reader
                            .read_byte_record(&mut byte_record)
                            .expect("can be read");
                        Some(DiffRow::Added(RecordLineInfo::new(byte_record, pos.line)))
                    }
                    _ => None,
                }),
        );

        Ok(if diff_records.is_empty() {
            DiffResult::Equal
        } else {
            DiffResult::Different {
                diff_records: DiffRecords(diff_records),
            }
        })
    }
}
