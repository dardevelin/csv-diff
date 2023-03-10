use crate::csv_parse_result::CsvLeftRightParseResult;
use crate::csv_parse_result::RecordHash;
use crate::csv_parse_result::RecordHashWithPosition;
use crate::csv_parser_hasher::HashMapValue;
use crate::diff_result::*;
use crate::diff_row::*;
use ahash::AHashMap as HashMap;
use std::io::Read;
use std::io::Seek;

pub(crate) struct CsvHashComparer<R: Read + Seek> {
    csv_records_left_map: CsvHashValueMap,
    csv_records_right_map: CsvHashValueMap,
    intermediate_left_map: CsvHashValueMap,
    intermediate_right_map: CsvHashValueMap,
    max_capacity_left_map: usize,
    max_capacity_right_map: usize,
    csv_seek_left_reader: csv::Reader<R>,
    csv_seek_right_reader: csv::Reader<R>,
    diff_records: Vec<DiffByteRecord>,
}

impl<R: Read + std::io::Seek> CsvHashComparer<R> {
    // TODO: maybe we can simplify this to only take one capacity and use it for both?
    // But keep in mind, we would loose on flexibility (one csv is very small and one very big?)
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
        csv_left_right_parse_results: impl IntoIterator<
            Item = CsvLeftRightParseResult<RecordHashWithPosition>,
        >,
    ) -> csv::Result<DiffByteRecords> {
        for csv_left_right_parse_result in csv_left_right_parse_results.into_iter() {
            match csv_left_right_parse_result {
                CsvLeftRightParseResult::Left(left_record_res) => {
                    let pos_left = left_record_res.pos;
                    let key = left_record_res.key();
                    let record_hash_left = left_record_res.record_hash_num();
                    match self.csv_records_right_map.get_mut(&key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(ref record_hash_right, ref pos_right) =
                                hash_map_val
                            {
                                if record_hash_left != *record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(pos_left, *pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal(
                                        left_record_res.record_hash,
                                        RecordHash::new(key, *record_hash_right),
                                    );
                                }
                            }
                        }
                        None => {
                            self.csv_records_left_map
                                .insert(key, HashMapValue::Initial(record_hash_left, pos_left));
                        }
                    }
                    if self.max_capacity_right_map > 0
                        && pos_left.line % self.max_capacity_right_map as u64 == 0
                    {
                        for (k, v) in self.csv_records_right_map.drain() {
                            match v {
                                HashMapValue::Equal(..) => {
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
                                            Vec::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.push(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.diff_records.push(DiffByteRecord::Modify {
                                        add: ByteRecordLineInfo::new(
                                            right_byte_record,
                                            pos_right.line,
                                        ),
                                        delete: ByteRecordLineInfo::new(
                                            left_byte_record,
                                            pos_left.line,
                                        ),
                                        field_indices: fields_modified,
                                    });
                                }
                            }
                        }

                        std::mem::swap(
                            &mut self.intermediate_right_map,
                            &mut self.csv_records_right_map,
                        );
                    }
                }
                CsvLeftRightParseResult::Right(right_record_res) => {
                    let pos_right = right_record_res.pos;
                    let key = right_record_res.key();
                    let record_hash_right = right_record_res.record_hash_num();
                    match self.csv_records_left_map.get_mut(&key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(ref record_hash_left, ref pos_left) =
                                hash_map_val
                            {
                                if *record_hash_left != record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(*pos_left, pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal(
                                        RecordHash::new(key, *record_hash_left),
                                        right_record_res.record_hash,
                                    );
                                }
                            }
                        }
                        None => {
                            self.csv_records_right_map
                                .insert(key, HashMapValue::Initial(record_hash_right, pos_right));
                        }
                    }
                    if self.max_capacity_left_map > 0
                        && pos_right.line % self.max_capacity_left_map as u64 == 0
                    {
                        for (k, v) in self.csv_records_left_map.drain() {
                            match v {
                                HashMapValue::Equal(..) => {
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
                                            Vec::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.push(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.diff_records.push(DiffByteRecord::Modify {
                                        add: ByteRecordLineInfo::new(
                                            right_byte_record,
                                            pos_right.line,
                                        ),
                                        delete: ByteRecordLineInfo::new(
                                            left_byte_record,
                                            pos_left.line,
                                        ),
                                        field_indices: fields_modified,
                                    });
                                }
                            }
                        }
                        std::mem::swap(
                            &mut self.intermediate_left_map,
                            &mut self.csv_records_left_map,
                        );
                    }
                }
            }
        }

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
                        Some(DiffByteRecord::Delete(ByteRecordLineInfo::new(
                            byte_record,
                            pos.line,
                        )))
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
                            .fold(Vec::new(), |mut acc, ((idx, field_left), field_right)| {
                                if field_left != field_right {
                                    acc.push(idx);
                                }
                                acc
                            });
                        Some(DiffByteRecord::Modify {
                            add: ByteRecordLineInfo::new(right_byte_record, pos_right.line),
                            delete: ByteRecordLineInfo::new(left_byte_record, pos_left.line),
                            field_indices: fields_modified,
                        })
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
                        Some(DiffByteRecord::Add(ByteRecordLineInfo::new(
                            byte_record,
                            pos.line,
                        )))
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
                            .fold(Vec::new(), |mut acc, ((idx, field_left), field_right)| {
                                if field_left != field_right {
                                    acc.push(idx);
                                }
                                acc
                            });
                        Some(DiffByteRecord::Modify {
                            add: ByteRecordLineInfo::new(right_byte_record, pos_right.line),
                            delete: ByteRecordLineInfo::new(left_byte_record, pos_left.line),
                            field_indices: fields_modified,
                        })
                    }
                    _ => None,
                }),
        );

        Ok(DiffByteRecords(diff_records))
    }
}
