use crate::{
    csv_parse_result::CsvLeftRightParseResult, csv_parser_hasher::HashMapValue, diff_row::*,
};
use ahash::AHashMap as HashMap;
use crossbeam_channel::Receiver;
use std::{
    cmp::Ordering,
    collections::hash_map::IntoIter,
    io::{Read, Seek},
};

/// Holds all information about the difference between two CSVs, after they have
/// been compared with [`CsvByteDiffLocal.diff`](crate::csv_diff::CsvByteDiffLocal::diff).
/// CSV records that are equal are __not__ stored in this structure.
///
/// Also, keep in mind, that differences are stored _unordered_ (with regard to the line in the CSV).
/// You can use [`DiffByteRecords.sort_by_line`](DiffByteRecords::sort_by_line) to sort them in-place.
///
/// See the example on [`CsvByteDiffLocal`](crate::csv_diff::CsvByteDiffLocal) for general usage.
#[derive(Debug, PartialEq, Clone)]
pub struct DiffByteRecords(pub(crate) Vec<DiffByteRecord>);

impl DiffByteRecords {
    /// Sort the underlying [`DiffByteRecord`](crate::diff_row::DiffByteRecord)s by line.
    ///
    /// Note that comparison is done in parallel. Therefore, __without calling this method__, the resulting `DiffByteRecord`s are out of order
    /// after the comparison (with regard to their line in the original CSV).
    pub fn sort_by_line(&mut self) {
        self.0.sort_by(|a, b| match (a.line_num(), b.line_num()) {
            (LineNum::OneSide(line_num_a), LineNum::OneSide(line_num_b)) => line_num_a
                .cmp(&line_num_b)
                .then(if matches!(a, DiffByteRecord::Delete(..)) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }),
            (
                LineNum::OneSide(line_num_a),
                LineNum::BothSides {
                    for_deleted,
                    for_added,
                },
            ) => line_num_a
                .cmp(if for_deleted < for_added {
                    &for_deleted
                } else {
                    &for_added
                })
                .then(if matches!(a, DiffByteRecord::Delete(..)) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }),
            (
                LineNum::BothSides {
                    for_deleted,
                    for_added,
                },
                LineNum::OneSide(line_num_b),
            ) => if for_deleted < for_added {
                &for_deleted
            } else {
                &for_added
            }
            .cmp(&line_num_b)
            .then(if matches!(b, DiffByteRecord::Add(..)) {
                Ordering::Less
            } else {
                Ordering::Greater
            }),
            (
                LineNum::BothSides {
                    for_deleted: for_deleted_a,
                    for_added: for_added_a,
                },
                LineNum::BothSides {
                    for_deleted: for_deleted_b,
                    for_added: for_added_b,
                },
            ) => if for_deleted_a < for_added_a {
                &for_deleted_a
            } else {
                &for_added_a
            }
            .cmp(if for_deleted_b < for_added_b {
                &for_deleted_b
            } else {
                &for_added_b
            }),
        })
    }

    /// Return the `DiffByteRecord`s as a single slice.
    /// # Example
    #[cfg_attr(
        feature = "rayon-threads",
        doc = r##"
    use csv_diff::{csv_diff::CsvByteDiffLocal, csv::Csv};
    use std::collections::HashSet;
    use std::iter::FromIterator;
    # fn main() -> Result<(), Box<dyn std::error::Error>> {
    // some csv data with a header, where the first column is a unique id
    let csv_data_left = "id,name,kind\n\
                         1,lemon,fruit\n\
                         2,strawberry,fruit";
    let csv_data_right = "id,name,kind\n\
                          1,lemon,fruit\n\
                          2,strawberry,nut\n\
                          3,cherry,fruit";

    let csv_byte_diff = CsvByteDiffLocal::new()?;

    let mut diff_byte_records = csv_byte_diff.diff(
        Csv::with_reader_seek(csv_data_left.as_bytes()),
        Csv::with_reader_seek(csv_data_right.as_bytes()),
    )?;
    
    let diff_byte_record_slice = diff_byte_records.as_slice();

    assert_eq!(
        diff_byte_record_slice.len(),
        2
    );
    Ok(())
    # }
    "##
    )]
    pub fn as_slice(&self) -> &[DiffByteRecord] {
        self.0.as_slice()
    }

    /// Return an iterator over the `DiffByteRecord`s.
    pub fn iter(&self) -> core::slice::Iter<'_, DiffByteRecord> {
        self.0.iter()
    }
}

impl IntoIterator for DiffByteRecords {
    type Item = DiffByteRecord;
    type IntoIter = DiffByteRecordsIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        DiffByteRecordsIntoIterator {
            inner: self.0.into_iter(),
        }
    }
}

/// Consuming iterator that can be created from [`DiffByteRecords`](DiffByteRecords)
pub struct DiffByteRecordsIntoIterator {
    inner: std::vec::IntoIter<DiffByteRecord>,
}

impl Iterator for DiffByteRecordsIntoIterator {
    type Item = DiffByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub(crate) type CsvHashValueMap = HashMap<u128, HashMapValue>;

pub struct DiffByteRecordsIterator<R> {
    buf: Vec<DiffByteRecord>,
    csv_left_right_parse_results: Receiver<CsvLeftRightParseResult>,
    csv_records_left_map: CsvHashValueMap,
    csv_records_left_map_iter: Option<IntoIter<u128, HashMapValue>>,
    csv_records_right_map: CsvHashValueMap,
    csv_records_right_map_iter: Option<IntoIter<u128, HashMapValue>>,
    intermediate_left_map: CsvHashValueMap,
    intermediate_right_map: CsvHashValueMap,
    max_capacity_left_map: usize,
    max_capacity_right_map: usize,
    csv_seek_left_reader: csv::Reader<R>,
    csv_seek_right_reader: csv::Reader<R>,
}

impl<R: Read + Seek> DiffByteRecordsIterator<R> {
    pub(crate) fn new(
        csv_left_right_parse_results: Receiver<CsvLeftRightParseResult>,
        left_capacity: usize,
        right_capacity: usize,
        left_reader: csv::Reader<R>,
        right_reader: csv::Reader<R>,
    ) -> Self {
        Self {
            buf: Default::default(),
            csv_left_right_parse_results,
            csv_records_left_map: HashMap::with_capacity(left_capacity),
            csv_records_left_map_iter: None,
            csv_records_right_map: HashMap::with_capacity(right_capacity),
            csv_records_right_map_iter: None,
            intermediate_left_map: HashMap::new(),
            intermediate_right_map: HashMap::new(),
            max_capacity_left_map: left_capacity,
            max_capacity_right_map: right_capacity,
            csv_seek_left_reader: left_reader,
            csv_seek_right_reader: right_reader,
        }
    }
}

impl<R: Read + Seek> Iterator for DiffByteRecordsIterator<R> {
    type Item = DiffByteRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buf.is_empty() {
            return self.buf.pop();
        }
        while let Ok(csv_left_right_parse_result) = self.csv_left_right_parse_results.recv() {
            match csv_left_right_parse_result {
                CsvLeftRightParseResult::Left(left_record_res) => {
                    let pos_left = left_record_res.pos;
                    let key = left_record_res.key;
                    let record_hash_left = left_record_res.record_hash;
                    match self.csv_records_right_map.get_mut(&key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(ref record_hash_right, ref pos_right) =
                                hash_map_val
                            {
                                if record_hash_left != *record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(pos_left, *pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal;
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
                        // in order to circumvent borrowing issues in closure
                        let mut csv_records_right_map =
                            std::mem::take(&mut self.csv_records_right_map);
                        for (k, v) in csv_records_right_map.drain() {
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
                                            Vec::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.push(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.buf.push(DiffByteRecord::Modify {
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
                        self.csv_records_right_map = csv_records_right_map;
                        std::mem::swap(
                            &mut self.intermediate_right_map,
                            &mut self.csv_records_right_map,
                        );
                        if !self.buf.is_empty() {
                            break;
                        }
                    }
                }
                CsvLeftRightParseResult::Right(right_record_res) => {
                    let pos_right = right_record_res.pos;
                    let key = right_record_res.key;
                    let record_hash_right = right_record_res.record_hash;
                    match self.csv_records_left_map.get_mut(&key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(ref record_hash_left, ref pos_left) =
                                hash_map_val
                            {
                                if *record_hash_left != record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(*pos_left, pos_right);
                                } else {
                                    *hash_map_val = HashMapValue::Equal;
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
                        // in order to circumvent borrowing issues in closure
                        let mut csv_records_left_map =
                            std::mem::take(&mut self.csv_records_left_map);
                        for (k, v) in csv_records_left_map.drain() {
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
                                            Vec::new(),
                                            |mut acc, ((idx, field_left), field_right)| {
                                                if field_left != field_right {
                                                    acc.push(idx);
                                                }
                                                acc
                                            },
                                        );
                                    self.buf.push(DiffByteRecord::Modify {
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
                        self.csv_records_left_map = csv_records_left_map;
                        std::mem::swap(
                            &mut self.intermediate_left_map,
                            &mut self.csv_records_left_map,
                        );
                        if !self.buf.is_empty() {
                            break;
                        }
                    }
                }
            }
        }

        if !self.buf.is_empty() {
            return self.buf.pop();
        }

        let iter_left_map = self
            .csv_records_left_map_iter
            .get_or_insert(std::mem::take(&mut self.csv_records_left_map).into_iter());

        let mut iter_left_map = iter_left_map.skip_while(|(_, v)| matches!(v, HashMapValue::Equal));
        match iter_left_map.next() {
            Some((_, HashMapValue::Initial(_hash, pos))) => {
                self.csv_seek_left_reader
                    .seek(pos.into())
                    .expect("must be found");
                let mut byte_record = csv::ByteRecord::new();
                self.csv_seek_left_reader
                    .read_byte_record(&mut byte_record)
                    .expect("can be read");
                return Some(DiffByteRecord::Delete(ByteRecordLineInfo::new(
                    byte_record,
                    pos.line,
                )));
            }
            Some((_, HashMapValue::Modified(pos_left, pos_right))) => {
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
                return Some(DiffByteRecord::Modify {
                    add: ByteRecordLineInfo::new(right_byte_record, pos_right.line),
                    delete: ByteRecordLineInfo::new(left_byte_record, pos_left.line),
                    field_indices: fields_modified,
                });
            }
            _ => (),
        }

        let iter_right_map = self
            .csv_records_right_map_iter
            .get_or_insert(std::mem::take(&mut self.csv_records_right_map).into_iter());

        let mut iter_right_map =
            iter_right_map.skip_while(|(_, v)| matches!(v, HashMapValue::Equal));
        match iter_right_map.next() {
            Some((_, HashMapValue::Initial(_hash, pos))) => {
                self.csv_seek_right_reader
                    .seek(pos.into())
                    .expect("must be found");
                let mut byte_record = csv::ByteRecord::new();
                self.csv_seek_right_reader
                    .read_byte_record(&mut byte_record)
                    .expect("can be read");
                return Some(DiffByteRecord::Add(ByteRecordLineInfo::new(
                    byte_record,
                    pos.line,
                )));
            }
            Some((_, HashMapValue::Modified(pos_left, pos_right))) => {
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
                return Some(DiffByteRecord::Modify {
                    add: ByteRecordLineInfo::new(right_byte_record, pos_right.line),
                    delete: ByteRecordLineInfo::new(left_byte_record, pos_left.line),
                    field_indices: fields_modified,
                });
            }
            _ => (),
        }
        None
    }
}
