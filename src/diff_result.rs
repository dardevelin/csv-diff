use crate::{
    csv_parse_result::{CsvByteRecordWithHash, CsvLeftRightParseResult, Position, RecordHash},
    csv_parser_hasher::HashMapValue,
    diff_row::*,
};
use ahash::AHashMap as HashMap;
use crossbeam_channel::{Receiver, Sender};
use std::{
    cmp::{max, Ordering},
    collections::{hash_map::IntoIter, VecDeque},
    convert::{TryFrom, TryInto},
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

pub(crate) type CsvHashValueMap = HashMap<u128, HashMapValue<Position, RecordHash>>;
pub(crate) type CsvByteRecordValueMap = HashMap<u128, HashMapValue<csv::ByteRecord>>;

struct MaxCapacityThreshold(usize);

impl MaxCapacityThreshold {
    #[inline]
    fn value(&self) -> usize {
        self.0
    }
    fn calc_new(&mut self, current_line: u64) {
        if current_line % 100 == 0 {
            self.0 = max(
                10,
                (current_line / 100)
                    .try_into()
                    .unwrap_or(usize::max_value()),
            );
        }
    }
}

/// Emits all information about the difference between two CSVs as
/// [`Result`](::csv::Result)<[`DiffByteRecord`](crate::diff_row::DiffByteRecord)>, after they have been compared with
/// [`CsvByteDiff.diff`](crate::csv_diff::CsvByteDiff::diff).
/// CSV records that are equal are __not__ emitted by this iterator.
///
/// Also, keep in mind, that this iterator produces values _unordered_ (with regard to the line in the CSV).
/// If you want to have them ordered, you first need to collect them into [`DiffByteRecords`] and then use
/// [`DiffByteRecords.sort_by_line`](DiffByteRecords::sort_by_line) to sort them in-place.
///
/// See the example on [`CsvByteDiff`](crate::csv_diff::CsvByteDiff) for general usage.
pub struct DiffByteRecordsIterator {
    buf: VecDeque<csv::Result<DiffByteRecord>>,
    csv_left_right_parse_results: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
    csv_records_left_map: CsvByteRecordValueMap,
    csv_records_left_map_iter: Option<IntoIter<u128, HashMapValue<csv::ByteRecord>>>,
    csv_records_right_map: CsvByteRecordValueMap,
    csv_records_right_map_iter: Option<IntoIter<u128, HashMapValue<csv::ByteRecord>>>,
    intermediate_left_map: CsvByteRecordValueMap,
    intermediate_right_map: CsvByteRecordValueMap,
    max_capacity_left_map: MaxCapacityThreshold,
    max_capacity_right_map: MaxCapacityThreshold,
    sender_csv_records_recycle: Sender<csv::ByteRecord>,
}

impl DiffByteRecordsIterator {
    pub(crate) fn new(
        csv_left_right_parse_results: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
        sender_csv_records_recycle: Sender<csv::ByteRecord>,
    ) -> Self {
        Self {
            buf: Default::default(),
            csv_left_right_parse_results,
            csv_records_left_map: HashMap::new(),
            csv_records_left_map_iter: None,
            csv_records_right_map: HashMap::new(),
            csv_records_right_map_iter: None,
            intermediate_left_map: HashMap::new(),
            intermediate_right_map: HashMap::new(),
            max_capacity_left_map: MaxCapacityThreshold(10),
            max_capacity_right_map: MaxCapacityThreshold(10),
            sender_csv_records_recycle,
        }
    }
}

impl Iterator for DiffByteRecordsIterator {
    type Item = csv::Result<DiffByteRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buf.is_empty() {
            return self.buf.pop_front();
        }
        while let Ok(csv_left_right_parse_result) = self.csv_left_right_parse_results.recv() {
            match csv_left_right_parse_result {
                CsvLeftRightParseResult::Left(CsvByteRecordWithHash {
                    byte_record: Ok(byte_record_left),
                    record_hash: record_hash_left,
                }) => {
                    let byte_record_left_line =
                        // TODO: the closure _might_ be a performance bottleneck!?
                        byte_record_left.position().map_or(0, |pos| pos.line());
                    match self.csv_records_right_map.get_mut(&record_hash_left.key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(record_hash_right, byte_record_right) =
                                hash_map_val
                            {
                                if record_hash_left.record_hash != *record_hash_right {
                                    *hash_map_val = HashMapValue::Modified(
                                        byte_record_left,
                                        std::mem::take(byte_record_right),
                                    );
                                } else {
                                    *hash_map_val = HashMapValue::Equal(
                                        byte_record_left,
                                        std::mem::take(byte_record_right),
                                    );
                                }
                            }
                        }
                        None => {
                            self.csv_records_left_map.insert(
                                record_hash_left.key,
                                HashMapValue::Initial(
                                    record_hash_left.record_hash,
                                    byte_record_left,
                                ),
                            );
                        }
                    }
                    if self.max_capacity_right_map.value() > 0
                        && byte_record_left_line % self.max_capacity_right_map.value() as u64 == 0
                    {
                        self.max_capacity_right_map.calc_new(byte_record_left_line);
                        for (k, v) in self.csv_records_right_map.drain() {
                            match v {
                                HashMapValue::Equal(byte_record_left, byte_record_right) => {
                                    // can be recycled, so we send it upstream;
                                    // if receiver is already gone, we ignore the error that occurs when sending,
                                    // which only leads to the byte record not being recycled (it can't be recycled,
                                    // because upstream has finished it's work)
                                    let _ = self.sender_csv_records_recycle.send(byte_record_left);
                                    let _ = self.sender_csv_records_recycle.send(byte_record_right);
                                }
                                HashMapValue::Initial(_hash, ref _byte_record) => {
                                    // put it back, because we don't know what to do with this value yet
                                    self.intermediate_right_map.insert(k, v);
                                }
                                HashMapValue::Modified(left_byte_record, right_byte_record) => {
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
                                    let left_byte_record_line = left_byte_record
                                        .position()
                                        // TODO: handle error (although it shouldn't error here)
                                        .expect("a record position")
                                        .line();
                                    let right_byte_record_line = right_byte_record
                                        .position()
                                        // TODO: handle error (although it shouldn't error here)
                                        .expect("a record position")
                                        .line();
                                    self.buf.push_back(Ok(DiffByteRecord::Modify {
                                        add: ByteRecordLineInfo::new(
                                            right_byte_record,
                                            right_byte_record_line,
                                        ),
                                        delete: ByteRecordLineInfo::new(
                                            left_byte_record,
                                            left_byte_record_line,
                                        ),
                                        field_indices: fields_modified,
                                    }));
                                }
                            }
                        }
                        std::mem::swap(
                            &mut self.intermediate_right_map,
                            &mut self.csv_records_right_map,
                        );
                        if !self.buf.is_empty() {
                            break;
                        }
                    }
                }
                CsvLeftRightParseResult::Left(CsvByteRecordWithHash {
                    byte_record: Err(byte_record_left_err),
                    ..
                }) => {
                    self.buf.push_back(Err(byte_record_left_err));
                    break;
                }
                CsvLeftRightParseResult::Right(CsvByteRecordWithHash {
                    byte_record: Ok(byte_record_right),
                    record_hash: record_hash_right,
                }) => {
                    // TODO: the closure _might_ be a performance bottleneck!?
                    let byte_record_right_line =
                        byte_record_right.position().map_or(0, |pos| pos.line());
                    match self.csv_records_left_map.get_mut(&record_hash_right.key) {
                        Some(hash_map_val) => {
                            if let HashMapValue::Initial(record_hash_left, byte_record_left) =
                                hash_map_val
                            {
                                if *record_hash_left != record_hash_right.record_hash {
                                    *hash_map_val = HashMapValue::Modified(
                                        std::mem::take(byte_record_left),
                                        byte_record_right,
                                    );
                                } else {
                                    *hash_map_val = HashMapValue::Equal(
                                        std::mem::take(byte_record_left),
                                        byte_record_right,
                                    );
                                }
                            }
                        }
                        None => {
                            self.csv_records_right_map.insert(
                                record_hash_right.key,
                                HashMapValue::Initial(
                                    record_hash_right.record_hash,
                                    byte_record_right,
                                ),
                            );
                        }
                    }
                    if self.max_capacity_left_map.value() > 0
                        && byte_record_right_line % self.max_capacity_left_map.value() as u64 == 0
                    {
                        self.max_capacity_left_map.calc_new(byte_record_right_line);
                        for (k, v) in self.csv_records_left_map.drain() {
                            match v {
                                HashMapValue::Equal(byte_record_left, byte_record_right) => {
                                    // can be recycled, so we send it upstream;
                                    // if receiver is already gone, we ignore the error that occurs when sending,
                                    // which only leads to the byte record not being recycled (it can't be recycled,
                                    // because upstream has finished it's work)
                                    let _ = self.sender_csv_records_recycle.send(byte_record_left);
                                    let _ = self.sender_csv_records_recycle.send(byte_record_right);
                                }
                                HashMapValue::Initial(_hash, ref _byte_record) => {
                                    // put it back, because we don't know what to do with this value yet
                                    self.intermediate_left_map.insert(k, v);
                                }
                                HashMapValue::Modified(left_byte_record, right_byte_record) => {
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
                                    let left_byte_record_line = left_byte_record
                                        .position()
                                        .expect("a record position")
                                        .line();
                                    let right_byte_record_line = right_byte_record
                                        .position()
                                        .expect("a record position")
                                        .line();
                                    self.buf.push_back(Ok(DiffByteRecord::Modify {
                                        add: ByteRecordLineInfo::new(
                                            right_byte_record,
                                            right_byte_record_line,
                                        ),
                                        delete: ByteRecordLineInfo::new(
                                            left_byte_record,
                                            left_byte_record_line,
                                        ),
                                        field_indices: fields_modified,
                                    }));
                                }
                            }
                        }
                        std::mem::swap(
                            &mut self.intermediate_left_map,
                            &mut self.csv_records_left_map,
                        );
                        if !self.buf.is_empty() {
                            break;
                        }
                    }
                }
                CsvLeftRightParseResult::Right(CsvByteRecordWithHash {
                    byte_record: Err(e),
                    ..
                }) => {
                    self.buf.push_back(Err(e));
                    break;
                }
            }
        }

        if !self.buf.is_empty() {
            return self.buf.pop_front();
        }

        let iter_left_map = self
            .csv_records_left_map_iter
            .get_or_insert(std::mem::take(&mut self.csv_records_left_map).into_iter());

        let mut iter_left_map =
            iter_left_map.skip_while(|(_, v)| matches!(v, HashMapValue::Equal(_, _)));
        match iter_left_map.next() {
            Some((_, HashMapValue::Initial(_hash, byte_record))) => {
                let line = byte_record.position().expect("a record position").line();
                return Some(Ok(DiffByteRecord::Delete(ByteRecordLineInfo::new(
                    byte_record,
                    line,
                ))));
            }
            Some((_, HashMapValue::Modified(left_byte_record, right_byte_record))) => {
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
                let left_byte_record_line = left_byte_record
                    .position()
                    .expect("a record position")
                    .line();
                let right_byte_record_line = right_byte_record
                    .position()
                    .expect("a record position")
                    .line();
                return Some(Ok(DiffByteRecord::Modify {
                    add: ByteRecordLineInfo::new(right_byte_record, right_byte_record_line),
                    delete: ByteRecordLineInfo::new(left_byte_record, left_byte_record_line),
                    field_indices: fields_modified,
                }));
            }
            _ => (),
        }

        let iter_right_map = self
            .csv_records_right_map_iter
            .get_or_insert(std::mem::take(&mut self.csv_records_right_map).into_iter());

        let mut iter_right_map =
            iter_right_map.skip_while(|(_, v)| matches!(v, HashMapValue::Equal(_, _)));
        match iter_right_map.next() {
            Some((_, HashMapValue::Initial(_hash, byte_record))) => {
                let line = byte_record.position().expect("a record position").line();
                return Some(Ok(DiffByteRecord::Add(ByteRecordLineInfo::new(
                    byte_record,
                    line,
                ))));
            }
            Some((_, HashMapValue::Modified(left_byte_record, right_byte_record))) => {
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
                let left_byte_record_line = left_byte_record
                    .position()
                    .expect("a record position")
                    .line();
                let right_byte_record_line = right_byte_record
                    .position()
                    .expect("a record position")
                    .line();
                return Some(Ok(DiffByteRecord::Modify {
                    add: ByteRecordLineInfo::new(right_byte_record, right_byte_record_line),
                    delete: ByteRecordLineInfo::new(left_byte_record, left_byte_record_line),
                    field_indices: fields_modified,
                }));
            }
            _ => (),
        }
        None
    }
}

impl TryFrom<DiffByteRecordsIterator> for DiffByteRecords {
    type Error = csv::Error;

    fn try_from(value: DiffByteRecordsIterator) -> Result<Self, Self::Error> {
        Ok(DiffByteRecords(value.collect::<csv::Result<_>>()?))
    }
}
