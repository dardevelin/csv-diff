use crate::diff_row::{DiffRow, LineNum, RecordLineInfo};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::iter::FromIterator;

#[derive(Debug, PartialEq)]
pub struct CsvDiff {
    primary_key_columns: HashSet<usize>,
}

#[derive(Debug, PartialEq)]
pub enum DiffResult {
    Equal,
    Different { diff_records: DiffRecords },
}

impl DiffResult {
    pub fn sort_by_line(&mut self) -> Option<&mut DiffRecords> {
        match self {
            Self::Different { diff_records } => {
                diff_records.sort_by_line();
                Some(diff_records)
            }
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DiffRecords(Vec<DiffRow>);

impl DiffRecords {
    pub fn sort_by_line(&mut self) {
        self.0.sort_by(|a, b| match (a.line_num(), b.line_num()) {
            (LineNum::OneSide(line_num_a), LineNum::OneSide(line_num_b)) => {
                line_num_a.cmp(&line_num_b)
            }
            (
                LineNum::OneSide(line_num_a),
                LineNum::BothSides {
                    for_deleted,
                    for_added,
                },
            ) => line_num_a.cmp(if for_deleted < for_added {
                &for_deleted
            } else {
                &for_added
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
            .cmp(&line_num_b),
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
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CsvRowKey {
    key: Vec<Vec<u8>>,
}

impl CsvRowKey {
    pub fn new() -> Self {
        Self { key: Vec::new() }
    }

    pub fn push_key_column(&mut self, key_column: Vec<u8>) {
        self.key.push(key_column);
    }
}

impl From<Vec<Vec<u8>>> for CsvRowKey {
    fn from(csv_row_key_vec: Vec<Vec<u8>>) -> Self {
        Self {
            key: csv_row_key_vec,
        }
    }
}

struct KeyByteRecord<'a, 'b> {
    key_idx: &'a HashSet<usize>,
    byte_record: &'b csv::ByteRecord,
}

impl From<KeyByteRecord<'_, '_>> for CsvRowKey {
    fn from(key_byte_record: KeyByteRecord<'_, '_>) -> Self {
        let mut row_key = Vec::new();
        for idx in key_byte_record.key_idx.iter() {
            if let Some(field) = key_byte_record.byte_record.get(*idx) {
                let slice = Vec::from(field);
                row_key.push(slice);
            }
        }
        CsvRowKey::from(row_key)
    }
}

pub struct CsvDiffBuilder {
    primary_key_columns: HashSet<usize>,
}

impl CsvDiffBuilder {
    pub fn primary_key_columns(self, columns: impl IntoIterator<Item = usize>) -> Self {
        Self {
            primary_key_columns: HashSet::from_iter(columns),
            ..self
        }
    }
    pub fn build(self) -> CsvDiff {
        CsvDiff {
            primary_key_columns: self.primary_key_columns,
        }
    }
}

impl CsvDiff {
    pub fn new() -> Self {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
        };
        instance.primary_key_columns.insert(0);
        // TODO: when more than one primary key column is specified, we have to sort and dedup
        instance
    }

    pub fn builder() -> CsvDiffBuilder {
        CsvDiffBuilder {
            primary_key_columns: HashSet::new(),
        }
    }

    pub fn diff<R: Read + Send>(&self, csv_left: R, csv_right: R) -> csv::Result<DiffResult> {
        use rayon::prelude::*;
        use std::sync::mpsc::{channel, Receiver, Sender};

        let (sender_right, receiver) = channel();
        let sender_left = sender_right.clone();
        rayon::scope(move |s| {
            s.spawn(move |_s1| {
                let csv_reader_right = csv::Reader::from_reader(csv_right);
                let csv_records_right = csv_reader_right.into_byte_records();
                // TODO: do proper error handling
                csv_records_right.for_each(|record| {
                    sender_right
                        .send(CsvLeftRightParseResult::Right(record))
                        .unwrap();
                });
            });
            s.spawn(move |_s2| {
                let csv_reader_left = csv::Reader::from_reader(csv_left);
                let csv_records_left = csv_reader_left.into_byte_records();
                // TODO: do proper error handling
                csv_records_left.for_each(|record| {
                    sender_left
                        .send(CsvLeftRightParseResult::Left(record))
                        .unwrap();
                });
            });
        });

        let mut csv_records_right_map = ByteRecordMap::new(&self.primary_key_columns);
        let mut csv_records_left_map = ByteRecordMap::new(&self.primary_key_columns);

        let mut diff_records = Vec::new();
        for record_left_right in receiver {
            match record_left_right {
                CsvLeftRightParseResult::Left(left_record_res) => {
                    let byte_record_left = left_record_res.unwrap();
                    let csv_row_key_left = CsvRowKey::from(KeyByteRecord {
                        key_idx: &self.primary_key_columns,
                        byte_record: &byte_record_left,
                    });
                    match csv_records_right_map.remove(&csv_row_key_left) {
                        Some(byte_record_right) => {
                            let line_left = byte_record_left
                                .position()
                                .expect("There should be line info given.")
                                .line();
                            //we have a modification in our csv line or they are equal
                            let fields_modified = byte_record_left
                                .iter()
                                .enumerate()
                                .zip(byte_record_right.iter())
                                .fold(
                                    HashSet::new(),
                                    |mut acc, ((idx, field_left), field_right)| {
                                        if field_left != field_right {
                                            acc.insert(idx);
                                        }
                                        acc
                                    },
                                );
                            if !fields_modified.is_empty() {
                                let line_right = byte_record_right
                                    .position()
                                    .expect("There should be line info given.")
                                    .line();
                                diff_records.push(DiffRow::Modified {
                                    deleted: RecordLineInfo::new(byte_record_left, line_left),
                                    added: RecordLineInfo::new(byte_record_right, line_right),
                                    field_indices: fields_modified,
                                });
                            }
                        }
                        None => {
                            csv_records_left_map.insert(byte_record_left);
                        }
                    }
                }
                CsvLeftRightParseResult::Right(right_record_res) => {
                    let byte_record_right = right_record_res.unwrap();
                    let csv_row_key_right = CsvRowKey::from(KeyByteRecord {
                        key_idx: &self.primary_key_columns,
                        byte_record: &byte_record_right,
                    });
                    match csv_records_left_map.remove(&csv_row_key_right) {
                        Some(byte_record_left) => {
                            let line_right = byte_record_right
                                .position()
                                .expect("There should be line info given.")
                                .line();
                            //we have a modification in our csv line or they are equal
                            let fields_modified = byte_record_left
                                .iter()
                                .enumerate()
                                .zip(byte_record_right.iter())
                                .fold(
                                    HashSet::new(),
                                    |mut acc, ((idx, field_left), field_right)| {
                                        if field_left != field_right {
                                            acc.insert(idx);
                                        }
                                        acc
                                    },
                                );
                            if !fields_modified.is_empty() {
                                let line_left = byte_record_left
                                    .position()
                                    .expect("There should be line info given.")
                                    .line();
                                diff_records.push(DiffRow::Modified {
                                    deleted: RecordLineInfo::new(byte_record_left, line_left),
                                    added: RecordLineInfo::new(byte_record_right, line_right),
                                    field_indices: fields_modified,
                                });
                            }
                        }
                        None => {
                            csv_records_right_map.insert(byte_record_right);
                        }
                    }
                }
            }
        }

        diff_records.reserve(csv_records_left_map.map.len() + csv_records_right_map.map.len());

        diff_records.par_extend(csv_records_left_map.map.into_par_iter().map(|(_, byte_record_left)| {
            let line_left = byte_record_left
                .position()
                .expect("There should be line info given.")
                .line();
            DiffRow::Deleted(RecordLineInfo::new(
                byte_record_left,
                line_left,
            ))
        }));

        diff_records.par_extend(csv_records_right_map.map.into_par_iter().map(|(_, byte_record_right)| {
            let line_right = byte_record_right
                .position()
                .expect("There should be line info given.")
                .line();
            DiffRow::Added(RecordLineInfo::new(
                byte_record_right,
                line_right,
            ))
        }));

        Ok(if diff_records.is_empty() {
            DiffResult::Equal
        } else {
            DiffResult::Different {
                diff_records: DiffRecords(diff_records),
            }
        })
    }
}

enum CsvLeftRightParseResult {
    Left(csv::Result<csv::ByteRecord>),
    Right(csv::Result<csv::ByteRecord>),
}

struct ByteRecordMap<'a> {
    key_idx: &'a HashSet<usize>,
    map: HashMap<CsvRowKey, csv::ByteRecord>,
}

impl<'a> ByteRecordMap<'a> {
    pub fn new(key_idx: &'a HashSet<usize>) -> Self {
        Self {
            key_idx,
            map: HashMap::new(),
        }
    }

    pub fn with_capacity(key_idx: &'a HashSet<usize>, capacity: usize) -> Self {
        Self {
            key_idx,
            map: HashMap::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, byte_record: csv::ByteRecord) {
        let mut row_key = Vec::new();
        for idx in self.key_idx.iter() {
            if let Some(field) = byte_record.get(*idx) {
                let slice = Vec::from(field);
                row_key.push(slice);
            }
        }

        self.map.insert(CsvRowKey::from(row_key), byte_record);
    }

    pub fn get(&self, csv_row_key: &CsvRowKey) -> Option<&csv::ByteRecord> {
        self.map.get(csv_row_key)
    }

    pub fn remove(&mut self, csv_row_key: &CsvRowKey) -> Option<csv::ByteRecord> {
        self.map.remove(csv_row_key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::iter::FromIterator;

    #[test]
    fn diff_one_line_with_header_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_crazy_characters_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";
        let csv_right = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_added_one_line() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "b", "c"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_deleted_one_line() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        ";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "b", "c"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_modified_one_field() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_modified_all_fields() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,c,d";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "c", "d"]), 2),
                field_indices: HashSet::from_iter(vec![1, 2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_different_order_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        d,e,f\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_start() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        x,y,z\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_middle() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                3,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_end() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                4,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_start() {
        let csv_left = "\
                        header1,header2,header3\n\
                        x,y,z\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_middle() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                3,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_end() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                4,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_start() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,x,c\n\
                        d,e,f\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 2),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_start_different_order() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        d,e,f\n\
                        a,x,c\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 3),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_middle() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,x,f\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 3),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_middle_different_order() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        d,x,f\n\
                        a,b,c\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 2),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_end() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,x,z";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 4),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_end_different_order() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        x,x,z\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 2),
                field_indices: HashSet::from_iter(vec![1]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_and_deleted_same_lines() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        g,h,i\n\
                        x,y,z";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["g", "h", "i"]),
                    3,
                )),
            ]),
        };
        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_and_deleted_different_lines() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z\n\
                        g,h,i";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["g", "h", "i"]),
                    4,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_modified_and_deleted() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        g,h,i\n\
                        a,b,d\n\
                        x,y,z";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 3),
                    field_indices: HashSet::from_iter(vec![2]),
                },
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["g", "h", "i"]),
                    2,
                )),
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_multiple() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i\n\
                        j,k,l\n\
                        m,n,o";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["j", "k", "l"]),
                    5,
                )),
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["m", "n", "o"]),
                    6,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_deleted_multiple() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["g", "h", "i"]),
                    4,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_modified_multiple() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,x\n\
                        d,e,f\n\
                        g,h,x";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                    field_indices: HashSet::from_iter(vec![2]),
                },
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "i"]), 4),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "x"]), 4),
                    field_indices: HashSet::from_iter(vec![2]),
                },
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_added_modified_deleted_multiple() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i\n\
                        j,k,l\n\
                        m,n,o";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,x\n\
                        p,q,r\n\
                        m,n,o\n\
                        x,y,z\n\
                        j,k,x\n";

        let mut diff_res_actual = CsvDiff::new()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                    field_indices: HashSet::from_iter(vec![2]),
                },
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["p", "q", "r"]),
                    3,
                )),
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["g", "h", "i"]),
                    4,
                )),
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "l"]), 5),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "x"]), 6),
                    field_indices: HashSet::from_iter(vec![2]),
                },
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["x", "y", "z"]),
                    5,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_multiple_lines_with_header_combined_key_added_deleted_modified() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i\n\
                        m,n,o";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,x\n\
                        g,h,i\n\
                        d,f,f\n\
                        m,n,o";

        let mut diff_res_actual = CsvDiff::builder()
            .primary_key_columns(vec![0, 1])
            .build()
            .diff(csv_left.as_bytes(), csv_right.as_bytes())
            .unwrap();
        let mut diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![
                DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                    field_indices: HashSet::from_iter(vec![2]),
                },
                DiffRow::Deleted(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "e", "f"]),
                    3,
                )),
                DiffRow::Added(RecordLineInfo::new(
                    csv::ByteRecord::from(vec!["d", "f", "f"]),
                    4,
                )),
            ]),
        };

        let _ = diff_res_actual.sort_by_line().unwrap();
        let _ = diff_res_expected.sort_by_line().unwrap();
        assert_eq!(diff_res_actual, diff_res_expected);
    }
}
