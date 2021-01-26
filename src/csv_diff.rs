use crate::csv_hash_comparer::CsvHashComparer;
use crate::csv_parser_hasher::*;
use crate::diff_result::DiffResult;
use crate::diff_row::{DiffRow, LineNum, RecordLineInfo};
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::io::{Read, Seek};
use std::iter::FromIterator;
use std::iter::Iterator;

#[derive(Debug, PartialEq)]
pub struct CsvDiff {
    primary_key_columns: HashSet<usize>,
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

    // TODO: we have to see, whether `convert::AsRef` is a problem here
    // maybe we instead need `Seek`
    pub fn diff<R: Read + std::convert::AsRef<[u8]> + Send>(
        &self,
        csv_left: R,
        csv_right: R,
    ) -> csv::Result<DiffResult> {
        use crossbeam_channel::unbounded;

        let (sender_total_lines_right, receiver_total_lines_right) = unbounded();
        let (sender_total_lines_left, receiver_total_lines_left) = unbounded();
        let (sender_csv_reader_right, receiver_csv_reader_right) = unbounded();
        let (sender_csv_reader_left, receiver_csv_reader_left) = unbounded();
        let (sender_right, receiver) = unbounded();
        let sender_left = sender_right.clone();
        rayon::scope(move |s| {
            s.spawn(move |_s1| {
                let mut csv_parser_hasher: CsvParserHasherSender<CsvLeftRightParseResult> =
                    CsvParserHasherSender::new(sender_left, sender_total_lines_left);
                sender_csv_reader_left
                    .send(csv_parser_hasher.parse_and_hash::<R, CsvParseResultLeft>(
                        csv_left,
                        &self.primary_key_columns,
                    ))
                    .unwrap();
            });
            s.spawn(move |_s2| {
                let mut csv_parser_hasher: CsvParserHasherSender<CsvLeftRightParseResult> =
                    CsvParserHasherSender::new(sender_right, sender_total_lines_right);
                sender_csv_reader_right
                    .send(csv_parser_hasher.parse_and_hash::<R, CsvParseResultRight>(
                        csv_right,
                        &self.primary_key_columns,
                    ))
                    .unwrap();
            });
        });

        let (total_lines_right, total_lines_left) = (
            receiver_total_lines_right.recv().unwrap(),
            receiver_total_lines_left.recv().unwrap(),
        );
        let (csv_reader_right_for_diff_seek, csv_reader_left_for_diff_seek) = (
            receiver_csv_reader_right.recv().unwrap(),
            receiver_csv_reader_left.recv().unwrap(),
        );
        let max_capacity_for_hash_map_right =
            if total_lines_right / 100 < total_lines_right && total_lines_right / 100 == 0 {
                total_lines_right
            } else {
                total_lines_right / 100
            } as usize;
        let max_capacity_for_hash_map_left =
            if total_lines_left / 100 < total_lines_left && total_lines_left / 100 == 0 {
                total_lines_left
            } else {
                total_lines_left / 100
            } as usize;

        let mut csv_hash_comparer = CsvHashComparer::with_capacity_and_reader(
            max_capacity_for_hash_map_left,
            max_capacity_for_hash_map_right,
            csv_reader_left_for_diff_seek,
            csv_reader_right_for_diff_seek,
        );
        csv_hash_comparer.compare_csv_left_right_parse_result(receiver)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::diff_result::*;
    use pretty_assertions::assert_eq;
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
