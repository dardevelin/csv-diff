use crate::csv_hash_task_spawner::CsvHashTaskSenders;
#[cfg(feature = "crossbeam-utils")]
use crate::csv_hash_task_spawner::{
    CsvHashTaskSpawnerBuilderCrossbeam, CsvHashTaskSpawnerCrossbeam,
};
#[cfg(feature = "rayon-threads")]
use crate::csv_hash_task_spawner::{CsvHashTaskSpawnerBuilderRayon, CsvHashTaskSpawnerRayon};
use crate::csv_parse_result::CsvLeftRightParseResult;
use crate::csv_parser_hasher::*;
use crate::diff_row::{DiffRow, LineNum, RecordLineInfo};
use crate::thread_scope_strategy::*;
use crate::{csv_hash_comparer::CsvHashComparer, csv_hash_task_spawner::CsvHashTaskSpawner};
use crate::{csv_hash_task_spawner::CsvHashTaskSpawnerBuilder, diff_result::DiffResult};
use crossbeam_channel::{Receiver, Sender};
use csv::Reader;
use std::hash::Hasher;
use std::io::{Read, Seek};
use std::iter::FromIterator;
use std::iter::Iterator;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};
use thiserror::Error;

#[derive(Debug)]
pub struct CsvDiff<T: CsvHashTaskSpawner> {
    primary_key_columns: HashSet<usize>,
    hash_task_spawner: T,
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

#[derive(Debug)]
pub struct CsvDiffBuilder<T: CsvHashTaskSpawner> {
    primary_key_columns: HashSet<usize>,
    hash_task_spawner: T,
}

impl<T> CsvDiffBuilder<T>
where
    T: CsvHashTaskSpawner,
{
    pub fn new<B>(csv_hash_task_spawner_builder: B) -> Self
    where
        B: CsvHashTaskSpawnerBuilder<T>,
    {
        Self {
            primary_key_columns: HashSet::from_iter(std::iter::once(0)),
            hash_task_spawner: csv_hash_task_spawner_builder.build(),
        }
    }

    pub fn primary_key_columns(mut self, columns: impl IntoIterator<Item = usize>) -> Self {
        self.primary_key_columns = HashSet::from_iter(columns);
        self
    }

    pub fn build(self) -> Result<CsvDiff<T>, CsvDiffBuilderError> {
        if !self.primary_key_columns.is_empty() {
            Ok(CsvDiff {
                primary_key_columns: self.primary_key_columns,
                hash_task_spawner: self.hash_task_spawner,
            })
        } else {
            Err(CsvDiffBuilderError::NoPrimaryKeyColumns)
        }
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum CsvDiffBuilderError {
    #[error("No primary key columns have been specified. You need to provide at least one column index.")]
    NoPrimaryKeyColumns,
}

#[cfg(feature = "rayon-threads")]
impl CsvDiff<CsvHashTaskSpawnerRayon> {
    pub fn new() -> Self {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: CsvHashTaskSpawnerRayon::new(RayonScope::new(
                rayon::ThreadPoolBuilder::new().build().unwrap(),
            )),
        };
        instance.primary_key_columns.insert(0);
        instance
    }

    pub fn with_rayon_thread_pool(thread_pool: rayon::ThreadPool) -> Self {
        Self::with_task_spawner_builder(CsvHashTaskSpawnerBuilderRayon::new(thread_pool))
    }
}

#[cfg(feature = "crossbeam-utils")]
impl CsvDiff<CsvHashTaskSpawnerCrossbeam> {
    pub fn new() -> Self {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: CsvHashTaskSpawnerCrossbeam::new(CrossbeamScope::new()),
        };
        instance.primary_key_columns.insert(0);
        instance
    }
}

impl<T> CsvDiff<T>
where
    T: CsvHashTaskSpawner,
{
    pub fn with_task_spawner_builder<B>(csv_hash_task_spawner_builder: B) -> Self
    where
        B: CsvHashTaskSpawnerBuilder<T>,
    {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: csv_hash_task_spawner_builder.build(),
        };
        instance.primary_key_columns.insert(0);
        instance
    }

    //TODO: maybe rename this to `diff_then_seek`, so that we can have a `diff`
    // method in the future that does not require `Seek`
    pub fn diff<R>(&self, csv_left: R, csv_right: R) -> csv::Result<DiffResult>
    where
        R: Read + Seek + Send,
    {
        use crossbeam_channel::unbounded;

        let (sender_total_lines_right, receiver_total_lines_right) = unbounded();
        let (sender_total_lines_left, receiver_total_lines_left) = unbounded();
        let (sender_csv_reader_right, receiver_csv_reader_right) = unbounded();
        let (sender_csv_reader_left, receiver_csv_reader_left) = unbounded();
        let (sender_right, receiver) = unbounded();
        let sender_left = sender_right.clone();

        self.hash_task_spawner.spawn_hashing_tasks_and_send_result(
            CsvHashTaskSenders::new(
                sender_left,
                sender_total_lines_left,
                sender_csv_reader_left,
                csv_left,
            ),
            CsvHashTaskSenders::new(
                sender_right,
                sender_total_lines_right,
                sender_csv_reader_right,
                csv_right,
            ),
            &self.primary_key_columns,
        );

        self.recv_hashes_and_compare(
            receiver_total_lines_left,
            receiver_total_lines_right,
            receiver_csv_reader_left,
            receiver_csv_reader_right,
            receiver,
        )
    }

    fn recv_hashes_and_compare<R>(
        &self,
        receiver_total_lines_left: Receiver<u64>,
        receiver_total_lines_right: Receiver<u64>,
        receiver_csv_reader_left: Receiver<Reader<R>>,
        receiver_csv_reader_right: Receiver<Reader<R>>,
        receiver: Receiver<StackVec<CsvLeftRightParseResult>>,
    ) -> csv::Result<DiffResult>
    where
        R: Read + Seek + Send,
    {
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
    #[cfg(feature = "rayon-threads")]
    use crate::csv_hash_task_spawner::CsvHashTaskSpawnerBuilderRayon;
    use crate::diff_result::DiffRecords;
    use pretty_assertions::assert_eq;
    use std::{io::Cursor, iter::FromIterator};

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_empty_no_diff() {
        let csv_left = "";
        let csv_right = "";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_empty_with_header_no_diff() {
        let csv_left = "header1,header2,header3";
        let csv_right = "header1,header2,header3";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_crazy_characters_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";
        let csv_right = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_crazy_characters_modified() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";
        let csv_right = "\
                        header1,header2,header3\n\
                        ༼,౪,༼";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༽"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༼"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_added_one_line() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "b", "c"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_deleted_one_line() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        ";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "b", "c"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_modified_one_field() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_modified_all_fields() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,c,d";

        let diff_res_actual = CsvDiff::new()
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                3,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Added(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                4,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                2,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                3,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Deleted(RecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                4,
            ))]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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
        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
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
            .diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_primary_key_columns_is_no_primary_key_columns_err() {
        let expected = CsvDiffBuilderError::NoPrimaryKeyColumns;
        let actual = CsvDiffBuilder::new(CsvHashTaskSpawnerBuilderRayon::new(
            rayon::ThreadPoolBuilder::new().build().unwrap(),
        ))
        .primary_key_columns(std::iter::empty())
        .build();

        assert!(actual.is_err());
        assert_eq!(expected, actual.unwrap_err());
        assert_eq!(expected.to_string(), "No primary key columns have been specified. You need to provide at least one column index.");
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_specified_primary_key_columns_is_ok() {
        // it is ok, because it gets a sensible default value
        assert!(CsvDiffBuilder::new(CsvHashTaskSpawnerBuilderRayon::new(
            rayon::ThreadPoolBuilder::new().build().unwrap()
        ),)
        .build()
        .is_ok());
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_combined_key_added_deleted_modified(
    ) -> Result<(), CsvDiffBuilderError> {
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

        let mut diff_res_actual = CsvDiffBuilder::<CsvHashTaskSpawnerRayon>::new(
            CsvHashTaskSpawnerBuilderRayon::new(rayon::ThreadPoolBuilder::new().build().unwrap()),
        )
        .primary_key_columns(vec![0, 1])
        .build()?
        .diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
        Ok(())
    }

    #[cfg(feature = "crossbeam-utils")]
    #[test]
    // TODO: this is our only test for the "crossbeam-utils" feature;
    // we should write a macro, so that we can reuse test code for both "rayon" and "crossbeam-utils"
    fn diff_multiple_lines_with_header_combined_key_added_deleted_modified(
    ) -> Result<(), CsvDiffBuilderError> {
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

        let mut diff_res_actual = CsvDiffBuilder::<CsvHashTaskSpawnerCrossbeam>::new(
            CsvHashTaskSpawnerBuilderCrossbeam::new(),
        )
        .primary_key_columns(vec![0, 1])
        .build()?
        .diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )
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

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
        Ok(())
    }
}
