use crate::csv_hash_task_spawner::CsvHashTaskSenders;
use crate::csv_hash_task_spawner::CsvHashTaskSpawnerBuilder;
#[cfg(feature = "crossbeam-utils")]
use crate::csv_hash_task_spawner::{
    CsvHashTaskSpawnerBuilderCrossbeam, CsvHashTaskSpawnerCrossbeam,
};
#[cfg(feature = "rayon-threads")]
use crate::csv_hash_task_spawner::{CsvHashTaskSpawnerBuilderRayon, CsvHashTaskSpawnerRayon};
use crate::csv_parse_result::CsvLeftRightParseResult;
use crate::csv_parser_hasher::*;
use crate::diff_result::DiffByteRecords;
use crate::thread_scope_strategy::*;
use crate::{csv_hash_comparer::CsvHashComparer, csv_hash_task_spawner::CsvHashTaskSpawner};
use crossbeam_channel::Receiver;
use csv::Reader;
use std::io::{Read, Seek};
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::{collections::HashSet, iter::Iterator};
use thiserror::Error;

#[derive(Debug)]
pub struct CsvDiff<T: CsvHashTaskSpawner> {
    primary_key_columns: HashSet<usize>,
    hash_task_spawner: T,
}

#[derive(Debug)]
pub struct CsvDiffBuilder<'tp, T: CsvHashTaskSpawner> {
    primary_key_columns: HashSet<usize>,
    #[cfg(feature = "rayon-threads")]
    hash_task_spawner: Option<CsvHashTaskSpawnerRayon<'tp>>,
    #[cfg(feature = "rayon-threads")]
    _phantom: PhantomData<T>,
    #[cfg(not(feature = "rayon-threads"))]
    _phantom: PhantomData<&'tp T>,
    #[cfg(not(feature = "rayon-threads"))]
    hash_task_spawner: T,
}

impl<'tp, T> CsvDiffBuilder<'tp, T>
where
    T: CsvHashTaskSpawner,
{
    #[cfg(not(feature = "rayon-threads"))]
    pub fn new<B>(csv_hash_task_spawner_builder: B) -> Self
    where
        B: CsvHashTaskSpawnerBuilder<T>,
    {
        Self {
            primary_key_columns: std::iter::once(0).collect(),
            hash_task_spawner: csv_hash_task_spawner_builder.build(),
            _phantom: PhantomData::default(),
        }
    }

    pub fn primary_key_columns(mut self, columns: impl IntoIterator<Item = usize>) -> Self {
        self.primary_key_columns = HashSet::from_iter(columns);
        self
    }

    #[cfg(not(feature = "rayon-threads"))]
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

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvDiffBuilder<'tp, CsvHashTaskSpawnerRayon<'tp>> {
    pub fn new() -> Self {
        Self {
            primary_key_columns: std::iter::once(0).collect(),
            hash_task_spawner: None,
            _phantom: PhantomData::default(),
        }
    }

    pub fn rayon_thread_pool(mut self, thread_pool: &'tp rayon::ThreadPool) -> Self {
        self.hash_task_spawner = Some(CsvHashTaskSpawnerBuilderRayon::new(thread_pool).build());
        self
    }

    #[cfg(feature = "rayon-threads")]
    pub fn build(self) -> Result<CsvDiff<CsvHashTaskSpawnerRayon<'tp>>, CsvDiffBuilderError> {
        if !self.primary_key_columns.is_empty() {
            Ok(CsvDiff {
                primary_key_columns: self.primary_key_columns,
                hash_task_spawner: match self.hash_task_spawner {
                    Some(x) => x,
                    None => CsvHashTaskSpawnerRayon::new(RayonScope::with_thread_pool_owned(
                        rayon::ThreadPoolBuilder::new().build()?,
                    )),
                },
            })
        } else {
            Err(CsvDiffBuilderError::NoPrimaryKeyColumns)
        }
    }
}

#[derive(Debug, Error)]
pub enum CsvDiffBuilderError {
    #[error("No primary key columns have been specified. You need to provide at least one column index.")]
    NoPrimaryKeyColumns,
    #[cfg(feature = "rayon-threads")]
    #[error("An error occured when trying to build the rayon thread pool.")]
    ThreadPoolBuildError(#[from] rayon::ThreadPoolBuildError),
}

#[derive(Debug, Error)]
#[cfg(feature = "rayon-threads")]
pub enum CsvDiffNewError {
    #[error("An error occured when trying to build the rayon thread pool.")]
    ThreadPoolBuildError(#[from] rayon::ThreadPoolBuildError),
}

#[cfg(feature = "rayon-threads")]
impl CsvDiff<CsvHashTaskSpawnerRayon<'_>> {
    pub fn new() -> Result<Self, CsvDiffNewError> {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: CsvHashTaskSpawnerRayon::new(RayonScope::with_thread_pool_owned(
                rayon::ThreadPoolBuilder::new().build()?,
            )),
        };
        instance.primary_key_columns.insert(0);
        Ok(instance)
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
    pub fn diff_bytes<R>(&self, csv_left: R, csv_right: R) -> csv::Result<DiffByteRecords>
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
        receiver_csv_reader_left: Receiver<csv::Result<Reader<R>>>,
        receiver_csv_reader_right: Receiver<csv::Result<Reader<R>>>,
        receiver: Receiver<StackVec<CsvLeftRightParseResult>>,
    ) -> csv::Result<DiffByteRecords>
    where
        R: Read + Seek + Send,
    {
        let (total_lines_right, total_lines_left) = (
            receiver_total_lines_right.recv().unwrap_or_default(),
            receiver_total_lines_left.recv().unwrap_or_default(),
        );
        let (csv_reader_right_for_diff_seek, csv_reader_left_for_diff_seek) = (
            receiver_csv_reader_right.recv().unwrap()?,
            receiver_csv_reader_left.recv().unwrap()?,
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
    use crate::diff_result::DiffByteRecords;
    use crate::diff_row::{ByteRecordLineInfo, DiffByteRow};
    use pretty_assertions::assert_eq;
    use std::{error::Error, io::Cursor, iter::FromIterator};

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_empty_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "";
        let csv_right = "";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_empty_with_header_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "header1,header2,header3";
        let csv_right = "header1,header2,header3";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_crazy_characters_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";
        let csv_right = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_crazy_characters_modified() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        ༼,౪,༽";
        let csv_right = "\
                        header1,header2,header3\n\
                        ༼,౪,༼";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༽"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༼"]), 2),
            field_indices: HashSet::from_iter(vec![2]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_added_one_line() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        ";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["a", "b", "c"]),
            2,
        ))]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_deleted_one_line() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        ";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Delete(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
        )]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_modified_one_field() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: HashSet::from_iter(vec![2]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_modified_all_fields() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,c,d";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "c", "d"]), 2),
            field_indices: HashSet::from_iter(vec![1, 2]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_different_order_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        d,e,f\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_start() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        x,y,z\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            2,
        ))]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_middle() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            3,
        ))]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_one_line_at_end() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            4,
        ))]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_start() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        x,y,z\n\
                        a,b,c\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Delete(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 2),
        )]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_middle() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z\n\
                        d,e,f";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Delete(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 3),
        )]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_deleted_one_line_at_end() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f";

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Delete(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
        )]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_start() -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 2),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_start_different_order(
    ) -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 3),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_middle() -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 3),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_middle_different_order(
    ) -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 2),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_end() -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 4),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_one_line_at_end_different_order(
    ) -> Result<(), Box<dyn Error>> {
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

        let diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 2),
            field_indices: HashSet::from_iter(vec![1]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_and_deleted_same_lines() -> Result<(), Box<dyn Error>>
    {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                3,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_and_deleted_different_lines(
    ) -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_modified_and_deleted() -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 3),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                2,
            )),
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_multiple() -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["j", "k", "l"]),
                5,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["m", "n", "o"]),
                6,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_deleted_multiple() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        d,e,f\n\
                        g,h,i";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_multiple() -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "i"]), 4),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "x"]), 4),
                field_indices: HashSet::from_iter(vec![2]),
            },
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_added_modified_deleted_multiple(
    ) -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiff::new()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["p", "q", "r"]),
                3,
            )),
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "l"]), 5),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "x"]), 6),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                5,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_primary_key_columns_is_no_primary_key_columns_err(
    ) -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let expected = CsvDiffBuilderError::NoPrimaryKeyColumns;
        let actual = CsvDiffBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .primary_key_columns(std::iter::empty())
            .build();

        assert!(actual.is_err());
        assert!(matches!(
            actual,
            Err(CsvDiffBuilderError::NoPrimaryKeyColumns)
        ));
        assert_eq!(expected.to_string(), "No primary key columns have been specified. You need to provide at least one column index.");
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_specified_primary_key_columns_is_ok() -> Result<(), Box<dyn Error>> {
        // it is ok, because it gets a sensible default value
        assert!(CsvDiffBuilder::new()
            .rayon_thread_pool(&rayon::ThreadPoolBuilder::new().build()?)
            .build()
            .is_ok());
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_created_with_existing_thread_pool_works() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = CsvDiffBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .build()?;

        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let diff_res_actual = csv_diff
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRow::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: HashSet::from_iter(vec![2]),
        }]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_combined_key_added_deleted_modified(
    ) -> Result<(), Box<dyn Error>> {
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

        let mut diff_res_actual = CsvDiffBuilder::new()
            .rayon_thread_pool(&rayon::ThreadPoolBuilder::new().build()?)
            .primary_key_columns(vec![0, 1])
            .build()?
            .diff_bytes(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )?;
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "f", "f"]),
                4,
            )),
        ]);

        diff_res_actual.sort_by_line();
        diff_res_expected.sort_by_line();
        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_error_left_has_different_num_of_fields(
    ) -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3,header4\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let diff_res_actual = CsvDiff::new()?.diff_bytes(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        );

        let err_kind = diff_res_actual.map_err(|err| err.into_kind());
        let mut pos_expected = csv::Position::new();
        let pos_expected = pos_expected.set_byte(32).set_line(2).set_record(1);
        match err_kind {
            Err(csv::ErrorKind::UnequalLengths {
                pos: Some(pos),
                expected_len,
                len,
            }) => {
                assert_eq!(pos, *pos_expected);
                assert_eq!(expected_len, 4);
                assert_eq!(len, 3);
            }
            res => panic!("match mismatch: got {:#?}", res),
        }
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_error_right_has_different_num_of_fields(
    ) -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3,header4\n\
                        a,b,d";

        let diff_res_actual = CsvDiff::new()?.diff_bytes(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        );

        let err_kind = diff_res_actual.map_err(|err| err.into_kind());
        let mut pos_expected = csv::Position::new();
        let pos_expected = pos_expected.set_byte(32).set_line(2).set_record(1);
        match err_kind {
            Err(csv::ErrorKind::UnequalLengths {
                pos: Some(pos),
                expected_len,
                len,
            }) => {
                assert_eq!(pos, *pos_expected);
                assert_eq!(expected_len, 4);
                assert_eq!(len, 3);
            }
            res => panic!("match mismatch: got {:#?}", res),
        }
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
        .diff_bytes(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )
        .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRow::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            },
            DiffByteRow::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRow::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "f", "f"]),
                4,
            )),
        ]);

        let diff_actual = diff_res_actual.sort_by_line();
        let diff_expected = diff_res_expected.sort_by_line();
        assert_eq!(diff_actual, diff_expected);
        Ok(())
    }
}
