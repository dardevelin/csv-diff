use crate::csv::Csv;
use crate::csv_hash_comparer::CsvHashComparer;
use crate::csv_hash_receiver_comparer::CsvHashReceiverStreamComparer;
#[cfg(feature = "rayon-threads")]
use crate::csv_hash_task_spawner::CsvHashTaskSpawnerRayon;
use crate::csv_hash_task_spawner::{
    CsvHashTaskLineSenders, CsvHashTaskSenderWithRecycleReceiver, CsvHashTaskSpawner,
    CsvHashTaskSpawnerLocal, CsvHashTaskSpawnerLocalBuilder,
};
#[cfg(feature = "crossbeam-threads")]
use crate::csv_hash_task_spawner::{
    CsvHashTaskSpawnerLocalBuilderCrossbeam, CsvHashTaskSpawnerLocalCrossbeam,
};
#[cfg(feature = "rayon-threads")]
use crate::csv_hash_task_spawner::{
    CsvHashTaskSpawnerLocalBuilderRayon, CsvHashTaskSpawnerLocalRayon,
};
use crate::csv_parse_result::{CsvLeftRightParseResult, RecordHashWithPosition};
use crate::diff_result::{DiffByteRecords, DiffByteRecordsIterator};
use crate::thread_scope_strategy::*;
use crossbeam_channel::Receiver;
use csv::Reader;
use std::cell::RefCell;
use std::io::{Read, Seek};
use std::marker::PhantomData;
use std::{collections::HashSet, iter::Iterator};
use thiserror::Error;

#[derive(Debug)]
pub struct CsvByteDiff<T: CsvHashTaskSpawner> {
    primary_key_columns: HashSet<usize>,
    // TODO: try to find a way to remove interior mutability in `diff` method
    hash_task_spawner: RefCell<Option<T>>,
}

#[cfg(feature = "rayon-threads")]
impl CsvByteDiff<CsvHashTaskSpawnerRayon<'static>> {
    pub fn new() -> Result<Self, CsvDiffNewError> {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: RefCell::new(Some(CsvHashTaskSpawnerRayon::with_thread_pool_owned(
                rayon::ThreadPoolBuilder::new().build()?,
            ))),
        };
        instance.primary_key_columns.insert(0);
        Ok(instance)
    }
}

impl<T> CsvByteDiff<T>
where
    T: CsvHashTaskSpawner,
{
    pub fn diff<R: Read + Send + 'static>(
        &self,
        csv_left: Csv<R>,
        csv_right: Csv<R>,
    ) -> DiffByteRecordsIterator {
        use crossbeam_channel::{bounded, unbounded};

        let (sender_right, receiver) = bounded(10_000);
        let sender_left = sender_right.clone();
        let (sender_right_first_few_lines, receiver_first_few_lines) = bounded(1);
        let sender_left_first_few_lines = sender_right_first_few_lines.clone();

        let (sender_csv_recycle, receiver_csv_recycle) = unbounded();

        let hts = self.hash_task_spawner.take().take();

        let (hash_task_spawner, receiver_diff_byte_record_iter) =
            // TODO: remove unwrap!!!
            hts.unwrap().spawn_hashing_tasks_and_send_result(
                CsvHashTaskSenderWithRecycleReceiver::new(
                    sender_left,
                    sender_left_first_few_lines,
                    csv_left,
                    receiver_csv_recycle.clone()
                ),
                CsvHashTaskSenderWithRecycleReceiver::new(
                    sender_right,
                    sender_right_first_few_lines,
                    csv_right,
                    receiver_csv_recycle
                ),
                CsvHashReceiverStreamComparer::new(receiver, receiver_first_few_lines, sender_csv_recycle),
                self.primary_key_columns.clone(),
            );

        let mut hash_task_spawner_mut = self.hash_task_spawner.borrow_mut();
        *hash_task_spawner_mut = Some(hash_task_spawner);

        receiver_diff_byte_record_iter.recv().unwrap()
    }
}

/// Compare two [CSVs](https://en.wikipedia.org/wiki/Comma-separated_values) with each other.
///
/// `CsvByteDiffLocal` uses scoped threads internally for comparison.
/// By default, it uses [rayon's scoped threads within a rayon thread pool](https://docs.rs/rayon/1.5.0/rayon/struct.ThreadPool.html#method.scope).
/// See also [`rayon_thread_pool`](CsvByteDiffLocalBuilder::rayon_thread_pool) on [`CsvByteDiffLocalBuilder`](CsvByteDiffLocalBuilder)
/// for using an existing [rayon thread-pool](https://docs.rs/rayon/1.5.0/rayon/struct.ThreadPool.html)
/// when creating `CsvByteDiffLocal`.
///
/// # Example: create `CsvByteDiffLocal` with default values and compare two CSVs byte-wise
#[cfg_attr(
    feature = "rayon-threads",
    doc = r##"
```
use csv_diff::{csv_diff::CsvByteDiffLocal, csv::Csv};
use csv_diff::diff_row::{ByteRecordLineInfo, DiffByteRecord};
use std::collections::HashSet;
use std::iter::FromIterator;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
// some csv data with a header, where the first column is a unique id
let csv_data_left = "id,name,kind\n\
                     1,lemon,fruit\n\
                     2,strawberry,fruit";
let csv_data_right = "id,name,kind\n\
                      1,lemon,fruit\n\
                      2,strawberry,nut";

let csv_byte_diff = CsvByteDiffLocal::new()?;

let mut diff_byte_records = csv_byte_diff.diff(
    Csv::with_reader_seek(csv_data_left.as_bytes()),
    Csv::with_reader_seek(csv_data_right.as_bytes()),
)?;

diff_byte_records.sort_by_line();

let diff_byte_rows = diff_byte_records.as_slice();

assert_eq!(
    diff_byte_rows,
    &[DiffByteRecord::Modify {
        delete: ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["2", "strawberry", "fruit"]),
            3
        ),
        add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["2", "strawberry", "nut"]), 3),
        field_indices: vec![2]
    }]
);
Ok(())
# }
```
"##
)]
#[derive(Debug)]
pub struct CsvByteDiffLocal<T: CsvHashTaskSpawnerLocal> {
    primary_key_columns: HashSet<usize>,
    hash_task_spawner: T,
}

/// Create a [`CsvByteDiffLocal`](CsvByteDiffLocal) with configuration options.
/// # Example: create a `CsvByteDiffLocal`, where column 1 and column 3 are treated as a compound primary key.
#[cfg_attr(
    feature = "rayon-threads",
    doc = r##"
```
use csv_diff::{csv_diff::{CsvByteDiffLocal, CsvByteDiffLocalBuilder}, csv::Csv};
use csv_diff::diff_row::{ByteRecordLineInfo, DiffByteRecord};
use std::collections::HashSet;
use std::iter::FromIterator;
# fn main() -> Result<(), Box<dyn std::error::Error>> {
// some csv data with a header, where the first column and third column represent a compound key
let csv_data_left = "\
                    id,name,commit_sha\n\
                    1,lemon,efae52\n\
                    2,strawberry,a33411"; // this csv line is seen as "Deleted" and not "Modified"
                                          // because "id" and "commit_sha" are different and both columns
                                          // _together_ represent the primary key
let csv_data_right = "\
                    id,name,commit_sha\n\
                    1,lemon,efae52\n\
                    2,strawberry,ddef23"; // this csv line is seen as "Added" and not "Modified",
                                          // because "id" and "commit_sha" are different and both columns
                                          // _together_ represent the primary key

let csv_byte_diff = CsvByteDiffLocalBuilder::new()
    .primary_key_columns(vec![0usize, 2])
    .build()?;

let mut diff_byte_records = csv_byte_diff.diff(
    Csv::with_reader_seek(csv_data_left.as_bytes()),
    Csv::with_reader_seek(csv_data_right.as_bytes()),
)?;

diff_byte_records.sort_by_line();

let diff_byte_rows = diff_byte_records.as_slice();

assert_eq!(
    diff_byte_rows,
    &[
        DiffByteRecord::Delete(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["2", "strawberry", "a33411"]),
            3
        ),),
        DiffByteRecord::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["2", "strawberry", "ddef23"]),
            3
        ),)
    ]
);
Ok(())
# }
```
"##
)]
#[derive(Debug)]
pub struct CsvByteDiffLocalBuilder<'tp, T: CsvHashTaskSpawnerLocal> {
    primary_key_columns: HashSet<usize>,
    #[cfg(feature = "rayon-threads")]
    hash_task_spawner: Option<CsvHashTaskSpawnerLocalRayon<'tp>>,
    #[cfg(feature = "rayon-threads")]
    _phantom: PhantomData<T>,
    #[cfg(not(feature = "rayon-threads"))]
    _phantom: PhantomData<&'tp T>,
    #[cfg(not(feature = "rayon-threads"))]
    hash_task_spawner: T,
}

impl<'tp, T> CsvByteDiffLocalBuilder<'tp, T>
where
    T: CsvHashTaskSpawnerLocal,
{
    #[cfg(not(feature = "rayon-threads"))]
    pub fn new<B>(csv_hash_task_spawner_builder: B) -> Self
    where
        B: CsvHashTaskSpawnerLocalBuilder<T>,
    {
        Self {
            primary_key_columns: std::iter::once(0).collect(),
            hash_task_spawner: csv_hash_task_spawner_builder.build(),
            _phantom: PhantomData::default(),
        }
    }

    pub fn primary_key_columns(mut self, columns: impl IntoIterator<Item = usize>) -> Self {
        self.primary_key_columns = columns.into_iter().collect();
        self
    }

    #[cfg(not(feature = "rayon-threads"))]
    pub fn build(self) -> Result<CsvByteDiffLocal<T>, CsvByteDiffBuilderError> {
        if !self.primary_key_columns.is_empty() {
            Ok(CsvByteDiffLocal {
                primary_key_columns: self.primary_key_columns,
                hash_task_spawner: self.hash_task_spawner,
            })
        } else {
            Err(CsvByteDiffBuilderError::NoPrimaryKeyColumns)
        }
    }
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvByteDiffLocalBuilder<'tp, CsvHashTaskSpawnerLocalRayon<'tp>> {
    pub fn new() -> Self {
        Self {
            primary_key_columns: std::iter::once(0).collect(),
            hash_task_spawner: None,
            _phantom: PhantomData::default(),
        }
    }

    pub fn rayon_thread_pool(mut self, thread_pool: &'tp rayon::ThreadPool) -> Self {
        self.hash_task_spawner =
            Some(CsvHashTaskSpawnerLocalBuilderRayon::new(thread_pool).build());
        self
    }

    #[cfg(feature = "rayon-threads")]
    pub fn build(
        self,
    ) -> Result<CsvByteDiffLocal<CsvHashTaskSpawnerLocalRayon<'tp>>, CsvByteDiffBuilderError> {
        if !self.primary_key_columns.is_empty() {
            Ok(CsvByteDiffLocal {
                primary_key_columns: self.primary_key_columns,
                hash_task_spawner: match self.hash_task_spawner {
                    Some(x) => x,
                    None => CsvHashTaskSpawnerLocalRayon::new(RayonScope::with_thread_pool_owned(
                        rayon::ThreadPoolBuilder::new().build()?,
                    )),
                },
            })
        } else {
            Err(CsvByteDiffBuilderError::NoPrimaryKeyColumns)
        }
    }
}

#[derive(Debug, Error)]
pub enum CsvByteDiffBuilderError {
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
impl CsvByteDiffLocal<CsvHashTaskSpawnerLocalRayon<'_>> {
    /// Constructs a new `CsvByteDiffLocal<CsvHashTaskSpawnerRayon<'_>>` with a default configuration.
    /// The values in the first column of each CSV will be declared as the primary key, in order
    /// to match the CSV records against each other.
    /// During the construction, a new [rayon thread-pool](https://docs.rs/rayon/1.5.0/rayon/struct.ThreadPool.html)
    /// is created, which will be used later during the [comparison of CSVs](CsvByteDiffLocal::diff).
    ///
    /// If you need to have more control over the configuration of `CsvByteDiffLocal<CsvHashTaskSpawnerRayon<'_>>`,
    /// consider using a [`CsvByteDiffLocalBuilder`](CsvByteDiffLocalBuilder) instead.
    pub fn new() -> Result<Self, CsvDiffNewError> {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: CsvHashTaskSpawnerLocalRayon::new(
                RayonScope::with_thread_pool_owned(rayon::ThreadPoolBuilder::new().build()?),
            ),
        };
        instance.primary_key_columns.insert(0);
        Ok(instance)
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CsvByteDiffLocal<CsvHashTaskSpawnerLocalCrossbeam> {
    pub fn new() -> Self {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
            hash_task_spawner: CsvHashTaskSpawnerLocalCrossbeam::new(CrossbeamScope::new()),
        };
        instance.primary_key_columns.insert(0);
        instance
    }
}

impl<T> CsvByteDiffLocal<T>
where
    T: CsvHashTaskSpawnerLocal,
{
    /// Compares `csv_left` with `csv_right` and returns the [CSV byte records](crate::diff_result::DiffByteRecords) that are different.
    ///
    /// [`Csv<R>`](Csv<R>) is a wrapper around a CSV reader with some configuration options.
    ///
    /// # Example
    #[cfg_attr(
        feature = "rayon-threads",
        doc = r##"
    use csv_diff::{csv_diff::CsvByteDiffLocal, csv::Csv};
    use csv_diff::diff_row::{ByteRecordLineInfo, DiffByteRecord};
    use std::collections::HashSet;
    use std::iter::FromIterator;
    # fn main() -> Result<(), Box<dyn std::error::Error>> {
    // some csv data with a header, where the first column is a unique id
    let csv_data_left = "id,name,kind\n\
                         1,lemon,fruit\n\
                         2,strawberry,fruit";
    let csv_data_right = "id,name,kind\n\
                          1,lemon,fruit\n\
                          2,strawberry,nut";

    let csv_byte_diff = CsvByteDiffLocal::new()?;

    let mut diff_byte_records = csv_byte_diff.diff(
        Csv::with_reader_seek(csv_data_left.as_bytes()),
        Csv::with_reader_seek(csv_data_right.as_bytes()),
    )?;

    diff_byte_records.sort_by_line();

    let diff_byte_rows = diff_byte_records.as_slice();

    assert_eq!(
        diff_byte_rows,
        &[DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["2", "strawberry", "fruit"]),
                3
            ),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["2", "strawberry", "nut"]), 3),
            field_indices: vec![2]
        }]
    );
    Ok(())
    # }
    "##
    )]
    pub fn diff<R: Read + Seek + Send>(
        &self,
        csv_left: Csv<R>,
        csv_right: Csv<R>,
    ) -> csv::Result<DiffByteRecords> {
        use crossbeam_channel::unbounded;

        let (sender_total_lines_right, receiver_total_lines_right) = unbounded();
        let (sender_total_lines_left, receiver_total_lines_left) = unbounded();
        let (sender_csv_reader_right, receiver_csv_reader_right) = unbounded();
        let (sender_csv_reader_left, receiver_csv_reader_left) = unbounded();
        let (sender_right, receiver) = unbounded();
        let sender_left = sender_right.clone();

        self.hash_task_spawner.spawn_hashing_tasks_and_send_result(
            CsvHashTaskLineSenders::new(
                sender_left,
                sender_total_lines_left,
                sender_csv_reader_left,
                csv_left,
            ),
            CsvHashTaskLineSenders::new(
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
        receiver: Receiver<CsvLeftRightParseResult<RecordHashWithPosition>>,
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
    use crate::diff_result::DiffByteRecords;
    use crate::diff_row::{ByteRecordLineInfo, DiffByteRecord};
    use pretty_assertions::assert_eq;
    use std::error::Error;

    fn csv_diff_local_with_sorting<T: CsvHashTaskSpawnerLocal>(
        csv_left: &str,
        csv_right: &str,
        mut expected: DiffByteRecords,
        csv_diff: CsvByteDiffLocal<T>,
    ) -> Result<(), Box<dyn Error>> {
        let mut diff_res_actual = csv_diff.diff(
            Csv::with_reader_seek(csv_left.as_bytes()),
            Csv::with_reader_seek(csv_right.as_bytes()),
        )?;

        diff_res_actual.sort_by_line();
        expected.sort_by_line();

        assert_eq!(diff_res_actual, expected, "csv_diff_local failed");

        Ok(())
    }

    fn csv_diff_with_sorting<T: CsvHashTaskSpawner>(
        csv_left: &'static str,
        csv_right: &'static str,
        mut expected: DiffByteRecords,
        csv_diff: CsvByteDiff<T>,
    ) -> Result<(), Box<dyn Error>> {
        let diff_iter = csv_diff.diff(
            Csv::with_reader(csv_left.as_bytes()),
            Csv::with_reader(csv_right.as_bytes()),
        );

        let mut actual = DiffByteRecords(diff_iter.collect());
        actual.sort_by_line();
        expected.sort_by_line();

        assert_eq!(actual, expected, "csv_diff failed");
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_empty_no_diff() -> Result<(), Box<dyn Error>> {
        let csv_left = "";
        let csv_right = "";
        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_no_headers_empty_no_diff() -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "";
        let csv_right = "";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                CsvBuilder::with_reader_seek(csv_left.as_bytes())
                    .headers(false)
                    .build(),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(false)
                    .build(),
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

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_no_header_no_diff() -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "\
                        a,b,c";
        let csv_right = "\
                        a,b,c";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                CsvBuilder::with_reader_seek(csv_left.as_bytes())
                    .headers(false)
                    .build(),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(false)
                    .build(),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_both_empty_but_one_has_header_and_the_other_has_none_both_with_correct_header_flag_no_diff(
    ) -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "\
                        header1,header2,header3";
        let csv_right = "";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                CsvBuilder::with_reader_seek(csv_left.as_bytes())
                    .headers(true)
                    .build(),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(false)
                    .build(),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![]);

        assert_eq!(diff_res_actual, diff_res_expected);
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_both_empty_but_one_has_header_and_the_other_has_none_both_with_header_flag_true_no_diff(
    ) -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3";
        let csv_right = "";

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_one_has_headers_one_does_not_no_diff() -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        a,b,c";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                Csv::with_reader_seek(csv_left.as_bytes()),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(false)
                    .build(),
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༽"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["༼", "౪", "༼"]), 2),
            field_indices: vec![2],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["a", "b", "c"]),
            2,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_one_with_header_and_one_not_added_one_line() -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "\
                        header1,header2,header3\n\
                        ";
        let csv_right = "\
                        a,b,c";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                CsvBuilder::with_reader_seek(csv_left.as_bytes())
                    .headers(true)
                    .build(),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(false)
                    .build(),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRecord::Add(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 1),
        )]);

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

        let expected = DiffByteRecords(vec![DiffByteRecord::Delete(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["a", "b", "c"]),
            2,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_one_with_header_and_one_not_deleted_one_line() -> Result<(), Box<dyn Error>> {
        use crate::csv::CsvBuilder;

        let csv_left = "\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        ";

        let diff_res_actual = CsvByteDiffLocal::new()?
            .diff(
                CsvBuilder::with_reader_seek(csv_left.as_bytes())
                    .headers(false)
                    .build(),
                CsvBuilder::with_reader_seek(csv_right.as_bytes())
                    .headers(true)
                    .build(),
            )
            .unwrap();
        let diff_res_expected = DiffByteRecords(vec![DiffByteRecord::Delete(
            ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 1),
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "c", "d"]), 2),
            field_indices: vec![1, 2],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_one_line_with_header_modified_all_fields_long() -> Result<(), Box<dyn Error>> {
        let csv_left = "\
                        header1,header2,header3,header4,header5,header6,header7,header8\n\
                        a,b,c,d,e,f,g,h";
        let csv_right = "\
                        header1,header2,header3,header4,header5,header6,header7,header8\n\
                        a,c,d,e,f,g,h,i";

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "b", "c", "d", "e", "f", "g", "h"]),
                2,
            ),
            add: ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["a", "c", "d", "e", "f", "g", "h", "i"]),
                2,
            ),
            field_indices: vec![1, 2, 3, 4, 5, 6, 7],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(csv_left, csv_right, expected, CsvByteDiffLocal::new()?)
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

        let expected = DiffByteRecords(vec![]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            2,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            3,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Add(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            4,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Delete(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            2,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Delete(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            3,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Delete(ByteRecordLineInfo::new(
            csv::ByteRecord::from(vec!["x", "y", "z"]),
            4,
        ))]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 2),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "x", "c"]), 3),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 3),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "e", "f"]), 3),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["d", "x", "f"]), 2),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 4),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 4),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "x", "z"]), 2),
            field_indices: vec![1],
        }]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                3,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 3),
                field_indices: vec![2],
            },
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                2,
            )),
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_multiple_lines_with_header_modified_at_end_added_at_end() -> Result<(), Box<dyn Error>>
    {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,z";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c\n\
                        x,y,a\n\
                        g,h,i";

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "z"]), 3),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["x", "y", "a"]), 3),
                field_indices: vec![2],
            },
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["j", "k", "l"]),
                5,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["m", "n", "o"]),
                6,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: vec![2],
            },
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "i"]), 4),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["g", "h", "x"]), 4),
                field_indices: vec![2],
            },
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
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

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: vec![2],
            },
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["p", "q", "r"]),
                3,
            )),
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["g", "h", "i"]),
                4,
            )),
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "l"]), 5),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["j", "k", "x"]), 6),
                field_indices: vec![2],
            },
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["x", "y", "z"]),
                5,
            )),
        ]);

        csv_diff_local_with_sorting(
            csv_left,
            csv_right,
            expected.clone(),
            CsvByteDiffLocal::new()?,
        )?;

        csv_diff_with_sorting(csv_left, csv_right, expected, CsvByteDiff::new()?)
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_primary_key_columns_is_no_primary_key_columns_err(
    ) -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let expected = CsvByteDiffBuilderError::NoPrimaryKeyColumns;
        let actual = CsvByteDiffLocalBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .primary_key_columns(std::iter::empty())
            .build();

        assert!(actual.is_err());
        assert!(matches!(
            actual,
            Err(CsvByteDiffBuilderError::NoPrimaryKeyColumns)
        ));
        assert_eq!(expected.to_string(), "No primary key columns have been specified. You need to provide at least one column index.");
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn builder_without_specified_primary_key_columns_is_ok() -> Result<(), Box<dyn Error>> {
        // it is ok, because it gets a sensible default value
        assert!(CsvByteDiffLocalBuilder::new()
            .rayon_thread_pool(&rayon::ThreadPoolBuilder::new().build()?)
            .build()
            .is_ok());
        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn diff_created_with_existing_thread_pool_works() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff_local = CsvByteDiffLocalBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .build()?;

        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";

        let expected = DiffByteRecords(vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }]);

        csv_diff_local_with_sorting(csv_left, csv_right, expected, csv_diff_local)

        // TODO: also create a builder for `CsvByteDiff`, so that we can test the following
        // csv_diff_with_sorting(csv_left, csv_right, expected, csv_diff)?
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

        let thread_pool = &rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = CsvByteDiffLocalBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .primary_key_columns(vec![0, 1])
            .build()?;

        let expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: vec![2],
            },
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "f", "f"]),
                4,
            )),
        ]);

        csv_diff_local_with_sorting(csv_left, csv_right, expected, csv_diff)

        // TODO: also create a builder for `CsvByteDiff`, so that we can test the following
        // csv_diff_with_sorting(csv_left, csv_right, expected, csv_diff)?
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

        let diff_res_actual = CsvByteDiffLocal::new()?.diff(
            Csv::with_reader_seek(csv_left.as_bytes()),
            Csv::with_reader_seek(csv_right.as_bytes()),
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

        let diff_res_actual = CsvByteDiffLocal::new()?.diff(
            Csv::with_reader_seek(csv_left.as_bytes()),
            Csv::with_reader_seek(csv_right.as_bytes()),
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

    #[cfg(feature = "crossbeam-threads")]
    #[test]
    // TODO: this is our only test for the "crossbeam-threads" feature;
    // we should write a macro, so that we can reuse test code for both "rayon" and "crossbeam-threads"
    fn diff_multiple_lines_with_header_combined_key_added_deleted_modified(
    ) -> Result<(), CsvByteDiffBuilderError> {
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

        let mut diff_res_actual = CsvByteDiffLocalBuilder::<CsvHashTaskSpawnerLocalCrossbeam>::new(
            CsvHashTaskSpawnerLocalBuilderCrossbeam::new(),
        )
        .primary_key_columns(vec![0, 1])
        .build()?
        .diff(
            Csv::with_reader_seek(csv_left.as_bytes()),
            Csv::with_reader_seek(csv_right.as_bytes()),
        )
        .unwrap();
        let mut diff_res_expected = DiffByteRecords(vec![
            DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "x"]), 2),
                field_indices: vec![2],
            },
            DiffByteRecord::Delete(ByteRecordLineInfo::new(
                csv::ByteRecord::from(vec!["d", "e", "f"]),
                3,
            )),
            DiffByteRecord::Add(ByteRecordLineInfo::new(
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
