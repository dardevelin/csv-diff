#[cfg(test)]
mod integration_test {
    #[cfg(feature = "rayon-threads")]
    use csv_diff::csv_hash_task_spawner::CsvHashTaskSpawnerBuilderRayon;
    use csv_diff::{
        diff_result::{DiffRecords, DiffResult},
        diff_row::{DiffRow, RecordLineInfo},
    };
    use pretty_assertions::assert_eq;
    use std::{collections::HashSet, error::Error, io::Cursor, iter::FromIterator};

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_default_instance_and_diff() -> Result<(), Box<dyn Error>> {
        let csv_diff = csv_diff::csv_diff::CsvDiff::new()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let diff_res_actual = csv_diff.diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )?;

        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);

        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_instance_with_builder_and_diff() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = csv_diff::csv_diff::CsvDiffBuilder::new(
            CsvHashTaskSpawnerBuilderRayon::new(&thread_pool),
        )
        .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let diff_res_actual = csv_diff.diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )?;

        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);

        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_instance_with_builder_set_all_and_diff() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = csv_diff::csv_diff::CsvDiffBuilder::new(
            CsvHashTaskSpawnerBuilderRayon::new(&thread_pool),
        )
        .primary_key_columns(std::iter::once(0))
        .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let diff_res_actual = csv_diff.diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )?;

        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);

        Ok(())
    }

    #[cfg(feature = "crossbeam-utils")]
    #[test]
    fn create_instance_with_builder_crossbeam_and_diff() -> Result<(), Box<dyn Error>> {
        use csv_diff::csv_hash_task_spawner::CsvHashTaskSpawnerBuilderCrossbeam;

        let csv_diff =
            csv_diff::csv_diff::CsvDiffBuilder::new(CsvHashTaskSpawnerBuilderCrossbeam::new())
                .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let diff_res_actual = csv_diff.diff(
            Cursor::new(csv_left.as_bytes()),
            Cursor::new(csv_right.as_bytes()),
        )?;

        let diff_res_expected = DiffResult::Different {
            diff_records: DiffRecords(vec![DiffRow::Modified {
                deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: HashSet::from_iter(vec![2]),
            }]),
        };

        assert_eq!(diff_res_actual, diff_res_expected);

        Ok(())
    }

    mod custom_scoped_threads {
        use super::*;
        use csv_diff::{
            csv_hash_task_spawner::{
                CsvHashTaskSenders, CsvHashTaskSpawner, CsvHashTaskSpawnerBuilder,
            },
            csv_parse_result::{CsvParseResultLeft, CsvParseResultRight},
        };
        use pretty_assertions::assert_eq;
        use std::{
            collections::HashSet,
            error::Error,
            io::{Cursor, Read, Seek},
        };

        struct CsvHashTaskSpawnerCustom {
            pool: scoped_pool::Pool,
        }

        impl CsvHashTaskSpawnerCustom {
            pub fn new(pool_size: usize) -> Self {
                Self {
                    pool: scoped_pool::Pool::new(pool_size),
                }
            }
        }

        impl CsvHashTaskSpawner for CsvHashTaskSpawnerCustom {
            fn spawn_hashing_tasks_and_send_result<R>(
                &self,
                csv_hash_task_senders_left: CsvHashTaskSenders<R>,
                csv_hash_task_senders_right: CsvHashTaskSenders<R>,
                primary_key_columns: &HashSet<usize>,
            ) where
                R: Read + Seek + Send,
            {
                self.pool.scoped(move |s| {
                    s.execute(move || {
                        self.parse_hash_and_send_for_compare::<R, CsvParseResultLeft>(
                            csv_hash_task_senders_left,
                            primary_key_columns,
                        );
                    });
                    s.execute(move || {
                        self.parse_hash_and_send_for_compare::<R, CsvParseResultRight>(
                            csv_hash_task_senders_right,
                            primary_key_columns,
                        );
                    });
                });
            }
        }

        struct CsvHashTaskSpawnerBuilderCustom {
            pool_size: usize,
        }

        impl CsvHashTaskSpawnerBuilderCustom {
            pub fn new(pool_size: usize) -> Self {
                Self { pool_size }
            }
        }

        impl CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerCustom> for CsvHashTaskSpawnerBuilderCustom {
            fn build(self) -> CsvHashTaskSpawnerCustom {
                CsvHashTaskSpawnerCustom::new(self.pool_size)
            }
        }
        #[test]
        fn create_instance_with_builder_custom_scoped_threads_and_diff(
        ) -> Result<(), Box<dyn Error>> {
            let csv_diff =
                csv_diff::csv_diff::CsvDiffBuilder::new(CsvHashTaskSpawnerBuilderCustom::new(4))
                    .primary_key_columns(std::iter::once(0))
                    .build()?;
            let csv_left = "\
                            header1,header2,header3\n\
                            a,b,c";
            let csv_right = "\
                            header1,header2,header3\n\
                            a,b,d";
            let diff_res_actual = csv_diff.diff(
                Cursor::new(csv_left.as_bytes()),
                Cursor::new(csv_right.as_bytes()),
            )?;

            let diff_res_expected = DiffResult::Different {
                diff_records: DiffRecords(vec![DiffRow::Modified {
                    deleted: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                    added: RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                    field_indices: HashSet::from_iter(vec![2]),
                }]),
            };

            assert_eq!(diff_res_actual, diff_res_expected);

            Ok(())
        }
    }
}
