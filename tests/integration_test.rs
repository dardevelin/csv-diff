#[cfg(test)]
mod integration_test {
    use csv_diff::csv::Csv;
    use csv_diff::diff_row::{ByteRecordLineInfo, DiffByteRecord};
    use pretty_assertions::assert_eq;
    use std::{collections::HashSet, error::Error, io::Cursor, iter::FromIterator};

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_default_instance_and_diff() -> Result<(), Box<dyn Error>> {
        let csv_diff = csv_diff::csv_diff::CsvByteDiff::new()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let mut diff_res = csv_diff.diff(
            Csv::new(Cursor::new(csv_left.as_bytes())),
            Csv::new(Cursor::new(csv_right.as_bytes())),
        )?;

        diff_res.sort_by_line();
        let diff_rows_actual = diff_res.as_slice();

        let diff_rows_expected = vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }];

        assert_eq!(diff_rows_actual, diff_rows_expected.as_slice());

        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_instance_with_builder_and_diff() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = csv_diff::csv_diff::CsvByteDiffBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let mut diff_res = csv_diff.diff(
            Csv::new(Cursor::new(csv_left.as_bytes())),
            Csv::new(Cursor::new(csv_right.as_bytes())),
        )?;

        diff_res.sort_by_line();
        let diff_rows_actual = diff_res.as_slice();

        let diff_rows_expected = vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }];

        assert_eq!(diff_rows_actual, diff_rows_expected.as_slice());

        Ok(())
    }

    #[cfg(feature = "rayon-threads")]
    #[test]
    fn create_instance_with_builder_set_all_and_diff() -> Result<(), Box<dyn Error>> {
        let thread_pool = rayon::ThreadPoolBuilder::new().build()?;
        let csv_diff = csv_diff::csv_diff::CsvByteDiffBuilder::new()
            .rayon_thread_pool(&thread_pool)
            .primary_key_columns(std::iter::once(0))
            .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let mut diff_res = csv_diff.diff(
            Csv::new(Cursor::new(csv_left.as_bytes())),
            Csv::new(Cursor::new(csv_right.as_bytes())),
        )?;

        diff_res.sort_by_line();
        let diff_rows_actual = diff_res.as_slice();

        let diff_rows_expected = vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }];

        assert_eq!(diff_rows_actual, diff_rows_expected.as_slice());

        Ok(())
    }

    #[cfg(feature = "crossbeam-threads")]
    #[test]
    fn create_instance_with_builder_crossbeam_and_diff() -> Result<(), Box<dyn Error>> {
        use csv_diff::csv_hash_task_spawner::CsvHashTaskSpawnerBuilderCrossbeam;

        let csv_byte_diff =
            csv_diff::csv_diff::CsvByteDiffBuilder::new(CsvHashTaskSpawnerBuilderCrossbeam::new())
                .build()?;
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,d";
        let mut diff_res = csv_byte_diff.diff(
            Csv::new(Cursor::new(csv_left.as_bytes())),
            Csv::new(Cursor::new(csv_right.as_bytes())),
        )?;

        diff_res.sort_by_line();
        let diff_rows_actual = diff_res.as_slice();

        let diff_rows_expected = vec![DiffByteRecord::Modify {
            delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
            add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
            field_indices: vec![2],
        }];

        assert_eq!(diff_rows_actual, diff_rows_expected);

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
        #[cfg(not(feature = "rayon-threads"))]
        #[test]
        fn create_instance_with_builder_custom_scoped_threads_and_diff(
        ) -> Result<(), Box<dyn Error>> {
            let csv_byte_diff = csv_diff::csv_diff::CsvByteDiffBuilder::new(
                CsvHashTaskSpawnerBuilderCustom::new(4),
            )
            .primary_key_columns(std::iter::once(0))
            .build()?;
            let csv_left = "\
                            header1,header2,header3\n\
                            a,b,c";
            let csv_right = "\
                            header1,header2,header3\n\
                            a,b,d";
            let mut diff_res = csv_byte_diff.diff(
                Csv::new(Cursor::new(csv_left.as_bytes())),
                Csv::new(Cursor::new(csv_right.as_bytes())),
            )?;

            diff_res.sort_by_line();
            let diff_rows_actual = diff_res.as_slice();

            let diff_rows_expected = vec![DiffByteRecord::Modify {
                delete: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2),
                add: ByteRecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "d"]), 2),
                field_indices: vec![2],
            }];

            assert_eq!(diff_rows_actual, diff_rows_expected);

            Ok(())
        }
    }
}
