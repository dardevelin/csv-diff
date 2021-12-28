use crate::diff_row::*;

/// Holds all information about the difference between two CSVs, after they have
/// been compared with [`CsvByteDiff.diff`](crate::csv_diff::CsvByteDiff::diff).
/// CSV records that are equal are __not__ stored in this structure.
///
/// Also, keep in mind, that differences are stored _unordered_ (with regard to the line in the CSV).
/// You can use [`DiffByteRecords.sort_by_line`](DiffByteRecords::sort_by_line) to sort them in-place.
///
/// See the example on [`CsvByteDiff`](crate::csv_diff::CsvByteDiff) for general usage.
#[derive(Debug, PartialEq)]
pub struct DiffByteRecords(pub(crate) Vec<DiffByteRecord>);

impl DiffByteRecords {
    /// Sort the underlying [`DiffByteRecord`](crate::diff_row::DiffByteRecord)s by line.
    ///
    /// Note that comparison is done in parallel. Therefore, __without calling this method__, the resulting `DiffByteRecord`s are out of order
    /// after the comparison (with regard to their line in the original CSV).
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

    /// Return the `DiffByteRecord`s as a single slice.
    /// # Example
    #[cfg_attr(
        feature = "rayon-threads",
        doc = r##"
    use std::io::Cursor;
    use csv_diff::{csv_diff::CsvByteDiff, csv::Csv};
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

    let csv_byte_diff = CsvByteDiff::new()?;

    let mut diff_byte_records = csv_byte_diff.diff(
        Csv::new(Cursor::new(csv_data_left.as_bytes())),
        Csv::new(Cursor::new(csv_data_right.as_bytes())),
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
