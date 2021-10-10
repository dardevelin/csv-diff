use crate::diff_row::*;

#[derive(Debug, PartialEq)]
pub struct DiffByteRecords(pub(crate) Vec<DiffByteRow>);

impl DiffByteRecords {
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

    pub fn as_slice(&self) -> &[DiffByteRow] {
        self.0.as_slice()
    }

    pub fn iter(&self) -> core::slice::Iter<'_, DiffByteRow> {
        self.0.iter()
    }
}

impl IntoIterator for DiffByteRecords {
    type Item = DiffByteRow;
    type IntoIter = DiffRecordsIntoIterator;

    fn into_iter(self) -> Self::IntoIter {
        DiffRecordsIntoIterator {
            inner: self.0.into_iter(),
        }
    }
}

pub struct DiffRecordsIntoIterator {
    inner: std::vec::IntoIter<DiffByteRow>,
}

impl Iterator for DiffRecordsIntoIterator {
    type Item = DiffByteRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
