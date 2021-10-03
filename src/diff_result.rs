use crate::diff_row::*;

#[derive(Debug, PartialEq)]
pub enum DiffResult {
    Equal,
    Different { diff_records: DiffRecords },
}

impl DiffResult {
    pub fn sort_by_line(self) -> DiffResultSorted {
        match self {
            Self::Different { mut diff_records } => {
                diff_records.sort_by_line();
                DiffResultSorted::ByLine(diff_records)
            }
            Self::Equal => DiffResultSorted::ByLine(DiffRecords(vec![])),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DiffResultSorted {
    ByLine(DiffRecords),
}

impl DiffResultSorted {
    pub fn into_vec(self) -> Vec<DiffRow> {
        match self {
            Self::ByLine(diff_records) => diff_records.into_vec(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DiffRecords(pub(crate) Vec<DiffRow>);

impl DiffRecords {
    pub(crate) fn sort_by_line(&mut self) {
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

    pub(crate) fn into_vec(self) -> Vec<DiffRow> {
        self.0
    }
}
