use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub(crate) enum DiffRow {
    Added(RecordLineInfo),
    Modified {
        deleted: RecordLineInfo,
        added: RecordLineInfo,
        field_indices: HashSet<usize>,
    },
    Deleted(RecordLineInfo),
}

impl DiffRow {
    pub fn line_num(&self) -> LineNum {
        match self {
            Self::Added(rli) | Self::Deleted(rli) => LineNum::OneSide(rli.line),
            Self::Modified { deleted, added, .. } => LineNum::BothSides {
                for_deleted: deleted.line,
                for_added: added.line,
            },
        }
    }
}

pub(crate) enum LineNum {
    OneSide(u64),
    BothSides { for_deleted: u64, for_added: u64 },
}

#[derive(Debug, PartialEq)]
pub(crate) struct RecordLineInfo {
    byte_record: csv::ByteRecord,
    line: u64,
}

impl RecordLineInfo {
    pub fn new(byte_record: csv::ByteRecord, line: u64) -> Self {
        Self { byte_record, line }
    }
}
