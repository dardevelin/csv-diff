use std::collections::HashSet;

#[derive(Debug, PartialEq)]
pub enum DiffByteRow {
    Add(ByteRecordLineInfo),
    Modify {
        delete: ByteRecordLineInfo,
        add: ByteRecordLineInfo,
        field_indices: HashSet<usize>,
    },
    Delete(ByteRecordLineInfo),
}

impl DiffByteRow {
    pub fn line_num(&self) -> LineNum {
        match self {
            Self::Add(rli) | Self::Delete(rli) => LineNum::OneSide(rli.line),
            Self::Modify {
                delete: deleted,
                add: added,
                ..
            } => LineNum::BothSides {
                for_deleted: deleted.line,
                for_added: added.line,
            },
        }
    }
}

pub enum LineNum {
    OneSide(u64),
    BothSides { for_deleted: u64, for_added: u64 },
}

#[derive(Debug, PartialEq)]
pub struct ByteRecordLineInfo {
    byte_record: csv::ByteRecord,
    line: u64,
}

impl ByteRecordLineInfo {
    pub fn new(byte_record: csv::ByteRecord, line: u64) -> Self {
        Self { byte_record, line }
    }
}
