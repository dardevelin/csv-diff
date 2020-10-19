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

#[derive(Debug, PartialEq)]
pub(crate) struct RecordLineInfo {
    byte_record: csv::ByteRecord,
    line: u64,
}

impl RecordLineInfo {
    pub fn new(byte_record: csv::ByteRecord, line: u64) -> Self {
        Self {
            byte_record,
            line,
        }
    }
}
