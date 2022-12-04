#[derive(Debug, PartialEq, Clone)]
pub enum DiffByteRecord {
    Add(ByteRecordLineInfo),
    Modify {
        delete: ByteRecordLineInfo,
        add: ByteRecordLineInfo,
        field_indices: Vec<usize>,
    },
    Delete(ByteRecordLineInfo),
}

impl DiffByteRecord {
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

#[derive(Debug, PartialEq, Clone)]
pub struct ByteRecordLineInfo {
    byte_record: csv::ByteRecord,
    line: u64,
}

impl ByteRecordLineInfo {
    pub fn new(byte_record: csv::ByteRecord, line: u64) -> Self {
        Self { byte_record, line }
    }

    pub fn byte_record(&self) -> &csv::ByteRecord {
        &self.byte_record
    }

    pub fn into_byte_record(self) -> csv::ByteRecord {
        self.byte_record
    }

    pub fn line(&self) -> u64 {
        self.line
    }
}
