use crate::{
    csv_parse_result::{CsvByteRecordWithHash, CsvLeftRightParseResult, RecordHashWithPosition},
    diff_result::DiffByteRecordsIterator,
};
use crossbeam_channel::{Receiver, Sender};
use csv::Reader;
use std::io::{Read, Seek};

pub struct CsvHashReceiverStreamComparer {
    receiver: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
    sender_csv_records_recycle: Sender<csv::ByteRecord>,
}

impl CsvHashReceiverStreamComparer {
    pub(crate) fn new(
        receiver: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
        sender_csv_records_recycle: Sender<csv::ByteRecord>,
    ) -> Self {
        Self {
            receiver,
            sender_csv_records_recycle,
        }
    }
    pub fn recv_hashes_and_compare(self) -> DiffByteRecordsIterator {
        DiffByteRecordsIterator::new(
            self.receiver,
            // TODO: for now we just put a hard code value in here (optimal for 1_000_000 lines),
            // but this needs to be revised. The challenge is: how do we know the optimal capacity,
            // when we don't know the total num of lines?
            10_000,
            10_000,
            self.sender_csv_records_recycle,
        )
    }
}
