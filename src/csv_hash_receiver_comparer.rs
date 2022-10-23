use crate::{
    csv_parse_result::{CsvByteRecordWithHash, CsvLeftRightParseResult},
    diff_result::DiffByteRecordsIterator,
};
use crossbeam_channel::{Receiver, Sender};

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
    pub(crate) fn recv_hashes_and_compare(self) -> DiffByteRecordsIterator {
        DiffByteRecordsIterator::new(self.receiver, self.sender_csv_records_recycle)
    }
}
