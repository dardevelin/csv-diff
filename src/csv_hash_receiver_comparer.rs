use crate::{
    csv_parse_result::{
        CsvByteRecordWithHash, CsvByteRecordWithHashFirstFewLines, CsvLeftRightParseResult,
    },
    diff_result::DiffByteRecordsIterator,
};
use crossbeam_channel::{Receiver, Sender};

pub struct CsvHashReceiverStreamComparer {
    receiver: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
    receiver_first_few_lines: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHashFirstFewLines>>,
    sender_csv_records_recycle: Sender<csv::ByteRecord>,
}

impl CsvHashReceiverStreamComparer {
    pub(crate) fn new(
        receiver: Receiver<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
        receiver_first_few_lines: Receiver<
            CsvLeftRightParseResult<CsvByteRecordWithHashFirstFewLines>,
        >,
        sender_csv_records_recycle: Sender<csv::ByteRecord>,
    ) -> Self {
        Self {
            receiver,
            receiver_first_few_lines,
            sender_csv_records_recycle,
        }
    }
    pub fn recv_hashes_and_compare(self) -> DiffByteRecordsIterator {
        DiffByteRecordsIterator::new(
            self.receiver,
            self.receiver_first_few_lines,
            self.sender_csv_records_recycle,
        )
    }
}
