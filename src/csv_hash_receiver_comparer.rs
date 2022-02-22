use crate::{
    csv_hash_comparer::CsvHashComparer,
    csv_parse_result::CsvLeftRightParseResult,
    diff_result::{DiffByteRecords, DiffByteRecordsIter},
};
use crossbeam_channel::Receiver;
use csv::Reader;
use std::io::{Read, Seek};

pub struct CsvHashReceiverComparer<R: Read + Seek + Send> {
    // make it more private
    pub receiver_total_lines_left: Receiver<u64>,
    pub receiver_total_lines_right: Receiver<u64>,
    pub receiver_csv_reader_left: Receiver<csv::Result<Reader<R>>>,
    pub receiver_csv_reader_right: Receiver<csv::Result<Reader<R>>>,
    pub receiver: Receiver<CsvLeftRightParseResult>,
}

impl<R: Read + Seek + Send> CsvHashReceiverComparer<R> {
    pub fn recv_hashes_and_compare(self) -> csv::Result<DiffByteRecordsIter<R>> {
        let (total_lines_right, total_lines_left) = (
            self.receiver_total_lines_right.recv().unwrap_or_default(),
            self.receiver_total_lines_left.recv().unwrap_or_default(),
        );
        let (csv_reader_right_for_diff_seek, csv_reader_left_for_diff_seek) = (
            self.receiver_csv_reader_right.recv().unwrap()?,
            self.receiver_csv_reader_left.recv().unwrap()?,
        );
        let max_capacity_for_hash_map_right =
            if total_lines_right / 100 < total_lines_right && total_lines_right / 100 == 0 {
                total_lines_right
            } else {
                total_lines_right / 100
            } as usize;
        let max_capacity_for_hash_map_left =
            if total_lines_left / 100 < total_lines_left && total_lines_left / 100 == 0 {
                total_lines_left
            } else {
                total_lines_left / 100
            } as usize;

        let csv_hash_comparer = CsvHashComparer::with_capacity_and_reader(
            max_capacity_for_hash_map_left,
            max_capacity_for_hash_map_right,
            csv_reader_left_for_diff_seek,
            csv_reader_right_for_diff_seek,
        );
        Ok(csv_hash_comparer.compare_csv_left_right_parse_result(self.receiver))
    }
}
