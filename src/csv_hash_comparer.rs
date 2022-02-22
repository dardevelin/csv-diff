use crate::csv_parse_result::CsvLeftRightParseResult;
use crate::diff_result::*;
use crate::diff_row::*;
use ahash::AHashMap as HashMap;
use crossbeam_channel::Receiver;
use std::io::Read;
use std::io::Seek;

pub(crate) struct CsvHashComparer<R: Read + Seek> {
    csv_records_left_map: CsvHashValueMap,
    csv_records_right_map: CsvHashValueMap,
    intermediate_left_map: CsvHashValueMap,
    intermediate_right_map: CsvHashValueMap,
    max_capacity_left_map: usize,
    max_capacity_right_map: usize,
    csv_seek_left_reader: csv::Reader<R>,
    csv_seek_right_reader: csv::Reader<R>,
    diff_records: Vec<DiffByteRecord>,
}

impl<R: Read + std::io::Seek> CsvHashComparer<R> {
    // TODO: maybe we can simplify this to only take one capacity and use it for both?
    // But keep in mind, we would loose on flexibility (one csv is very small and one very big?)
    pub fn with_capacity_and_reader(
        left_capacity: usize,
        right_capacity: usize,
        left_reader: csv::Reader<R>,
        right_reader: csv::Reader<R>,
    ) -> Self {
        Self {
            csv_records_left_map: HashMap::with_capacity(left_capacity),
            csv_records_right_map: HashMap::with_capacity(right_capacity),
            intermediate_left_map: HashMap::new(),
            intermediate_right_map: HashMap::new(),
            max_capacity_left_map: left_capacity,
            max_capacity_right_map: right_capacity,
            csv_seek_left_reader: left_reader,
            csv_seek_right_reader: right_reader,
            diff_records: Vec::new(),
        }
    }

    pub fn compare_csv_left_right_parse_result(
        self,
        csv_left_right_parse_results: Receiver<CsvLeftRightParseResult>,
    ) -> DiffByteRecordsIter<R> {
        let iter = DiffByteRecordsIter::new(
            csv_left_right_parse_results,
            self.max_capacity_left_map,
            self.max_capacity_right_map,
            self.csv_seek_left_reader,
            self.csv_seek_right_reader,
        );

        iter
    }
}
