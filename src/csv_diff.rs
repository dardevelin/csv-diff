use std::io::Read;
use std::error::Error;
use std::collections::{
    HashSet,
    HashMap,
};
use crate::diff_row::{
    DiffRow,
    RecordLineInfo
};

#[derive(Debug, PartialEq)]
struct CsvDiff {
    primary_key_columns: HashSet<usize>,
}

#[derive(Debug, PartialEq)]
enum DiffResult {
    Equal,
    Different {
        diff_records: Vec<DiffRow>,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CsvRowKey<'a> {
    key: Vec<&'a [u8]>,
}

impl<'a> CsvRowKey<'a> {
    pub fn new() -> Self {
        Self {
            key: Vec::new(),
        }
    }

    pub fn push_key_column(&mut self, key_column: &'a [u8]) {
        self.key.push(key_column);
    }
}

impl<'a> From<Vec<&'a [u8]>> for CsvRowKey<'a> {
    
    fn from(csv_row_key_vec: Vec<&'a [u8]>) -> Self {
        Self {
            key: csv_row_key_vec,
        }
    }
}

impl CsvDiff {

    pub fn new() -> Self {
        let mut instance = Self {
            primary_key_columns: HashSet::new(),
        };
        instance.primary_key_columns.insert(0);
        // TODO: when more than one primary key column is specified, we have to sort and dedup
        instance
    }

    pub fn diff<R: Read>(&self, csv_left: R, csv_right: R) -> csv::Result<DiffResult> {
        let mut csv_reader_left = csv::Reader::from_reader(csv_left);
        let csv_reader_right = csv::Reader::from_reader(csv_right);

        let csv_records_left = csv_reader_left.byte_records();
        let mut csv_records_left_iter_enumerate = csv_records_left.enumerate();
        let mut csv_records_right_iter = csv_reader_right.into_byte_records();

        let csv_records_left_map: std::result::Result<ByteRecordMap, csv::Error> =
            csv_records_left_iter_enumerate.try_fold(ByteRecordMap::new(&self.primary_key_columns), |mut acc, (row, curr_byte_record_res)| {
                let byte_record = curr_byte_record_res?;
                acc.insert(byte_record);
                Ok(acc)
        });
        Ok(DiffResult::Equal)
    }
}

struct ByteRecordMap<'a, 'b> where 'b:'a {
    key_idx: &'a HashSet<usize>,
    map: HashMap<CsvRowKey<'a>, &'b csv::ByteRecord>,
}

impl<'a, 'b> ByteRecordMap<'a, 'b> {

    pub fn new(key_idx: &'a HashSet<usize>) -> Self {
        Self {
            key_idx,
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, byte_record: &'b csv::ByteRecord) where 'b:'a {
        let mut row_key = Vec::new();
        {
            for idx in self.key_idx.iter() {
                if let Some(field) = byte_record.get(*idx) {
                    row_key.push(field);
                }
            }
        }
        
        self.map.insert(CsvRowKey::from(row_key), byte_record);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};

    #[test]
    fn diff_one_line_with_header_no_diff() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new().diff(csv_left.as_bytes(), csv_right.as_bytes()).unwrap();
        let diff_res_expected = DiffResult::Equal;

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_added_one() {
        let csv_left = "\
                        header1,header2,header3\n\
                        ";
        let csv_right = "\
                        header1,header2,header3\n\
                        a,b,c";

        let diff_res_actual = CsvDiff::new().diff(csv_left.as_bytes(), csv_right.as_bytes()).unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: vec![
                DiffRow::Added(RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2))
            ]
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }

    #[test]
    fn diff_one_line_with_header_deleted_one() {
        let csv_left = "\
                        header1,header2,header3\n\
                        a,b,c";
        let csv_right = "\
                        header1,header2,header3\n\
                        ";

        let diff_res_actual = CsvDiff::new().diff(csv_left.as_bytes(), csv_right.as_bytes()).unwrap();
        let diff_res_expected = DiffResult::Different {
            diff_records: vec![
                DiffRow::Deleted(RecordLineInfo::new(csv::ByteRecord::from(vec!["a", "b", "c"]), 2))
            ]
        };

        assert_eq!(diff_res_actual, diff_res_expected);
    }
}