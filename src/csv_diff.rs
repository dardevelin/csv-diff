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
struct CsvRowKey {
    key: Vec<Vec<u8>>,
}

impl CsvRowKey {
    pub fn new() -> Self {
        Self {
            key: Vec::new(),
        }
    }

    pub fn push_key_column(&mut self, key_column: Vec<u8>) {
        self.key.push(key_column);
    }
}

impl From<Vec<Vec<u8>>> for CsvRowKey {
    
    fn from(csv_row_key_vec: Vec<Vec<u8>>) -> Self {
        Self {
            key: csv_row_key_vec,
        }
    }
}

struct KeyByteRecord<'a, 'b> {
    key_idx: &'a HashSet<usize>,
    byte_record: &'b csv::ByteRecord,
}

impl From<KeyByteRecord<'_,'_>> for CsvRowKey {

    fn from(key_byte_record: KeyByteRecord<'_, '_>) -> Self {
        let mut row_key = Vec::new();
        for idx in key_byte_record.key_idx.iter() {
            if let Some(field) = key_byte_record.byte_record.get(*idx) {
                let slice = Vec::from(field);
                row_key.push(slice);
            }
        }
        CsvRowKey::from(row_key)
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
        let csv_reader_left = csv::Reader::from_reader(csv_left);
        let csv_reader_right = csv::Reader::from_reader(csv_right);

        let csv_records_left = csv_reader_left.into_byte_records();
        let mut csv_records_right_enumerate = csv_reader_right.into_byte_records().enumerate();

        let mut csv_records_right_map: ByteRecordMap =
            csv_records_right_enumerate.try_fold::<_, _, csv::Result<ByteRecordMap>>(
                    ByteRecordMap::new(&self.primary_key_columns), |mut acc, (row, curr_byte_record_res)| {
                        let byte_record = curr_byte_record_res?;
                        acc.insert(byte_record, row);
                        Ok(acc)
            })?;
        
        let mut diff_records = Vec::new();
        for (line_left, byte_record_left_res) in csv_records_left.enumerate() {
            let byte_record_left = byte_record_left_res?;
            let csv_row_key = CsvRowKey::from(KeyByteRecord {
                key_idx: &self.primary_key_columns,
                byte_record: &byte_record_left
            });
            if let Some((line_right, byte_record_right)) = csv_records_right_map.remove(&csv_row_key) {
                // we have a modification in our csv line or they are equal
            } else {
                // record has been deleted as it can't be found in csv on the right
                let line_left = byte_record_left.position().unwrap().line();
                diff_records.push(DiffRow::Deleted(RecordLineInfo::new(byte_record_left, line_left)));
            }
        }
        // every record that has been added (which is in the right csv, but not in the left)
        diff_records
            .extend(csv_records_right_map.map.into_iter()
                .map(|(_, (line, byte_record))| {
                    let line = byte_record.position().unwrap().line();
                    DiffRow::Added(RecordLineInfo::new(byte_record, line))
                }));
        Ok(if diff_records.is_empty() {
            DiffResult::Equal
        } else {
            DiffResult::Different { diff_records }
        })
    }
}

struct ByteRecordMap<'a> {
    key_idx: &'a HashSet<usize>,
    map: HashMap<CsvRowKey, (usize, csv::ByteRecord)>,
}

impl<'a> ByteRecordMap<'a> {

    pub fn new(key_idx: &'a HashSet<usize>) -> Self {
        Self {
            key_idx,
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, byte_record: csv::ByteRecord, row: usize) {
        let mut row_key = Vec::new();
        for idx in self.key_idx.iter() {
            if let Some(field) = byte_record.get(*idx) {
                let slice = Vec::from(field);
                row_key.push(slice);
            }
        }
        
        self.map.insert(CsvRowKey::from(row_key), (row, byte_record));
    }

    pub fn remove(&mut self, csv_row_key: &CsvRowKey) -> Option<(usize, csv::ByteRecord)> {
        self.map.remove(csv_row_key)
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