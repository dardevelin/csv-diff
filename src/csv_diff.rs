use std::io::Read;
use std::error::Error;
use std::collections::{
    HashSet,
    HashMap,
};

struct CsvDiff {
    primary_key_columns: HashSet<usize>,
}

struct DiffResult {

}

impl DiffResult {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(PartialEq, Eq)]
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

        let mut csv_records_left_iter_enumerate = csv_reader_left.into_byte_records().enumerate();
        let mut csv_records_right_iter = csv_reader_right.into_byte_records();

        let mut csv_records_left_map: HashMap<CsvRowKey, &csv::ByteRecord> = HashMap::new();

        while let (Some((row_pos, byte_record_left_res)), Some(byte_record_right_res)) = (csv_records_left_iter_enumerate.next(), csv_records_right_iter.next()) {
            let byte_record_left = byte_record_left_res?;
            let byte_record_right = byte_record_right_res?;

            let mut csv_row_key_left = CsvRowKey::new();
            let mut csv_row_key_right = CsvRowKey::new();
            // column count is guaranteed to be the same here -> otherwise error would have been thrown above
            for (column_pos, (field_left, field_right)) in byte_record_left.iter().zip(byte_record_right.iter()).enumerate() {
                if self.primary_key_columns.contains(&column_pos) {
                    csv_row_key_left.push_key_column(field_left);
                    csv_row_key_right.push_key_column(field_right);
                }

            }
        }
        Ok(DiffResult::new())
    }
}