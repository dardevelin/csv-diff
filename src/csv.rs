use std::{
    collections::HashSet,
    default,
    io::{Cursor, Read, Seek},
};

use csv::Reader;

use crate::{
    csv_hasher::CsvHasherExt,
    csv_parse_result::{CsvByteRecordWithHash, RecordHash},
};

pub struct Csv<R> {
    reader: R,
    headers: bool,
}

impl<R: Read + Seek + Send> Csv<R> {
    pub fn with_reader_seek<RSeek: CsvReadSeek<R>>(reader: RSeek) -> Self {
        Self {
            reader: reader.into_read_seek(),
            headers: true,
        }
    }
}

pub struct CsvFirstFew<R> {
    csv_reader: Reader<R>,
    num_of_bytes_hint: Option<u64>,
    pub(crate) num_of_lines_hint: Option<u64>,
    pub(crate) first_few_records: Vec<CsvByteRecordWithHash>,
}

impl<R: Read + Send> Csv<R> {
    pub fn with_reader(reader: R) -> Self {
        Self {
            reader,
            headers: true,
        }
    }
}

impl<R> CsvFirstFew<R> {
    pub fn num_of_bytes_hint(self, num_of_bytes: u64) -> Self {
        Self {
            num_of_bytes_hint: Some(num_of_bytes),
            ..self
        }
    }
}

impl<R: Read> CsvFirstFew<R> {
    pub(crate) fn approx_num_of_lines(
        &mut self,
        primary_key_columns: &HashSet<usize>,
    ) -> Result<(), csv::Error> {
        let mut count = 0usize;
        let mut byte_record = csv::ByteRecord::new();
        let mut num_of_bytes = 0usize;
        let fields_as_key: Vec<_> = primary_key_columns.iter().copied().collect();
        let mut more_csv = true;
        while more_csv && count < 2 {
            more_csv = self.csv_reader.read_byte_record(&mut byte_record)?;
            if more_csv {
                num_of_bytes += byte_record.as_slice().len();
                let key = byte_record.hash_key_fields(fields_as_key.as_slice());
                let record_hash = byte_record.hash_record();
                self.first_few_records.push(CsvByteRecordWithHash::new(
                    std::mem::replace(&mut byte_record, csv::ByteRecord::new()),
                    RecordHash::new(key, record_hash),
                ));
            }
            count += 1;
        }

        self.num_of_lines_hint = self.num_of_bytes_hint.and_then(|bytes_hint| {
            (count > 0)
                .then(|| num_of_bytes / count)
                .and_then(|avg_bytes_per_line| {
                    (avg_bytes_per_line > 0).then(|| bytes_hint / avg_bytes_per_line as u64)
                })
        });
        Ok(())
    }
}

impl<R: Read> From<Csv<R>> for CsvFirstFew<R> {
    fn from(csv: Csv<R>) -> Self {
        Self {
            csv_reader: csv.into(),
            first_few_records: Default::default(),
            num_of_bytes_hint: Default::default(),
            num_of_lines_hint: Default::default(),
        }
    }
}

pub(crate) struct CsvRemaining<R> {
    csv_reader: Reader<R>,
}

impl<R> From<CsvRemaining<R>> for csv::Reader<R> {
    fn from(csv_rem: CsvRemaining<R>) -> Self {
        csv_rem.csv_reader
    }
}

impl<R> From<CsvFirstFew<R>> for CsvRemaining<R> {
    fn from(first_few: CsvFirstFew<R>) -> Self {
        Self {
            csv_reader: first_few.csv_reader,
        }
    }
}

impl<R: Read> From<Csv<R>> for csv::Reader<R> {
    fn from(csv: Csv<R>) -> Self {
        csv::ReaderBuilder::new()
            .has_headers(csv.headers)
            .from_reader(csv.reader)
    }
}

pub struct CsvBuilder<R> {
    reader: R,
    headers: bool,
}

impl<R: Read + Seek + Send> CsvBuilder<R> {
    pub fn with_reader_seek<RSeek: CsvReadSeek<R>>(reader: RSeek) -> Self {
        Self {
            reader: reader.into_read_seek(),
            headers: true,
        }
    }
}

impl<R: Read + Send> CsvBuilder<R> {
    pub fn with_reader(reader: R) -> Self {
        Self {
            reader,
            headers: true,
        }
    }
}

impl<R> CsvBuilder<R> {
    pub fn headers(self, yes: bool) -> Self {
        Self {
            headers: yes,
            ..self
        }
    }

    pub fn build(self) -> Csv<R> {
        Csv {
            reader: self.reader,
            headers: self.headers,
        }
    }
}

/// Produces a value that implements [`Read`](std::io::Read) + [`Seek`](std::io::Seek) + [`Send`](core::marker::Send).
pub trait CsvReadSeek<R>
where
    R: Read + Seek + Send,
{
    /// Converts this value into `R`.
    fn into_read_seek(self) -> R;
}

impl<T> CsvReadSeek<Cursor<T>> for T
where
    T: AsRef<[u8]> + Send,
{
    fn into_read_seek(self) -> Cursor<T> {
        Cursor::new(self)
    }
}

impl<R> CsvReadSeek<R> for R
where
    R: Read + Seek + Send,
{
    fn into_read_seek(self) -> R {
        self
    }
}
