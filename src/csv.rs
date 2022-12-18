use std::io::{Cursor, Read, Seek};

pub struct Csv<R> {
    csv_reader: csv::Reader<R>,
}

impl<R: Read + Seek + Send> Csv<R> {
    /// Create a new `Csv` with something that can read Csv data and implements [`CsvReadSeek`].
    /// # Example: use `Csv` together with `CsvByteDiffLocal` to compare CSV data
    #[cfg_attr(
        feature = "rayon-threads",
        doc = r##"
```
use csv_diff::{csv_diff::CsvByteDiffLocal, csv::Csv};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
let csv_data_left = "id,name,kind\n\
                    1,lemon,fruit\n\
                    2,strawberry,fruit";
let csv_data_right = "id,name,kind\n\
                    1,lemon,fruit\n\
                    2,strawberry,fruit";

let csv_byte_diff = CsvByteDiffLocal::new()?;

let mut diff_byte_records = csv_byte_diff.diff(
    // bytes are not `Seek`able by default, but trait `CsvReadSeek` makes them seekable
    Csv::with_reader_seek(csv_data_left.as_bytes()),
    Csv::with_reader_seek(csv_data_right.as_bytes()),
)?;

let num_of_rows_different = diff_byte_records.as_slice().len();

assert_eq!(
    num_of_rows_different,
    0
);
Ok(())
# }
```
    "##
    )]
    pub fn with_reader_seek<RSeek: CsvReadSeek<R>>(reader: RSeek) -> Self {
        Self {
            csv_reader: csv::Reader::from_reader(reader.into_read_seek()),
        }
    }
}

impl<R: Read> Csv<R> {
    pub fn with_reader(reader: R) -> Self {
        Self {
            csv_reader: csv::Reader::from_reader(reader),
        }
    }
}

impl<R> Csv<R> {
    pub fn into_csv_reader(self) -> csv::Reader<R> {
        self.csv_reader
    }
}

impl<R> From<csv::Reader<R>> for Csv<R> {
    fn from(rdr: csv::Reader<R>) -> Self {
        Self { csv_reader: rdr }
    }
}

/// Produces a CSV reader that implements [`Read`](std::io::Read) + [`Seek`](std::io::Seek) + [`Send`](core::marker::Send).
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

pub trait CsvReaderBuilderExt<R: Read + Seek + Send> {
    fn from_reader_seek<RSeek: CsvReadSeek<R>>(&self, reader: RSeek) -> csv::Reader<R>;
}

impl<R: Read + Seek + Send> CsvReaderBuilderExt<R> for csv::ReaderBuilder {
    fn from_reader_seek<RSeek: CsvReadSeek<R>>(&self, reader: RSeek) -> csv::Reader<R> {
        self.from_reader(reader.into_read_seek())
    }
}
