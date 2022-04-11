use std::io::{Cursor, Read, Seek};

#[derive(Clone)]
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

impl<R: Read + Send> Csv<R> {
    pub fn with_reader(reader: R) -> Self {
        Self {
            reader,
            headers: true,
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

#[derive(Clone)]
pub struct IoArcAsRef<T>(pub io_arc::IoArc<T>)
where
    T: AsRef<[u8]> + Send;

impl<T> AsRef<[u8]> for IoArcAsRef<T>
where
    T: AsRef<[u8]> + Send,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}
