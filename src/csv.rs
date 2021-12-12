use std::io::{Read, Seek};

pub struct Csv<R: Read + Seek + Send> {
    reader: R,
    headers: bool,
}

impl<R: Read + Seek + Send> Csv<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            headers: true,
        }
    }
}

impl<R: Read + Send + Seek> From<Csv<R>> for csv::Reader<R> {
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

impl<R: Read + Send + Seek> CsvBuilder<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            headers: true,
        }
    }

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
