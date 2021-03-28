use core::fmt::Display;

#[derive(Debug)]
pub struct CsvGenerator {
    rows: usize,
    columns: usize,
}

impl CsvGenerator {

    pub fn new(rows: usize, columns: usize) -> Self {
        Self {
            rows,
            columns,
        }
    }

    pub fn generate(&self) -> Vec<u8> {
        use fake::{
            Faker,
            Fake,
            faker::lorem::en::*
        };
        let mut headers = (1..=self.columns).map(|col| format!("header{}", col)).collect::<Vec<_>>().join(",");
        headers.push('\n');
        
        let rows = (0..self.rows())
            .map(|row_idx| {
                let mut row: Vec<String> = Words(self.columns..self.columns + 1).fake::<Vec<String>>();
                row[0] = row_idx.to_string();
                let mut row_string = row.join(",");
                row_string.push('\n');
                row_string.into_bytes()
            })
            .flatten()
            .collect();
        rows
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
}

impl Display for CsvGenerator { 
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}x{}", self.rows, self.columns)
    }
}