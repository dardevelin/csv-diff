use std::hash::Hasher;
use xxhash_rust::xxh3::{xxh3_128, Xxh3};

pub(crate) trait CsvHasherExt {
    fn hash_key_fields(&self, key_fields_idx: &[usize]) -> u128;

    fn hash_record(&self) -> u128;
}

impl CsvHasherExt for csv::ByteRecord {
    #[inline]
    fn hash_key_fields(&self, key_fields_idx: &[usize]) -> u128 {
        let mut hasher = Xxh3::new();
        let key_fields = key_fields_idx.iter().filter_map(|k_idx| self.get(*k_idx));

        // TODO: try to do it with as few calls to `write` as possible
        // in order to still be efficient and do as few `write` calls as possible
        // consider using `csv_record.range(...)` method
        for key_field in key_fields {
            hasher.write(key_field);
        }
        hasher.digest128()
    }

    #[inline]
    fn hash_record(&self) -> u128 {
        // TODO: don't hash all of it -> exclude the key fields
        // in order to still be efficient and do as few `write` calls as possible
        // consider using `csv_record.range(...)` method
        xxh3_128(self.as_slice())
    }
}
