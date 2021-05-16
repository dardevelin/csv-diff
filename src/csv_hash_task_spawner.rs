use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use crossbeam_channel::Sender;
use csv::Reader;

use crate::{
    csv_parser_hasher::{
        CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight,
        CsvParserHasherSender, RecordHash, StackVec,
    },
    thread_scope_strategy::{RayonScope, ThreadScoper},
};

pub trait CsvHashTaskSpawner {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        sender_left: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines_left: Sender<u64>,
        sender_csv_reader_left: Sender<Reader<R>>,
        csv_left: R,
        sender_right: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines_right: Sender<u64>,
        sender_csv_reader_right: Sender<Reader<R>>,
        csv_right: R,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send;

    fn parse_hash_and_send_for_compare<R, P>(
        &self,
        sender: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines: Sender<u64>,
        sender_csv_reader: Sender<Reader<R>>,
        csv: R,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
        P: CsvParseResult<CsvLeftRightParseResult, RecordHash>,
    {
        let mut csv_parser_hasher: CsvParserHasherSender<CsvLeftRightParseResult> =
            CsvParserHasherSender::new(sender, sender_total_lines);
        sender_csv_reader
            .send(csv_parser_hasher.parse_and_hash::<R, P>(csv, &primary_key_columns))
            .unwrap();
    }
}

#[derive(Debug)]
pub struct CsvHashTaskSpawnerRayon {
    thread_scoper: RayonScope,
}

impl CsvHashTaskSpawnerRayon {
    pub fn new(thread_scoper: RayonScope) -> Self {
        Self { thread_scoper }
    }
}

impl CsvHashTaskSpawner for CsvHashTaskSpawnerRayon {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        sender_left: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines_left: Sender<u64>,
        sender_csv_reader_left: Sender<Reader<R>>,
        csv_left: R,
        sender_right: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines_right: Sender<u64>,
        sender_csv_reader_right: Sender<Reader<R>>,
        csv_right: R,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
    {
        self.thread_scoper.scope(move |s| {
            s.spawn(move |_s1| {
                self.parse_hash_and_send_for_compare::<R, CsvParseResultLeft>(
                    sender_left,
                    sender_total_lines_left,
                    sender_csv_reader_left,
                    csv_left,
                    primary_key_columns,
                );
            });
            s.spawn(move |_s2| {
                self.parse_hash_and_send_for_compare::<R, CsvParseResultRight>(
                    sender_right,
                    sender_total_lines_right,
                    sender_csv_reader_right,
                    csv_right,
                    primary_key_columns,
                );
            });
        });
    }
}
