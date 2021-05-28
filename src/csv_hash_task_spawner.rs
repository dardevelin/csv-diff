use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use crossbeam_channel::Sender;
use csv::Reader;

use crate::thread_scope_strategy::ThreadScoper;

use crate::csv_parser_hasher::{
    CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight,
    CsvParserHasherSender, RecordHash, StackVec,
};
#[cfg(feature = "crossbeam-utils")]
use crate::thread_scope_strategy::CrossbeamScope;
#[cfg(feature = "rayon")]
use crate::thread_scope_strategy::RayonScope;

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
#[cfg(feature = "rayon")]
pub struct CsvHashTaskSpawnerRayon {
    thread_scoper: RayonScope,
}

#[cfg(feature = "rayon")]
impl CsvHashTaskSpawnerRayon {
    pub fn new(thread_scoper: RayonScope) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "rayon")]
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

#[derive(Debug)]
#[cfg(feature = "crossbeam-utils")]
pub struct CsvHashTaskSpawnerCrossbeam {
    thread_scoper: CrossbeamScope,
}

#[cfg(feature = "crossbeam-utils")]
impl CsvHashTaskSpawnerCrossbeam {
    pub fn new(thread_scoper: CrossbeamScope) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "crossbeam-utils")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerCrossbeam {
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

pub trait CsvHashTaskSpawnerBuilder<T> {
    fn build(self) -> T;
}

#[cfg(feature = "rayon")]
pub struct CsvHashTaskSpawnerBuilderRayon {
    thread_pool: rayon::ThreadPool,
}

#[cfg(feature = "rayon")]
impl CsvHashTaskSpawnerBuilderRayon {
    pub fn new(thread_pool: rayon::ThreadPool) -> Self {
        Self { thread_pool }
    }
}

#[cfg(feature = "rayon")]
impl CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerRayon> for CsvHashTaskSpawnerBuilderRayon {
    fn build(self) -> CsvHashTaskSpawnerRayon {
        CsvHashTaskSpawnerRayon::new(RayonScope::new(self.thread_pool))
    }
}

#[cfg(feature = "crossbeam-utils")]
pub struct CsvHashTaskSpawnerBuilderCrossbeam;

#[cfg(feature = "crossbeam-utils")]
impl CsvHashTaskSpawnerBuilderCrossbeam {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "crossbeam-utils")]
impl CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerCrossbeam> for CsvHashTaskSpawnerBuilderCrossbeam {
    fn build(self) -> CsvHashTaskSpawnerCrossbeam {
        CsvHashTaskSpawnerCrossbeam::new(CrossbeamScope::new())
    }
}
