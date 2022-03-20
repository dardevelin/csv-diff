use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use csv::{Error, Reader};

#[cfg(feature = "crossbeam-threads")]
use crate::thread_scope_strategy::CrossbeamScope;
#[cfg(feature = "rayon-threads")]
use crate::thread_scope_strategy::RayonScope;
use crate::{
    csv::Csv, csv_parse_result::RecordHash, csv_parser_hasher::CsvParserHasherSender,
    diff_result::DiffByteRecordsIterator, thread_scope_strategy::ThreadScoper,
};
use crate::{
    csv_hash_receiver_comparer::{self, CsvHashReceiverComparer},
    csv_parse_result::{
        CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight,
    },
};

pub struct CsvHashTaskSenders<R: Read> {
    sender: Sender<CsvLeftRightParseResult>,
    sender_total_lines: Sender<u64>,
    sender_csv_reader: Sender<csv::Result<Reader<R>>>,
    csv: Csv<R>,
}

impl<R: Read> CsvHashTaskSenders<R> {
    pub(crate) fn new(
        sender: Sender<CsvLeftRightParseResult>,
        sender_total_lines: Sender<u64>,
        sender_csv_reader: Sender<csv::Result<Reader<R>>>,
        csv: Csv<R>,
    ) -> Self {
        Self {
            sender,
            sender_total_lines,
            sender_csv_reader,
            csv,
        }
    }
}

pub trait CsvHashTaskSpawner {
    fn spawn_hashing_tasks_and_send_result<R: Read + Seek + Send>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskSenders<R>,
        csv_hash_receiver_comparer: CsvHashReceiverComparer<R>,
        primary_key_columns: &HashSet<usize>,
    ) -> Receiver<csv::Result<DiffByteRecordsIterator<R>>>;

    fn parse_hash_and_send_for_compare<R, P>(
        &self,
        csv_hash_task_senders: CsvHashTaskSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
        P: CsvParseResult<CsvLeftRightParseResult, RecordHash>,
    {
        let mut csv_parser_hasher: CsvParserHasherSender<CsvLeftRightParseResult> =
            CsvParserHasherSender::new(
                csv_hash_task_senders.sender,
                csv_hash_task_senders.sender_total_lines,
            );
        csv_hash_task_senders
            .sender_csv_reader
            .send(
                csv_parser_hasher
                    .parse_and_hash::<R, P>(csv_hash_task_senders.csv, &primary_key_columns),
            )
            .unwrap();
    }
}

#[derive(Debug)]
#[cfg(feature = "rayon-threads")]
pub struct CsvHashTaskSpawnerRayon<'tp> {
    thread_scoper: RayonScope<'tp>,
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerRayon<'tp> {
    pub(crate) fn new(thread_scoper: RayonScope<'tp>) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerRayon<'_> {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskSenders<R>,
        csv_hash_receiver_comparer: CsvHashReceiverComparer<R>,
        primary_key_columns: &HashSet<usize>,
    ) -> Receiver<Result<DiffByteRecordsIterator<R>, Error>>
    where
        R: Read + Seek + Send,
    {
        let (sender, receiver) = unbounded();
        self.thread_scoper.scope(move |s| {
            s.spawn(move |ss| {
                ss.spawn(move |_s1| {
                    self.parse_hash_and_send_for_compare::<R, CsvParseResultLeft>(
                        csv_hash_task_senders_left,
                        primary_key_columns,
                    );
                });
                ss.spawn(move |_s2| {
                    self.parse_hash_and_send_for_compare::<R, CsvParseResultRight>(
                        csv_hash_task_senders_right,
                        primary_key_columns,
                    );
                });
                sender
                    .send(csv_hash_receiver_comparer.recv_hashes_and_compare())
                    .unwrap();
            });
        });
        receiver
    }
}

#[derive(Debug)]
#[cfg(feature = "crossbeam-threads")]
pub struct CsvHashTaskSpawnerCrossbeam {
    thread_scoper: CrossbeamScope,
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerCrossbeam {
    pub(crate) fn new(thread_scoper: CrossbeamScope) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerCrossbeam {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
    {
        self.thread_scoper.scope(move |s| {
            s.spawn(move |_s1| {
                self.parse_hash_and_send_for_compare::<R, CsvParseResultLeft>(
                    csv_hash_task_senders_left,
                    primary_key_columns,
                );
            });
            s.spawn(move |_s2| {
                self.parse_hash_and_send_for_compare::<R, CsvParseResultRight>(
                    csv_hash_task_senders_right,
                    primary_key_columns,
                );
            });
        });
    }
}

pub trait CsvHashTaskSpawnerBuilder<T> {
    fn build(self) -> T;
}

#[cfg(feature = "rayon-threads")]
pub struct CsvHashTaskSpawnerBuilderRayon<'tp> {
    thread_pool: &'tp rayon::ThreadPool,
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerBuilderRayon<'tp> {
    pub fn new(thread_pool: &'tp rayon::ThreadPool) -> Self {
        Self { thread_pool }
    }
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerRayon<'tp>>
    for CsvHashTaskSpawnerBuilderRayon<'tp>
{
    fn build(self) -> CsvHashTaskSpawnerRayon<'tp> {
        CsvHashTaskSpawnerRayon::new(RayonScope::with_thread_pool_ref(self.thread_pool))
    }
}

#[cfg(feature = "crossbeam-threads")]
pub struct CsvHashTaskSpawnerBuilderCrossbeam;

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerBuilderCrossbeam {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerCrossbeam> for CsvHashTaskSpawnerBuilderCrossbeam {
    fn build(self) -> CsvHashTaskSpawnerCrossbeam {
        CsvHashTaskSpawnerCrossbeam::new(CrossbeamScope::new())
    }
}
