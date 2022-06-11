use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use crossbeam_channel::{unbounded, Receiver, Sender};
use csv::{Error, Reader};
#[cfg(feature = "rayon-threads")]
use mown::Mown;

use crate::csv_parse_result::{
    CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight,
};
#[cfg(feature = "crossbeam-threads")]
use crate::thread_scope_strategy::CrossbeamScope;
#[cfg(feature = "rayon-threads")]
use crate::thread_scope_strategy::RayonScope;
use crate::{
    csv::Csv,
    csv_hash_receiver_comparer::CsvHashReceiverStreamComparer,
    csv_parse_result::{CsvByteRecordWithHash, RecordHashWithPosition},
    csv_parser_hasher::{CsvParserHasherLinesSender, CsvParserHasherSender},
    diff_result::{DiffByteRecords, DiffByteRecordsIterator},
    thread_scope_strategy::ThreadScoper,
};

pub struct CsvHashTaskSendersWithRecycleReceiver<R: Read> {
    sender: Sender<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
    csv: Csv<R>,
    receiver_recycle_csv: Receiver<csv::ByteRecord>,
}

impl<R: Read> CsvHashTaskSendersWithRecycleReceiver<R> {
    pub(crate) fn new(
        sender: Sender<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
        csv: Csv<R>,
        receiver_recycle_csv: Receiver<csv::ByteRecord>,
    ) -> Self {
        Self {
            sender,
            csv,
            receiver_recycle_csv,
        }
    }
}

pub struct CsvHashTaskLineSenders<R: Read> {
    sender: Sender<CsvLeftRightParseResult<RecordHashWithPosition>>,
    sender_total_lines: Sender<u64>,
    sender_csv_reader: Sender<csv::Result<Reader<R>>>,
    csv: Csv<R>,
}

impl<R: Read> CsvHashTaskLineSenders<R> {
    pub(crate) fn new(
        sender: Sender<CsvLeftRightParseResult<RecordHashWithPosition>>,
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
    fn spawn_hashing_tasks_and_send_result<R: Read + Send + 'static>(
        self,
        csv_hash_task_senders_left: CsvHashTaskSendersWithRecycleReceiver<R>,
        csv_hash_task_senders_right: CsvHashTaskSendersWithRecycleReceiver<R>,
        csv_hash_receiver_comparer: CsvHashReceiverStreamComparer,
        primary_key_columns: HashSet<usize>,
    ) -> (Self, Receiver<DiffByteRecordsIterator>)
    where
        // TODO: this bound is only necessary, because we are returning `self` here;
        // maybe we can do it differently
        Self: Sized;

    fn parse_hash_and_send_for_compare<R, P>(
        csv_hash_task_senders: CsvHashTaskSendersWithRecycleReceiver<R>,
        primary_key_columns: HashSet<usize>,
    ) -> csv::Result<()>
    where
        R: Read + Send,
        P: CsvParseResult<CsvLeftRightParseResult<CsvByteRecordWithHash>, CsvByteRecordWithHash>,
    {
        let mut csv_parser_hasher: CsvParserHasherSender<
            CsvLeftRightParseResult<CsvByteRecordWithHash>,
        > = CsvParserHasherSender::new(csv_hash_task_senders.sender);
        csv_parser_hasher.parse_and_hash::<R, P>(
            csv_hash_task_senders.csv,
            &primary_key_columns,
            csv_hash_task_senders.receiver_recycle_csv,
        )
    }
}

#[derive(Debug)]
#[cfg(feature = "rayon-threads")]
pub struct CsvHashTaskSpawnerRayon<'tp> {
    thread_pool: Mown<'tp, rayon::ThreadPool>,
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerRayon<'tp> {
    pub fn with_thread_pool_ref(thread_pool: &'tp rayon::ThreadPool) -> Self {
        Self {
            thread_pool: Mown::Borrowed(thread_pool),
        }
    }

    pub fn with_thread_pool_owned(thread_pool: rayon::ThreadPool) -> Self {
        Self {
            thread_pool: Mown::Owned(thread_pool),
        }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerRayon<'static> {
    fn spawn_hashing_tasks_and_send_result<R: Read + Send + 'static>(
        self,
        csv_hash_task_senders_left: CsvHashTaskSendersWithRecycleReceiver<R>,
        csv_hash_task_senders_right: CsvHashTaskSendersWithRecycleReceiver<R>,
        csv_hash_receiver_comparer: CsvHashReceiverStreamComparer,
        primary_key_columns: HashSet<usize>,
    ) -> (Self, Receiver<DiffByteRecordsIterator>) {
        let (sender, receiver) = unbounded();

        let prim_key_columns_clone = primary_key_columns.clone();

        self.thread_pool.spawn(move || {
            sender
                .send(csv_hash_receiver_comparer.recv_hashes_and_compare())
                .unwrap();
        });

        self.thread_pool.spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultLeft<CsvByteRecordWithHash>>(
                csv_hash_task_senders_left,
                primary_key_columns,
            );
        });

        self.thread_pool.spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultRight<CsvByteRecordWithHash>>(
                csv_hash_task_senders_right,
                prim_key_columns_clone,
            );
        });

        (self, receiver)
    }
}

pub trait CsvHashTaskSpawnerLocal {
    fn spawn_hashing_tasks_and_send_result<R: Read + Seek + Send>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskLineSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskLineSenders<R>,
        primary_key_columns: &HashSet<usize>,
    );

    fn parse_hash_and_send_for_compare<R, P>(
        csv_hash_task_senders: CsvHashTaskLineSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
        P: CsvParseResult<CsvLeftRightParseResult<RecordHashWithPosition>, RecordHashWithPosition>,
    {
        let mut csv_parser_hasher: CsvParserHasherLinesSender<
            CsvLeftRightParseResult<RecordHashWithPosition>,
        > = CsvParserHasherLinesSender::new(
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
pub struct CsvHashTaskSpawnerLocalRayon<'tp> {
    thread_scoper: RayonScope<'tp>,
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerLocalRayon<'tp> {
    pub(crate) fn new(thread_scoper: RayonScope<'tp>) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawnerLocal for CsvHashTaskSpawnerLocalRayon<'_> {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskLineSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskLineSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
    {
        self.thread_scoper.scope(move |s| {
            s.spawn(move |ss| {
                ss.spawn(move |_s1| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultLeft<RecordHashWithPosition>,
                    >(csv_hash_task_senders_left, primary_key_columns);
                });
                ss.spawn(move |_s2| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultRight<RecordHashWithPosition>,
                    >(csv_hash_task_senders_right, primary_key_columns);
                });
            });
        });
    }
}

#[derive(Debug)]
#[cfg(feature = "crossbeam-threads")]
pub struct CsvHashTaskSpawnerLocalCrossbeam {
    thread_scoper: CrossbeamScope,
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerLocalCrossbeam {
    pub(crate) fn new(thread_scoper: CrossbeamScope) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerLocal for CsvHashTaskSpawnerLocalCrossbeam {
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskLineSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskLineSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send,
    {
        self.thread_scoper.scope(move |s| {
            s.spawn(move |ss| {
                ss.spawn(move |_s1| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultLeft<RecordHashWithPosition>,
                    >(csv_hash_task_senders_left, primary_key_columns);
                });
                ss.spawn(move |_s2| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultRight<RecordHashWithPosition>,
                    >(csv_hash_task_senders_right, primary_key_columns);
                });
            });
        });
    }
}

pub trait CsvHashTaskSpawnerLocalBuilder<T> {
    fn build(self) -> T;
}

#[cfg(feature = "rayon-threads")]
pub struct CsvHashTaskSpawnerLocalBuilderRayon<'tp> {
    thread_pool: &'tp rayon::ThreadPool,
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerLocalBuilderRayon<'tp> {
    pub fn new(thread_pool: &'tp rayon::ThreadPool) -> Self {
        Self { thread_pool }
    }
}

#[cfg(feature = "rayon-threads")]
impl<'tp> CsvHashTaskSpawnerLocalBuilder<CsvHashTaskSpawnerLocalRayon<'tp>>
    for CsvHashTaskSpawnerLocalBuilderRayon<'tp>
{
    fn build(self) -> CsvHashTaskSpawnerLocalRayon<'tp> {
        CsvHashTaskSpawnerLocalRayon::new(RayonScope::with_thread_pool_ref(self.thread_pool))
    }
}

#[cfg(feature = "crossbeam-threads")]
pub struct CsvHashTaskSpawnerLocalBuilderCrossbeam;

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerLocalBuilderCrossbeam {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "crossbeam-threads")]
impl CsvHashTaskSpawnerLocalBuilder<CsvHashTaskSpawnerLocalCrossbeam>
    for CsvHashTaskSpawnerLocalBuilderCrossbeam
{
    fn build(self) -> CsvHashTaskSpawnerLocalCrossbeam {
        CsvHashTaskSpawnerLocalCrossbeam::new(CrossbeamScope::new())
    }
}
