use std::{
    collections::HashSet,
    io::{Read, Seek},
    ops::Deref,
    sync::Arc,
};

use crossbeam_channel::{bounded, Receiver, Sender};
use csv::Reader;
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
    diff_result::DiffByteRecordsIterator,
    thread_scope_strategy::ThreadScoper,
};

pub struct CsvHashTaskSenderWithRecycleReceiver<R: Read> {
    sender: Sender<CsvLeftRightParseResult<CsvByteRecordWithHash>>,
    csv: Csv<R>,
    receiver_recycle_csv: Receiver<csv::ByteRecord>,
}

impl<R: Read> CsvHashTaskSenderWithRecycleReceiver<R> {
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
        csv_hash_task_sender_left: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_task_sender_right: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_receiver_comparer: CsvHashReceiverStreamComparer,
        primary_key_columns: HashSet<usize>,
    ) -> (Self, Receiver<DiffByteRecordsIterator>)
    where
        // TODO: this bound is only necessary, because we are returning `self` here;
        // maybe we can do it differently
        Self: Sized;

    fn parse_hash_and_send_for_compare<R, P>(
        csv_hash_task_sender: CsvHashTaskSenderWithRecycleReceiver<R>,
        primary_key_columns: HashSet<usize>,
    ) where
        R: Read + Send,
        P: CsvParseResult<CsvLeftRightParseResult<CsvByteRecordWithHash>, CsvByteRecordWithHash>,
    {
        let mut csv_parser_hasher: CsvParserHasherSender<
            CsvLeftRightParseResult<CsvByteRecordWithHash>,
        > = CsvParserHasherSender::new(csv_hash_task_sender.sender);
        csv_parser_hasher.parse_and_hash::<R, P>(
            csv_hash_task_sender.csv,
            &primary_key_columns,
            csv_hash_task_sender.receiver_recycle_csv,
        )
    }
}

#[derive(Debug)]
#[cfg(feature = "rayon-threads")]
pub struct CsvHashTaskSpawnerRayon {
    thread_pool: OwnOrArc<rayon::ThreadPool>,
}

#[derive(Debug)]
enum OwnOrArc<T> {
    Arced(Arc<T>),
    Owned(T),
}

impl<T> Deref for OwnOrArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Arced(t) => &*t,
            Self::Owned(t) => t,
        }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawnerRayon {
    pub fn with_thread_pool_arc(thread_pool: Arc<rayon::ThreadPool>) -> Self {
        Self {
            thread_pool: OwnOrArc::Arced(thread_pool),
        }
    }

    pub fn with_thread_pool_owned(thread_pool: rayon::ThreadPool) -> Self {
        Self {
            thread_pool: OwnOrArc::Owned(thread_pool),
        }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerRayon {
    fn spawn_hashing_tasks_and_send_result<R: Read + Send + 'static>(
        self,
        csv_hash_task_sender_left: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_task_sender_right: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_receiver_comparer: CsvHashReceiverStreamComparer,
        primary_key_columns: HashSet<usize>,
    ) -> (Self, Receiver<DiffByteRecordsIterator>) {
        let (sender, receiver) = bounded(1);

        let prim_key_columns_clone = primary_key_columns.clone();

        self.thread_pool.spawn(move || {
            sender
                .send(csv_hash_receiver_comparer.recv_hashes_and_compare())
                .unwrap();
        });

        self.thread_pool.spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultLeft<CsvByteRecordWithHash>>(
                csv_hash_task_sender_left,
                primary_key_columns,
            );
        });

        self.thread_pool.spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultRight<CsvByteRecordWithHash>>(
                csv_hash_task_sender_right,
                prim_key_columns_clone,
            );
        });

        (self, receiver)
    }
}

#[derive(Debug, Default)]
pub struct CsvHashTaskSpawnerStdThreads;

impl CsvHashTaskSpawnerStdThreads {
    pub(crate) fn new() -> Self {
        Self
    }
}

impl CsvHashTaskSpawner for CsvHashTaskSpawnerStdThreads {
    fn spawn_hashing_tasks_and_send_result<R: Read + Send + 'static>(
        self,
        csv_hash_task_sender_left: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_task_sender_right: CsvHashTaskSenderWithRecycleReceiver<R>,
        csv_hash_receiver_comparer: CsvHashReceiverStreamComparer,
        primary_key_columns: HashSet<usize>,
    ) -> (Self, Receiver<DiffByteRecordsIterator>)
    where
        Self: Sized,
    {
        let (sender, receiver) = bounded(1);

        let prim_key_columns_clone = primary_key_columns.clone();

        std::thread::spawn(move || {
            sender
                .send(csv_hash_receiver_comparer.recv_hashes_and_compare())
                .unwrap();
        });

        std::thread::spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultLeft<CsvByteRecordWithHash>>(
                csv_hash_task_sender_left,
                primary_key_columns,
            );
        });

        std::thread::spawn(move || {
            Self::parse_hash_and_send_for_compare::<R, CsvParseResultRight<CsvByteRecordWithHash>>(
                csv_hash_task_sender_right,
                prim_key_columns_clone,
            );
        });

        (self, receiver)
    }
}

pub trait CsvHashTaskSpawnerBuilder<T> {
    fn build(self) -> T;
}

#[derive(Debug, Default)]
pub struct CsvHashTaskSpawnerBuilderStdThreads;

impl CsvHashTaskSpawnerBuilderStdThreads {
    pub fn new() -> Self {
        Self
    }
}

impl CsvHashTaskSpawnerBuilder<CsvHashTaskSpawnerStdThreads>
    for CsvHashTaskSpawnerBuilderStdThreads
{
    fn build(self) -> CsvHashTaskSpawnerStdThreads {
        CsvHashTaskSpawnerStdThreads::new()
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
                    .parse_and_hash::<R, P>(csv_hash_task_senders.csv, primary_key_columns),
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
            s.spawn(move |inner_scope| {
                inner_scope.spawn(move |_s1| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultLeft<RecordHashWithPosition>,
                    >(csv_hash_task_senders_left, primary_key_columns);
                });
                inner_scope.spawn(move |_s2| {
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
            s.spawn(move |inner_scope| {
                inner_scope.spawn(move |_s1| {
                    Self::parse_hash_and_send_for_compare::<
                        R,
                        CsvParseResultLeft<RecordHashWithPosition>,
                    >(csv_hash_task_senders_left, primary_key_columns);
                });
                inner_scope.spawn(move |_s2| {
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
