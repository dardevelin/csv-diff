use std::{
    collections::HashSet,
    io::{Read, Seek},
};

use crossbeam_channel::Sender;
use csv::Reader;

#[cfg(feature = "crossbeam-utils")]
use crate::thread_scope_strategy::CrossbeamScope;
#[cfg(feature = "rayon-threads")]
use crate::thread_scope_strategy::RayonScope;
use crate::{
    csv_parse_result::RecordHash, csv_parser_hasher::CsvParserHasherSender,
    thread_scope_strategy::ThreadScoper,
};
use crate::{
    csv_parse_result::{
        CsvLeftRightParseResult, CsvParseResult, CsvParseResultLeft, CsvParseResultRight,
    },
    csv_parser_hasher::StackVec,
};

pub struct CsvHashTaskSenders<R>
where
    R: Read + Seek + Send,
{
    sender: Sender<StackVec<CsvLeftRightParseResult>>,
    sender_total_lines: Sender<u64>,
    sender_csv_reader: Sender<Reader<R>>,
    csv: R,
}

impl<R> CsvHashTaskSenders<R>
where
    R: Read + Seek + Send,
{
    pub(crate) fn new(
        sender: Sender<StackVec<CsvLeftRightParseResult>>,
        sender_total_lines: Sender<u64>,
        sender_csv_reader: Sender<Reader<R>>,
        csv: R,
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
    fn spawn_hashing_tasks_and_send_result<R>(
        &self,
        csv_hash_task_senders_left: CsvHashTaskSenders<R>,
        csv_hash_task_senders_right: CsvHashTaskSenders<R>,
        primary_key_columns: &HashSet<usize>,
    ) where
        R: Read + Seek + Send;

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
pub struct CsvHashTaskSpawnerRayon {
    thread_scoper: RayonScope,
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawnerRayon {
    pub fn new(thread_scoper: RayonScope) -> Self {
        Self { thread_scoper }
    }
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawner for CsvHashTaskSpawnerRayon {
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
pub struct CsvHashTaskSpawnerBuilderRayon {
    thread_pool: rayon::ThreadPool,
}

#[cfg(feature = "rayon-threads")]
impl CsvHashTaskSpawnerBuilderRayon {
    pub fn new(thread_pool: rayon::ThreadPool) -> Self {
        Self { thread_pool }
    }
}

#[cfg(feature = "rayon-threads")]
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
