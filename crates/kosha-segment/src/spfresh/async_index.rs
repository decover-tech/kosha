use std::sync::{mpsc, Arc, RwLock};
use std::thread::{self, JoinHandle};

use kosha_core::KoshaError;

use super::index::SpFreshIndex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalRebuildJob {
    Split(u32),
    Merge(u32),
    Reassign,
    Stabilize,
    Stop,
}

pub struct SpFreshAsyncIndex {
    index: Arc<RwLock<SpFreshIndex>>,
    jobs: mpsc::Sender<LocalRebuildJob>,
    worker: Option<JoinHandle<()>>,
}

impl SpFreshAsyncIndex {
    pub fn new(index: SpFreshIndex) -> Self {
        let index = Arc::new(RwLock::new(index));
        let (tx, rx) = mpsc::channel();
        let worker_index = Arc::clone(&index);
        let worker = thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                if job == LocalRebuildJob::Stop {
                    break;
                }
                let mut index = worker_index.write().expect("spfresh worker lock poisoned");
                match job {
                    LocalRebuildJob::Split(posting_id) => {
                        if let Some(idx) = index.postings.iter().position(|p| p.id == posting_id) {
                            index.split_posting(idx);
                        }
                        index.stabilize_assignments();
                    }
                    LocalRebuildJob::Merge(posting_id) => {
                        if index.postings.iter().any(|p| p.id == posting_id) {
                            index.merge_underfull();
                        }
                        index.stabilize_assignments();
                    }
                    LocalRebuildJob::Reassign | LocalRebuildJob::Stabilize => {
                        index.stabilize_assignments();
                    }
                    LocalRebuildJob::Stop => {}
                }
            }
        });
        Self {
            index,
            jobs: tx,
            worker: Some(worker),
        }
    }

    pub fn insert(&self, doc_seq: u32, vector: Vec<f32>) -> Result<(), KoshaError> {
        let mut index = self
            .index
            .write()
            .expect("spfresh foreground lock poisoned");
        index.foreground_insert(doc_seq, vector)?;
        let overfull: Vec<u32> = index
            .postings
            .iter()
            .filter(|posting| index.live_entry_count(posting) > index.options.max_posting_len)
            .map(|posting| posting.id)
            .collect();
        drop(index);
        for posting_id in overfull {
            let _ = self.jobs.send(LocalRebuildJob::Split(posting_id));
        }
        let _ = self.jobs.send(LocalRebuildJob::Stabilize);
        Ok(())
    }

    pub fn delete(&self, doc_seq: u32) -> bool {
        let mut index = self
            .index
            .write()
            .expect("spfresh foreground lock poisoned");
        let deleted = index.foreground_delete(doc_seq);
        drop(index);
        if deleted {
            let _ = self.jobs.send(LocalRebuildJob::Merge(0));
            let _ = self.jobs.send(LocalRebuildJob::Stabilize);
        }
        deleted
    }

    pub fn search(&self, query: &[f32], k: usize, candidate_postings: usize) -> Vec<(u32, f64)> {
        self.index
            .read()
            .expect("spfresh search lock poisoned")
            .search(query, k, candidate_postings)
    }

    pub fn snapshot(&self) -> SpFreshIndex {
        self.index
            .read()
            .expect("spfresh snapshot lock poisoned")
            .clone()
    }

    pub fn rebuild_now(&self) {
        let mut index = self.index.write().expect("spfresh rebuild lock poisoned");
        index.merge_underfull();
        index.stabilize_assignments();
    }
}

impl Drop for SpFreshAsyncIndex {
    fn drop(&mut self) {
        let _ = self.jobs.send(LocalRebuildJob::Stop);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
