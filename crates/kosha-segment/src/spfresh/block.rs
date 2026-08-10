use std::collections::{HashMap, VecDeque};

use super::types::SpFreshEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingBlockMapping {
    pub generation: u64,
    pub entry_count: usize,
    pub block_ids: Vec<u64>,
}

#[derive(Debug)]
pub struct SpFreshBlockController {
    block_capacity: usize,
    next_block_id: u64,
    free_blocks: VecDeque<u64>,
    blocks: HashMap<u64, Vec<SpFreshEntry>>,
    mapping: HashMap<u32, PostingBlockMapping>,
}

impl SpFreshBlockController {
    pub fn new(block_capacity: usize) -> Self {
        Self {
            block_capacity: block_capacity.max(1),
            next_block_id: 0,
            free_blocks: VecDeque::new(),
            blocks: HashMap::new(),
            mapping: HashMap::new(),
        }
    }

    pub fn get(&self, posting_id: u32) -> Option<Vec<SpFreshEntry>> {
        let mapping = self.mapping.get(&posting_id)?;
        let mut entries = Vec::with_capacity(mapping.entry_count);
        for block_id in &mapping.block_ids {
            entries.extend(self.blocks.get(block_id)?.iter().cloned());
        }
        entries.truncate(mapping.entry_count);
        Some(entries)
    }

    pub fn parallel_get(&self, posting_ids: &[u32]) -> HashMap<u32, Vec<SpFreshEntry>> {
        posting_ids
            .iter()
            .filter_map(|posting_id| self.get(*posting_id).map(|entries| (*posting_id, entries)))
            .collect()
    }

    pub fn put(&mut self, posting_id: u32, entries: Vec<SpFreshEntry>) -> PostingBlockMapping {
        let old = self.mapping.remove(&posting_id);
        let generation = old.as_ref().map(|m| m.generation).unwrap_or(0);
        if let Some(old) = old {
            for block_id in old.block_ids {
                self.blocks.remove(&block_id);
                self.free_blocks.push_back(block_id);
            }
        }
        let mapping = self.write_blocks(generation, entries);
        self.mapping.insert(posting_id, mapping.clone());
        mapping
    }

    pub fn append(
        &mut self,
        posting_id: u32,
        entry: SpFreshEntry,
        expected_generation: Option<u64>,
    ) -> Result<PostingBlockMapping, PostingCasError> {
        if let Some(expected) = expected_generation {
            let actual = self
                .mapping
                .get(&posting_id)
                .map(|m| m.generation)
                .unwrap_or(0);
            if actual != expected {
                return Err(PostingCasError { expected, actual });
            }
        }
        let mut entries = self.get(posting_id).unwrap_or_default();
        entries.push(entry);
        Ok(self.put(posting_id, entries))
    }

    pub fn mapping(&self, posting_id: u32) -> Option<&PostingBlockMapping> {
        self.mapping.get(&posting_id)
    }

    fn write_blocks(&mut self, generation: u64, entries: Vec<SpFreshEntry>) -> PostingBlockMapping {
        let entry_count = entries.len();
        let mut block_ids = Vec::new();
        for chunk in entries.chunks(self.block_capacity) {
            let block_id = self.allocate_block();
            self.blocks.insert(block_id, chunk.to_vec());
            block_ids.push(block_id);
        }
        PostingBlockMapping {
            generation: generation.wrapping_add(1),
            entry_count,
            block_ids,
        }
    }

    fn allocate_block(&mut self) -> u64 {
        self.free_blocks.pop_front().unwrap_or_else(|| {
            let id = self.next_block_id;
            self.next_block_id = self.next_block_id.wrapping_add(1);
            id
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingCasError {
    pub expected: u64,
    pub actual: u64,
}
