//! Read-side range tombstone filtering.
//!
//! [`RangeTombstoneIterator`] wraps the merged point stream and a side
//! collection of [`RangeTombstone`]s. A point entry is yielded only when no
//! tombstone in the side collection hides it (same-or-older sequence number
//! and inside the interval). The wrapper supports both ascending and
//! descending scans and forwards `seek`.

use async_trait::async_trait;

#[allow(unused_imports)]
use crate::bytes_range::BytesRange as _BytesRange;
use crate::error::SlateDBError;
use crate::iter::{IterationOrder, RowEntryIterator};
use crate::range_tombstone::RangeTombstone;
use crate::types::RowEntry;

pub(crate) struct RangeTombstoneIterator<I: RowEntryIterator> {
    inner: I,
    /// All visible tombstones, sorted by descending sequence number.
    tombstones: Vec<RangeTombstone>,
}

impl<I: RowEntryIterator> RangeTombstoneIterator<I> {
    pub(crate) fn new(
        inner: I,
        mut tombstones: Vec<RangeTombstone>,
        _order: IterationOrder,
    ) -> Self {
        tombstones.sort_by(|a, b| b.seq.cmp(&a.seq));
        Self { inner, tombstones }
    }

    fn is_visible(&self, entry: &RowEntry) -> bool {
        if entry.is_range_tombstone() {
            return false;
        }
        !self
            .tombstones
            .iter()
            .any(|tombstone| tombstone.covers_entry(entry))
    }
}

#[async_trait]
impl<I: RowEntryIterator> RowEntryIterator for RangeTombstoneIterator<I> {
    async fn init(&mut self) -> Result<(), SlateDBError> {
        self.inner.init().await
    }

    async fn next(&mut self) -> Result<Option<RowEntry>, SlateDBError> {
        loop {
            let Some(entry) = self.inner.next().await? else {
                return Ok(None);
            };
            if self.is_visible(&entry) {
                return Ok(Some(entry));
            }
            tokio::task::coop::consume_budget().await;
        }
    }

    async fn seek(&mut self, next_key: &[u8]) -> Result<(), SlateDBError> {
        self.inner.seek(next_key).await
    }
}

/// Merge several tombstone collections (write batch, memtables, on-disk SSTs)
/// into one de-duplicated, descending-sequence list. De-duplication matters on
/// the flush/compaction boundary, where the same interval may briefly be
/// visible from a memtable and from an L0 SST.
pub(crate) fn merge_tombstone_collections(
    collections: impl IntoIterator<Item = Vec<RangeTombstone>>,
) -> Vec<RangeTombstone> {
    let mut merged: Vec<RangeTombstone> = collections.into_iter().flatten().collect();
    merged.sort_by(|a, b| b.seq.cmp(&a.seq).then_with(|| b.range.cmp(&a.range)));
    merged.dedup_by(|a, b| a == b);
    merged
}
