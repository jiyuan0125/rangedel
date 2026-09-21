//! Range tombstones.
//!
//! A range tombstone records the deletion of every key in a bounded interval
//! at a particular sequence number. Unlike a point
//! [`crate::types::RowEntry`] tombstone, one range tombstone covers an
//! arbitrary number of keys, so the write, WAL and on-disk cost of deleting a
//! key interval stays O(1) in the number of deleted keys.
//!
//! Bounds use the same inclusive/exclusive/unbounded semantics as
//! [`crate::bytes_range::BytesRange`] and the `scan`/`delete_range` APIs:
//! `[a, b)`, `[a, b]`, `a..`, `..=b`, `..`, ...

use std::ops::{Bound, RangeBounds};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::bytes_range::BytesRange;

/// One range deletion: all keys within `range` are deleted as of sequence
/// `seq`. Writes (and point tombstones) with a larger sequence number still
/// shadow this deletion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RangeTombstone {
    pub(crate) range: BytesRange,
    pub(crate) seq: u64,
}

impl RangeTombstone {
    pub(crate) fn new(range: BytesRange, seq: u64) -> Self {
        Self { range, seq }
    }

    /// True iff this tombstone hides `entry`.
    ///
    /// The comparison is strictly less than (`<`): a write and a range delete
    /// in the same write batch share one commit sequence number, and the point
    /// write must win over the interval deletion when both target the same
    /// key. Reads filter entries above the visibility boundary first, so this
    /// remains correct across snapshots.
    pub(crate) fn covers_entry(&self, entry: &crate::types::RowEntry) -> bool {
        entry.seq < self.seq && self.range.contains(entry.key.as_ref())
    }

    /// Fixed-width little-endian layout:
    ///
    /// ```text
    /// | u64 seq | u8 start_flag | u32 start_len | start |
    /// | u8 end_flag | u32 end_len | end |
    /// ```
    ///
    /// Bound flags: `0` unbounded (length must be 0), `1` included,
    /// `2` excluded.
    pub(crate) fn encode_value(&self) -> Bytes {
        let start_flag = bound_flag(self.range.start_bound());
        let end_flag = bound_flag(self.range.end_bound());
        let start = bound_bytes(self.range.start_bound());
        let end = bound_bytes(self.range.end_bound());
        let mut buf = BytesMut::with_capacity(8 + 1 + 4 + start.len() + 1 + 4 + end.len());
        buf.put_u64(self.seq);
        buf.put_u8(start_flag);
        buf.put_u32(start.len() as u32);
        buf.put_slice(&start);
        buf.put_u8(end_flag);
        buf.put_u32(end.len() as u32);
        buf.put_slice(&end);
        buf.freeze()
    }

    pub(crate) fn decode_value(data: &mut Bytes) -> Result<Self, SlateRangeDecodeError> {
        if data.remaining() < 8 {
            return Err(SlateRangeDecodeError::Truncated);
        }
        let seq = data.get_u64();
        let start = decode_bound(data)?;
        let end = decode_bound(data)?;
        Ok(Self {
            range: BytesRange::new(start, end),
            seq,
        })
    }

    /// Encode the interval into a row's value area. The row key carries the
    /// interval start; this payload carries the start flag, end flag and end
    /// key. The row's own `seq` field is authoritative, so `seq` here is
    /// omitted.
    pub(crate) fn encode_row_payload(start_inclusive: bool, end: Bound<&Bytes>) -> Bytes {
        let end_flag = bound_flag(end);
        let end_bytes = bound_bytes(end);
        let mut buf = BytesMut::with_capacity(1 + 1 + 4 + end_bytes.len());
        buf.put_u8(u8::from(start_inclusive));
        buf.put_u8(end_flag);
        buf.put_u32(end_bytes.len() as u32);
        buf.put_slice(&end_bytes);
        buf.freeze()
    }

    pub(crate) fn decode_row_payload(
        data: &mut Bytes,
    ) -> Result<(bool, Bound<Bytes>), SlateRangeDecodeError> {
        if data.remaining() < 6 {
            return Err(SlateRangeDecodeError::Truncated);
        }
        let start_inclusive = data.get_u8() != 0;
        let end = decode_bound(data)?;
        Ok((start_inclusive, end))
    }

    /// Convert a row-key/flag pair into the interval start bound. An empty key
    /// with an excluded start denotes an unbounded start.
    pub(crate) fn start_bound_from_row_key(key: Bytes, start_inclusive: bool) -> Bound<Bytes> {
        if !start_inclusive && key.is_empty() {
            Bound::Unbounded
        } else if start_inclusive {
            Bound::Included(key)
        } else {
            Bound::Excluded(key)
        }
    }

    /// Encode an interval start bound as the row key plus the inclusive flag.
    pub(crate) fn row_key_from_start_bound(start: Bound<Bytes>) -> (Bytes, bool) {
        match start {
            Bound::Included(key) => (key, true),
            Bound::Excluded(key) => (key, false),
            Bound::Unbounded => (Bytes::new(), false),
        }
    }
}

fn bound_flag(bound: Bound<&Bytes>) -> u8 {
    match bound {
        Bound::Unbounded => 0,
        Bound::Included(_) => 1,
        Bound::Excluded(_) => 2,
    }
}

fn bound_bytes(bound: Bound<&Bytes>) -> Bytes {
    match bound {
        Bound::Included(b) | Bound::Excluded(b) => b.clone(),
        Bound::Unbounded => Bytes::new(),
    }
}

fn decode_bound(data: &mut Bytes) -> Result<Bound<Bytes>, SlateRangeDecodeError> {
    if data.remaining() < 5 {
        return Err(SlateRangeDecodeError::Truncated);
    }
    let flag = data.get_u8();
    let len = data.get_u32() as usize;
    if data.remaining() < len {
        return Err(SlateRangeDecodeError::Truncated);
    }
    let key = data.copy_to_bytes(len);
    match flag {
        0 => {
            if !key.is_empty() {
                return Err(SlateRangeDecodeError::InvalidFlag);
            }
            Ok(Bound::Unbounded)
        }
        1 => Ok(Bound::Included(key)),
        2 => Ok(Bound::Excluded(key)),
        _ => Err(SlateRangeDecodeError::InvalidFlag),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SlateRangeDecodeError {
    Truncated,
    InvalidFlag,
}

impl std::fmt::Display for SlateRangeDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "range tombstone record is truncated"),
            Self::InvalidFlag => {
                write!(f, "range tombstone record has an invalid bound flag")
            }
        }
    }
}

impl std::error::Error for SlateRangeDecodeError {}

/// Compaction/flush SSTs persist range tombstones in a dedicated side block.
/// The block is a sequence of length-prefixed [`RangeTombstone::encode_value`]
/// payloads. Compression and the block checksum are applied by the same
/// machinery used for stats blocks.
pub(crate) fn encode_range_tombstones(tombstones: &[RangeTombstone]) -> Bytes {
    let mut buf = BytesMut::new();
    for tombstone in tombstones {
        let value = tombstone.encode_value();
        buf.put_u32(value.len() as u32);
        buf.put_slice(&value);
    }
    buf.freeze()
}

pub(crate) fn decode_range_tombstones(
    mut data: Bytes,
) -> Result<Vec<RangeTombstone>, SlateRangeDecodeError> {
    let mut tombstones = Vec::new();
    while data.has_remaining() {
        if data.remaining() < 4 {
            return Err(SlateRangeDecodeError::Truncated);
        }
        let len = data.get_u32() as usize;
        if data.remaining() < len {
            return Err(SlateRangeDecodeError::Truncated);
        }
        let mut record = data.split_to(len);
        tombstones.push(RangeTombstone::decode_value(&mut record)?);
    }
    Ok(tombstones)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_all_bound_shapes() {
        let ranges = vec![
            BytesRange::new(
                Bound::Included(Bytes::from_static(b"a")),
                Bound::Excluded(Bytes::from_static(b"z")),
            ),
            BytesRange::new(
                Bound::Included(Bytes::from_static(b"a")),
                Bound::Included(Bytes::from_static(b"z")),
            ),
            BytesRange::new(Bound::Included(Bytes::from_static(b"a")), Bound::Unbounded),
            BytesRange::new(Bound::Unbounded, Bound::Included(Bytes::from_static(b"z"))),
            BytesRange::new(Bound::Unbounded, Bound::Unbounded),
        ];
        for range in ranges {
            let tombstone = RangeTombstone::new(range.clone(), 42);
            let encoded = encode_range_tombstones(std::slice::from_ref(&tombstone));
            let decoded = decode_range_tombstones(encoded).unwrap();
            assert_eq!(decoded, vec![tombstone]);
        }
    }

    #[test]
    fn covers_keys_inside_the_interval_only() {
        let tombstone = RangeTombstone::new(
            BytesRange::from(Bytes::from_static(b"k2")..=Bytes::from_static(b"k4")),
            5,
        );
        assert!(tombstone.range.contains(&Bytes::from_static(b"k2")));
        assert!(tombstone.range.contains(&Bytes::from_static(b"k3")));
        assert!(tombstone.range.contains(&Bytes::from_static(b"k4")));
        assert!(!tombstone.range.contains(&Bytes::from_static(b"k1")));
        assert!(!tombstone.range.contains(&Bytes::from_static(b"k5")));

        let half_open = RangeTombstone::new(
            BytesRange::from(Bytes::from_static(b"k2")..Bytes::from_static(b"k4")),
            5,
        );
        assert!(half_open.range.contains(&Bytes::from_static(b"k2")));
        assert!(!half_open.range.contains(&Bytes::from_static(b"k4")));
    }
}
