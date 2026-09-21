#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

//! Acceptance tests for `Db::delete_range` / `WriteBatch::delete_range`.

use slatedb::config::{FlushOptions, FlushType, ScanOptions, Settings, WriteOptions};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::ObjectStore;
use slatedb::IterationOrder;
use slatedb::{Db, WriteBatch};
use std::sync::Arc;

async fn open_db(path: &str, store: Arc<dyn ObjectStore>) -> Db {
    Db::open(path, store).await.unwrap()
}

fn kv(i: u32) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

#[tokio::test]
async fn test_delete_range_point_gets_and_scan() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_basic", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    let handle = db.delete_range(kv(2)..kv(5)).await.unwrap();
    handle.await_durable().await.unwrap();

    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    assert!(db.get(kv(2)).await.unwrap().is_none());
    assert!(db.get(kv(3)).await.unwrap().is_none());
    assert!(db.get(kv(4)).await.unwrap().is_none());
    assert_eq!(db.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");

    // Half-open semantics: scan k2..k5 yields nothing.
    let mut scan = db.scan(kv(2)..kv(5)).await.unwrap();
    assert!(scan.next().await.unwrap().is_none());

    // Inclusive end includes k4 (and the interval end marker is still k5).
    let mut full_scan = db.scan(..).await.unwrap();
    let mut remaining = Vec::new();
    while let Some(entry) = full_scan.next().await.unwrap() {
        remaining.push(String::from_utf8(entry.key.to_vec()).unwrap());
    }
    assert_eq!(remaining, vec!["k00000001", "k00000005"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_inclusive_endpoint_range() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_inclusive", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    for i in 2..=4 {
        assert!(
            db.get(kv(i)).await.unwrap().is_none(),
            "k{i} should be deleted"
        );
    }
    assert_eq!(db.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_put_after_range_delete_is_visible() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_then_put", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    // A newer write inside the deleted interval must shadow the tombstone.
    db.put(kv(3), b"new-v3")
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert_eq!(db.get(kv(3)).await.unwrap().unwrap().as_ref(), b"new-v3");
    assert!(db.get(kv(2)).await.unwrap().is_none());
    assert!(db.get(kv(4)).await.unwrap().is_none());

    // A later point delete of k3 works normally on top of the range delete.
    db.delete(kv(3))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert!(db.get(kv(3)).await.unwrap().is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_snapshot_before_range_delete_keeps_old_values() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_snapshot", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    let snapshot = db.snapshot().await.unwrap();
    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    // Old snapshot still sees everything.
    assert_eq!(snapshot.get(kv(3)).await.unwrap().unwrap().as_ref(), b"v3");
    assert_eq!(snapshot.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(snapshot.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");

    // Fresh reads see the deletion.
    assert!(db.get(kv(3)).await.unwrap().is_none());
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");

    // A snapshot taken after the deletion also sees it.
    let after = db.snapshot().await.unwrap();
    assert!(after.get(kv(3)).await.unwrap().is_none());
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_range_delete_survives_restart_without_flush() {
    let path = "/delete_range_wal_recovery";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let db = open_db(path, store.clone()).await;
    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    // Durable in L0, then delete ranges.
    db.flush().await.unwrap();
    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    // Intentionally do NOT flush the memtable: the range tombstone is only in
    // the WAL. Drop the instance and reopen.
    drop(db);

    let db = open_db(path, store).await;
    assert!(db.get(kv(2)).await.unwrap().is_none());
    assert!(db.get(kv(3)).await.unwrap().is_none());
    assert!(db.get(kv(4)).await.unwrap().is_none());
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(db.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_range_delete_survives_flush_and_restart() {
    let path = "/delete_range_flush_recovery";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    let db = open_db(path, store.clone()).await;
    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();
    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    drop(db);

    let db = open_db(path, store).await;
    for i in 2..=4 {
        assert!(db.get(kv(i)).await.unwrap().is_none());
    }
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(db.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_range_delete_in_write_batch_is_atomic() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_batch", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    let mut batch = WriteBatch::new();
    batch.delete_range(kv(2)..=kv(4));
    batch.put(kv(3), b"batched-v3");
    db.write(batch)
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    assert!(db.get(kv(2)).await.unwrap().is_none());
    assert_eq!(
        db.get(kv(3)).await.unwrap().unwrap().as_ref(),
        b"batched-v3"
    );
    assert!(db.get(kv(4)).await.unwrap().is_none());
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_range_delete_is_constant_storage_per_interval() {
    let path = "/delete_range_storage";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db(path, store.clone()).await;

    let n = 100_000u32;
    for i in 0..n {
        db.put(kv(i), b"v").await.unwrap();
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::Wal,
    })
    .await
    .unwrap();

    let before = store_size_bytes(&store, path).await;
    db.delete_range(kv(10)..=kv(n - 11))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::Wal,
    })
    .await
    .unwrap();
    let after = store_size_bytes(&store, path).await;

    // One range tombstone must not expand durable storage by anything close to
    // 100k per-key tombstones (~10+ bytes each). Allow a generous envelope for
    // WAL/index metadata, but far below a linear per-key cost.
    let delta = after.saturating_sub(before);
    assert!(
        delta < 64 * 1024,
        "range delete wrote {delta} bytes for one interval; expected O(1)"
    );
    db.close().await.unwrap();
}

async fn store_size_bytes(store: &Arc<dyn ObjectStore>, path: &str) -> u64 {
    use futures::TryStreamExt;
    use slatedb::object_store::path::Path;
    let prefix = Path::from(path.trim_start_matches('/'));
    let mut total = 0u64;
    let stream = store.list(Some(&prefix));
    let metas: Vec<_> = stream.try_collect().await.unwrap();
    for meta in metas {
        total += meta.size;
    }
    total
}

#[tokio::test]
async fn test_empty_range_delete_options_compile() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_options", store).await;
    db.put(b"a", b"1").await.unwrap();
    db.put(b"z", b"2").await.unwrap();
    db.delete_range_with_options(b"a".., &WriteOptions::default())
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();
    assert!(db.get(b"a").await.unwrap().is_none());
    assert!(db.get(b"z").await.unwrap().is_none());
    db.close().await.unwrap();
}

#[cfg(feature = "wal_disable")]
#[tokio::test]
async fn test_range_delete_with_wal_disabled_flushes_to_l0() {
    use slatedb::config::PutOptions;
    let path = "/delete_range_no_wal";
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let settings = Settings {
        wal_enabled: false,
        ..Settings::default()
    };
    let db = Db::builder(path, store.clone())
        .with_settings(settings)
        .build()
        .await
        .unwrap();
    for i in 1..=5 {
        db.put_with_options(
            kv(i),
            format!("v{i}"),
            &PutOptions::default(),
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    }
    // With the WAL disabled, durability comes from the memtable flush (the
    // range tombstone is applied in memory immediately).
    db.delete_range(kv(2)..=kv(4)).await.unwrap();
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .unwrap();
    assert!(db.get(kv(3)).await.unwrap().is_none());
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(db.get(kv(5)).await.unwrap().unwrap().as_ref(), b"v5");
    db.close().await.unwrap();

    // Reopen: without a WAL, durability comes entirely from the flushed L0
    // side block.
    let db = Db::open(path, store).await.unwrap();
    assert!(db.get(kv(3)).await.unwrap().is_none());
    assert_eq!(db.get(kv(1)).await.unwrap().unwrap().as_ref(), b"v1");
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_snapshot_scan_and_fresh_scan_coexist() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_snapshot_scan", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();

    let snapshot = db.snapshot().await.unwrap();
    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    let mut old_scan = snapshot.scan(kv(1)..=kv(5)).await.unwrap();
    let mut old_keys = Vec::new();
    while let Some(entry) = old_scan.next().await.unwrap() {
        old_keys.push(String::from_utf8(entry.key.to_vec()).unwrap());
    }
    assert_eq!(
        old_keys,
        vec![
            "k00000001",
            "k00000002",
            "k00000003",
            "k00000004",
            "k00000005"
        ]
    );

    let mut new_scan = db.scan(kv(1)..=kv(5)).await.unwrap();
    let mut new_keys = Vec::new();
    while let Some(entry) = new_scan.next().await.unwrap() {
        new_keys.push(String::from_utf8(entry.key.to_vec()).unwrap());
    }
    assert_eq!(new_keys, vec!["k00000001", "k00000005"]);
    db.close().await.unwrap();
}

#[tokio::test]
async fn test_range_delete_descending_scan() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let db = open_db("/delete_range_descending", store).await;

    for i in 1..=5 {
        db.put(kv(i), format!("v{i}")).await.unwrap();
    }
    db.flush().await.unwrap();
    db.delete_range(kv(2)..=kv(4))
        .await
        .unwrap()
        .await_durable()
        .await
        .unwrap();

    let options = ScanOptions::default().with_order(IterationOrder::Descending);
    let mut scan = db.scan_with_options(kv(1)..=kv(5), &options).await.unwrap();
    let mut keys = Vec::new();
    while let Some(entry) = scan.next().await.unwrap() {
        keys.push(String::from_utf8(entry.key.to_vec()).unwrap());
    }
    assert_eq!(keys, vec!["k00000005", "k00000001"]);
    db.close().await.unwrap();
}
