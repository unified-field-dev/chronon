//! SchedulerStore port contracts for publishable backends.
//!
//! Kept here so backend crates do not depend on `chronon-testkit` (publish = false).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use std::sync::Arc;
use std::time::Duration;

use chronon_backend_mem::InMemorySchedulerStore;
use chronon_backend_postgres::{postgres_test_url, PostgresSchedulerStore};
use chronon_backend_redis::{PostgresRedisSchedulerStore, RedisQueueLayer};
use chronon_backend_sqlite::SqliteSchedulerStore;
use chronon_core::store::SchedulerStore;
use chronon_testkit::{run_list_runs_after_contract, run_store_contract};
use uuid::Uuid;

#[tokio::test]
async fn mem_store_contract() {
    let store: Arc<dyn SchedulerStore> = Arc::new(InMemorySchedulerStore::new());
    run_store_contract(store).await.expect("mem store contract");
}

#[tokio::test]
async fn sqlite_store_contract() {
    let store: Arc<dyn SchedulerStore> = Arc::new(
        SqliteSchedulerStore::connect("sqlite://:memory:")
            .await
            .expect("connect"),
    );
    run_store_contract(store)
        .await
        .expect("sqlite store contract");
}

#[tokio::test]
async fn sqlite_file_store_contract() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("chronon.db");
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let store: Arc<dyn SchedulerStore> =
        Arc::new(SqliteSchedulerStore::connect(&url).await.expect("connect"));
    run_store_contract(store)
        .await
        .expect("sqlite file contract");
}

#[tokio::test]
#[ignore = "requires CHRONON_POSTGRES_URL or CHRONON_TEST_POSTGRES_URL"]
async fn postgres_store_contract() {
    if std::env::var("CHRONON_POSTGRES_URL").is_err()
        && std::env::var("CHRONON_TEST_POSTGRES_URL").is_err()
    {
        panic!("postgres URL env var required for ignored test");
    }
    let url = postgres_test_url();
    let schema = format!("chronon_test_{}", Uuid::new_v4().simple());
    let store: Arc<dyn SchedulerStore> = Arc::new(
        PostgresSchedulerStore::connect_isolated(&url, &schema)
            .await
            .expect("connect postgres"),
    );
    run_store_contract(store)
        .await
        .expect("postgres store contract");
}

#[tokio::test]
async fn mem_keyset_contract() {
    let store = InMemorySchedulerStore::new();
    run_list_runs_after_contract(&store)
        .await
        .expect("mem keyset contract");
}

#[tokio::test]
async fn sqlite_keyset_contract() {
    let store = SqliteSchedulerStore::connect("sqlite://:memory:")
        .await
        .expect("connect");
    run_list_runs_after_contract(&store)
        .await
        .expect("sqlite keyset contract");
}

#[tokio::test]
#[ignore = "requires CHRONON_POSTGRES_URL or CHRONON_TEST_POSTGRES_URL"]
async fn postgres_keyset_contract() {
    if std::env::var("CHRONON_POSTGRES_URL").is_err()
        && std::env::var("CHRONON_TEST_POSTGRES_URL").is_err()
    {
        panic!("postgres URL env var required for ignored test");
    }
    let url = postgres_test_url();
    let schema = format!("chronon_keyset_{}", Uuid::new_v4().simple());
    let store = PostgresSchedulerStore::connect_isolated(&url, &schema)
        .await
        .expect("connect postgres");
    run_list_runs_after_contract(&store)
        .await
        .expect("postgres keyset contract");
}

async fn connect_redis(prefix: &str) -> Option<RedisQueueLayer> {
    let redis_url = RedisQueueLayer::test_url();
    let connect = RedisQueueLayer::connect(&redis_url, Some(prefix));
    let redis = tokio::time::timeout(Duration::from_secs(2), connect)
        .await
        .ok()?
        .ok()?;
    tokio::time::timeout(Duration::from_secs(2), redis.flush_keys())
        .await
        .ok()?
        .ok()?;
    Some(redis)
}

#[tokio::test]
async fn composite_store_contract_sqlite_redis() {
    let prefix = format!("chronon_contract_{}", Uuid::new_v4().simple());
    let Some(redis) = connect_redis(&prefix).await else {
        eprintln!("skip composite_store_contract_sqlite_redis: redis unavailable");
        return;
    };
    let sql: Arc<dyn SchedulerStore> = Arc::new(
        SqliteSchedulerStore::connect("sqlite://:memory:")
            .await
            .expect("sqlite"),
    );
    let store: Arc<dyn SchedulerStore> = Arc::new(PostgresRedisSchedulerStore::new(sql, redis));
    run_store_contract(store)
        .await
        .expect("composite sqlite+redis contract");
}

#[tokio::test]
#[ignore = "requires CHRONON_POSTGRES_URL and Redis"]
async fn composite_store_contract_postgres_redis() {
    let prefix = format!("chronon_contract_{}", Uuid::new_v4().simple());
    let Some(redis) = connect_redis(&prefix).await else {
        panic!("redis required for ignored test");
    };
    if std::env::var("CHRONON_POSTGRES_URL").is_err()
        && std::env::var("CHRONON_TEST_POSTGRES_URL").is_err()
    {
        panic!("postgres URL required for ignored test");
    }
    let url = postgres_test_url();
    let schema = format!("chronon_redis_{}", Uuid::new_v4().simple());
    let sql: Arc<dyn SchedulerStore> = Arc::new(
        PostgresSchedulerStore::connect_isolated(&url, &schema)
            .await
            .expect("postgres"),
    );
    let store: Arc<dyn SchedulerStore> = Arc::new(PostgresRedisSchedulerStore::new(sql, redis));
    run_store_contract(store)
        .await
        .expect("composite postgres+redis contract");
}
