//! Test harness macros for matrix scenario expansion.

/// Expand embedded catalog rows as individual tokio tests (mem storage).
#[macro_export]
macro_rules! matrix_embedded_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                async fn [<$id _mem_embedded>]() {
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::Embedded,
                        $crate::StorageAdapter::Mem,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Expand coordinator–worker catalog rows as individual tokio tests (mem storage).
#[macro_export]
macro_rules! matrix_coordinator_worker_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                async fn [<$id _mem_coordinator_worker>]() {
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::CoordinatorWorker,
                        $crate::StorageAdapter::Mem,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Default coordinator–worker catalog (see `invoke_catalog_scenario_ids!` in catalog.rs).
#[macro_export]
macro_rules! matrix_coordinator_scenario_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_coordinator_worker_scenario_suite);
    };
    ($($id:ident),* $(,)?) => {
        $crate::matrix_coordinator_worker_scenario_suite!($($id),*);
    };
}

/// Expand embedded catalog rows for SQLite storage (PR CI durable slice).
#[macro_export]
macro_rules! matrix_sqlite_embedded_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                async fn [<$id _sqlite_embedded>]() {
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::Embedded,
                        $crate::StorageAdapter::Sqlite,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Expand coordinator–worker catalog rows for SQLite storage.
#[macro_export]
macro_rules! matrix_sqlite_coordinator_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                async fn [<$id _sqlite_coordinator_worker>]() {
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::CoordinatorWorker,
                        $crate::StorageAdapter::Sqlite,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Expand embedded catalog rows for PostgreSQL (`#[ignore]` until URL set).
#[macro_export]
macro_rules! matrix_postgres_embedded_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                #[ignore = "requires CHRONON_POSTGRES_URL — run with: cargo test -p chronon-e2e -- --ignored"]
                async fn [<$id _postgres_embedded>]() {
                    if std::env::var("CHRONON_POSTGRES_URL").is_err() {
                        return;
                    }
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::Embedded,
                        $crate::StorageAdapter::Postgres,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Expand coordinator–worker catalog rows for PostgreSQL (`#[ignore]` until URL set).
#[macro_export]
macro_rules! matrix_postgres_coordinator_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                #[ignore = "requires CHRONON_POSTGRES_URL — run with: cargo test -p chronon-e2e -- --ignored"]
                async fn [<$id _postgres_coordinator_worker>]() {
                    if std::env::var("CHRONON_POSTGRES_URL").is_err() {
                        return;
                    }
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::CoordinatorWorker,
                        $crate::StorageAdapter::Postgres,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Expand embedded catalog rows for Postgres+Redis composite (`#[ignore]`).
#[macro_export]
macro_rules! matrix_postgres_redis_embedded_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL"]
                async fn [<$id _postgres_redis_embedded>]() {
                    if std::env::var("CHRONON_POSTGRES_URL").is_err()
                        || (std::env::var("CHRONON_REDIS_URL").is_err()
                            && std::env::var("CHRONON_TEST_REDIS_URL").is_err())
                    {
                        return;
                    }
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::Embedded,
                        $crate::StorageAdapter::PostgresRedis,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Default embedded catalog (see `invoke_catalog_scenario_ids!` in catalog.rs).
#[macro_export]
macro_rules! matrix_scenario_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_embedded_scenario_suite);
    };
}

/// Default SQLite embedded catalog (full parity with mem).
#[macro_export]
macro_rules! matrix_sqlite_scenario_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_sqlite_embedded_scenario_suite);
    };
}

/// Default SQLite coordinator–worker catalog.
#[macro_export]
macro_rules! matrix_sqlite_coordinator_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_sqlite_coordinator_scenario_suite);
    };
}

/// Default PostgreSQL embedded catalog (extended CI).
#[macro_export]
macro_rules! matrix_postgres_scenario_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_postgres_embedded_scenario_suite);
    };
}

/// Default PostgreSQL coordinator–worker catalog (extended CI).
#[macro_export]
macro_rules! matrix_postgres_coordinator_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_postgres_coordinator_scenario_suite);
    };
}

/// Expand coordinator–worker catalog rows for Postgres+Redis composite (`#[ignore]`).
#[macro_export]
macro_rules! matrix_postgres_redis_coordinator_scenario_suite {
    ($($id:ident),* $(,)?) => {
        $(
            $crate::paste::paste! {
                #[tokio::test]
                #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL"]
                async fn [<$id _postgres_redis_coordinator_worker>]() {
                    if std::env::var("CHRONON_POSTGRES_URL").is_err()
                        || (std::env::var("CHRONON_REDIS_URL").is_err()
                            && std::env::var("CHRONON_TEST_REDIS_URL").is_err())
                    {
                        return;
                    }
                    $crate::run_catalog_entry_by_id(
                        stringify!($id),
                        $crate::CatalogDeployment::CoordinatorWorker,
                        $crate::StorageAdapter::PostgresRedis,
                    )
                    .await;
                }
            }
        )*
    };
}

/// Distributed postgres-redis smoke tests (multi-worker claim exclusivity).
#[macro_export]
macro_rules! matrix_distributed_scenario_suite {
    () => {
        #[tokio::test]
        #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL — distributed smoke"]
        async fn dual_worker_claim_exclusive_postgres_redis() {
            if !$crate::distributed_store_available() {
                return;
            }
            $crate::dual_worker_claim_exclusive_smoke()
                .await
                .expect("dual worker claim exclusive");
        }

        #[tokio::test]
        #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL — distributed smoke"]
        async fn dual_worker_wrong_pool_idle_postgres_redis() {
            if !$crate::distributed_store_available() {
                return;
            }
            $crate::dual_worker_wrong_pool_idle_smoke()
                .await
                .expect("wrong pool idle");
        }

        #[tokio::test]
        #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL — distributed smoke"]
        async fn coordinator_leader_exclusive_postgres_redis() {
            if !$crate::distributed_store_available() {
                return;
            }
            $crate::coordinator_leader_exclusive_smoke()
                .await
                .expect("leader exclusive");
        }

        #[tokio::test]
        #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL — distributed smoke"]
        async fn coordinator_failover_postgres_redis() {
            if !$crate::distributed_store_available() {
                return;
            }
            $crate::coordinator_failover_postgres_redis_smoke()
                .await
                .expect("coordinator failover");
        }

        #[tokio::test]
        #[ignore = "requires CHRONON_POSTGRES_URL and CHRONON_REDIS_URL — distributed smoke"]
        async fn postgres_redis_hybrid_claim_roundtrip() {
            if !$crate::distributed_store_available() {
                return;
            }
            $crate::postgres_redis_hybrid_claim_roundtrip_smoke()
                .await
                .expect("hybrid roundtrip");
        }
    };
}

/// Default Postgres+Redis coordinator–worker catalog (extended CI).
#[macro_export]
macro_rules! matrix_postgres_redis_coordinator_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!(
            $crate::matrix_postgres_redis_coordinator_scenario_suite
        );
    };
}

/// Default Postgres+Redis embedded catalog (extended CI).
#[macro_export]
macro_rules! matrix_postgres_redis_scenario_suite {
    () => {
        $crate::invoke_catalog_scenario_ids!($crate::matrix_postgres_redis_embedded_scenario_suite);
    };
}
