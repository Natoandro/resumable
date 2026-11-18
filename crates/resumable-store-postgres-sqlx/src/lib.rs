//! Planum's PostgreSQL adapter for the portable [`resumable`] core.
//!
//! This crate owns database persistence and PostgreSQL-specific transaction extensions. It must
//! not depend on the Planum server crate.

#![forbid(unsafe_code)]

mod postgres;

pub use postgres::{
    PostgresStore, RunnableExecution, SCHEMA_VERSION, migrate, validate_schema,
    validate_schema_version,
};
