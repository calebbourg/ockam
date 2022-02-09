//! Software implementation of ockam_core::capability traits.
//!
//! This crate contains one of the possible implementation of the capability traits
//! which you can use with Ockam library.
#![deny(unsafe_code)]
#![warn(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_qualifications
)]
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
extern crate core;

#[cfg(feature = "alloc")]
//#[macro_use]
extern crate alloc;

pub use ockam_core;

mod error;

// Re-export types commonly used by higher level APIs
pub use ockam_core::vault::{
    Hasher, KeyIdVault, PublicKey, Secret, SecretAttributes, SecretVault, Signer, Verifier,
};

pub use error::*;
