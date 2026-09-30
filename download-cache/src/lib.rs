//! Download cache management. At a high level, this crate provides a way to get a filesystem path
//! given one or more URLs. The filesystem path is important, as it allows consumers to directly
//! open or memory map the files as necessary.
//!
//! The async methods in this crate require a `tokio` runtime with the io and time drivers enabled.

pub mod fs;
// `http` is public so that some of the utilities can be used outside of this crate for the existing
// cache implementation. It will eventually be replaced by an implementation in this crate.
pub mod http;
