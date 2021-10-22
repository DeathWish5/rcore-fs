#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![feature(async_closure)]

extern crate alloc;

extern crate log;

pub mod dev;
pub mod dirty;
pub mod file;
pub mod util;
pub mod vfs;

#[cfg(any(test, feature = "std"))]
mod std;
