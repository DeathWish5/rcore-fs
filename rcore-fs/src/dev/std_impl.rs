#![cfg(any(test, feature = "std"))]

use super::*;
use async_trait::async_trait;
use std::fs::File;
use std::io::Error;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[async_trait]
impl Device for Mutex<File> {
    async fn read_at(&self, _offset: usize, _buf: &mut [u8]) -> Result<usize> {
        unimplemented!();
    }

    async fn write_at(&self, _offset: usize, _buf: &[u8]) -> Result<usize> {
        unimplemented!();
    }

    async fn sync(&self) -> Result<()> {
        let file = self.lock().unwrap();
        file.sync_all()?;
        Ok(())
    }
}

pub struct StdTimeProvider;

impl TimeProvider for StdTimeProvider {
    fn current_time(&self) -> Timespec {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Timespec {
            sec: duration.as_secs() as i64,
            nsec: duration.subsec_nanos() as i32,
        }
    }
}

impl From<Error> for DevError {
    fn from(_: Error) -> Self {
        DevError
    }
}
