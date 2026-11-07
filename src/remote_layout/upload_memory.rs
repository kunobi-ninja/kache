//! Reserve pack buffers before compression and keep the reservation with HTTP bodies.

use std::io::{self, Write};
use std::sync::{Arc, LazyLock};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const UNIT: u64 = 1024;

pub(super) static DEFAULT_UPLOAD_MEMORY: LazyLock<UploadMemory> =
    LazyLock::new(|| UploadMemory::new(crate::config::DEFAULT_REMOTE_UPLOAD_MEMORY_BYTES));

pub(crate) struct UploadMemory {
    semaphore: Arc<Semaphore>,
    units: u32,
}

impl UploadMemory {
    pub(crate) fn new(bytes: u64) -> Self {
        let units = (bytes / UNIT).clamp(
            2,
            (Semaphore::MAX_PERMITS as u64)
                .min(u32::MAX as u64)
                .min(usize::MAX as u64 / UNIT),
        ) as u32;
        Self {
            semaphore: Arc::new(Semaphore::new(units as usize)),
            units,
        }
    }

    pub(super) async fn acquire(&self, pack_bound: u64) -> Result<Reservation> {
        // Reserve before compression. Both conditional and fallback writes
        // share the resulting body, including its reservation.
        let units = pack_bound.div_ceil(UNIT).clamp(1, u64::from(self.units)) as u32;
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(units)
            .await
            .context("waiting for remote upload memory")?;
        Ok(Reservation { permit })
    }
}

pub(super) struct Reservation {
    permit: OwnedSemaphorePermit,
}

impl Reservation {
    pub(super) fn pack_limit(&self) -> usize {
        self.permit.num_permits() * UNIT as usize
    }

    pub(super) fn body(self, bytes: Vec<u8>) -> Bytes {
        Bytes::from_owner(Body {
            bytes,
            _permit: self.permit,
        })
    }
}

struct Body {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for Body {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

pub(super) struct PackBuffer {
    bytes: Vec<u8>,
    limit: Option<usize>,
}

impl PackBuffer {
    pub(super) fn new(limit: Option<usize>) -> Result<Self> {
        let mut bytes = Vec::new();
        if let Some(limit) = limit {
            bytes
                .try_reserve_exact(limit)
                .context("allocating remote upload pack buffer")?;
        }
        Ok(Self { bytes, limit })
    }

    pub(super) fn into_vec(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for PackBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .limit
            .is_some_and(|limit| bytes.len() > limit.saturating_sub(self.bytes.len()))
        {
            return Err(io::Error::other(
                "remote upload pack exceeds its memory budget; increase cache.remote_upload_memory_bytes",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, poll};

    #[tokio::test]
    async fn admission_reserves_pack_space_and_transport_clones_hold_it() {
        let memory = UploadMemory::new(10 << 10);
        let reservation = memory.acquire(6 << 10).await.unwrap();
        assert_eq!(reservation.pack_limit(), 6144);
        assert_eq!(memory.semaphore.available_permits(), 4);
        let body = reservation.body(vec![7; 6144]);
        let clone = body.clone();
        let slice = body.slice(1..2);
        let mut next = Box::pin(memory.acquire(6 << 10));
        assert!(poll!(&mut next).is_pending());
        drop(body);
        drop(clone);
        // The fair semaphore has lent the four free units to the waiter;
        // the six units owned by the retained slice are still unavailable.
        assert_eq!(memory.semaphore.available_permits(), 0);
        assert_eq!(&slice[..], &[7]);
        drop(slice);
        let next = next.await.unwrap();
        assert_eq!(memory.semaphore.available_permits(), 4);
        drop(next);
        assert_eq!(memory.semaphore.available_permits(), 10);
    }

    #[tokio::test]
    async fn cancelling_queued_upload_does_not_hold_memory() {
        let memory = UploadMemory::new(10 << 10);
        let held = memory.acquire(u64::MAX).await.unwrap();
        assert_eq!(held.pack_limit(), 10240);
        let mut queued = Box::pin(memory.acquire(1));
        assert!(poll!(&mut queued).is_pending());
        drop(queued);
        drop(held);
        let next = memory.acquire(1).now_or_never().unwrap().unwrap();
        assert_eq!(next.pack_limit(), 1024);
        drop(next);
        assert_eq!(memory.semaphore.available_permits(), 10);
    }

    #[tokio::test]
    async fn reservations_round_up_and_closed_admission_is_an_error() {
        let memory = UploadMemory::new(10 << 10);
        let reservation = memory.acquire(1025).await.unwrap();
        assert_eq!(reservation.pack_limit(), 2048);
        assert_eq!(memory.semaphore.available_permits(), 8);
        drop(reservation);
        memory.semaphore.close();
        assert_eq!(
            memory.acquire(1).await.err().unwrap().to_string(),
            "waiting for remote upload memory"
        );
    }

    #[test]
    fn pack_writer_accepts_the_exact_limit_without_growing_past_it() {
        let mut buffer = PackBuffer::new(Some(3)).unwrap();
        buffer.write_all(b"ab").unwrap();
        buffer.write_all(b"c").unwrap();
        buffer.write_all(b"").unwrap();
        buffer.flush().unwrap();
        assert!(
            buffer
                .write_all(b"d")
                .unwrap_err()
                .to_string()
                .contains("cache.remote_upload_memory_bytes")
        );
        assert_eq!(buffer.into_vec(), b"abc");
        let mut unbounded = PackBuffer::new(None).unwrap();
        unbounded.write_all(b"abcd").unwrap();
        assert_eq!(unbounded.into_vec(), b"abcd");
    }
}
