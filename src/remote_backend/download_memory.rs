//! Admission and lifetime accounting for buffered remote objects.
//!
//! Reserve before reading, including room for the final contiguous copy. Keep
//! the reservation with the returned bytes until their last clone is dropped.
//! Acquiring the whole reservation at once avoids readers each holding partial
//! allocations while waiting for one another to release the rest.

use std::sync::{Arc, LazyLock};

use anyhow::{Context, Result};
use bytes::Bytes;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MEMORY_UNIT_BYTES: u64 = 1024;
const BUFFER_BUDGET_UNITS: u32 = 512 << 10;

pub(super) static DOWNLOAD_MEMORY: LazyLock<Arc<DownloadMemory>> =
    LazyLock::new(|| Arc::new(DownloadMemory::new(BUFFER_BUDGET_UNITS)));

pub(super) struct DownloadMemory {
    semaphore: Arc<Semaphore>,
    capacity: u32,
}

impl DownloadMemory {
    // KiB permits fit Tokio's semaphore limit on 32-bit targets too.
    pub(super) fn new(capacity: u32) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    pub(super) async fn acquire(&self, max_bytes: Option<u64>) -> Result<OwnedSemaphorePermit> {
        // Unknown-size and oversized buffered requests run alone. This keeps
        // the existing per-object limits compatible; the admission budget is
        // not an additional rejection limit for a single large object.
        let reservation = max_bytes
            .unwrap_or(u64::MAX)
            .saturating_mul(2)
            .div_ceil(MEMORY_UNIT_BYTES)
            .clamp(1, u64::from(self.capacity)) as u32;
        self.semaphore
            .clone()
            .acquire_many_owned(reservation)
            .await
            .context("waiting for remote download memory")
    }
}

pub(super) struct BudgetedBody {
    pub(super) body: Bytes,
    pub(super) _memory: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BudgetedBody {
    fn as_ref(&self) -> &[u8] {
        &self.body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt, poll};
    use std::task::Poll;

    #[tokio::test]
    async fn reservations_include_copy_space_and_are_released_on_drop() {
        let budget = DownloadMemory::new(10);
        let first = budget.acquire(Some(3 << 10)).await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 4);
        let mut second = Box::pin(budget.acquire(Some(3 << 10)));
        assert!(poll!(&mut second).is_pending());
        let last = budget.acquire(Some(0));
        assert!(last.now_or_never().is_none(), "queued requests stay fair");
        drop(first);
        let second = second.await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 4);
        drop(second);
        assert_eq!(budget.semaphore.available_permits(), 10);
    }

    #[tokio::test]
    async fn unknown_and_oversized_requests_reserve_the_entire_budget() {
        let budget = DownloadMemory::new(10);
        for limit in [None, Some(5 << 10), Some(6 << 10), Some(u64::MAX)] {
            let permit = budget.acquire(limit).await.unwrap();
            assert_eq!(budget.semaphore.available_permits(), 0, "{limit:?}");
            drop(permit);
            assert_eq!(budget.semaphore.available_permits(), 10);
        }
        let empty = budget.acquire(Some(0)).await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 9);
        drop(empty);
        let rounded = budget.acquire(Some(513)).await.unwrap();
        assert_eq!(budget.semaphore.available_permits(), 8);
        drop(rounded);
        assert_eq!(
            u64::from(BUFFER_BUDGET_UNITS) * MEMORY_UNIT_BYTES,
            536_870_912
        );
    }

    #[tokio::test]
    async fn cancelling_a_waiter_does_not_leak_its_reservation() {
        let budget = DownloadMemory::new(10);
        let held = budget.acquire(Some(4 << 10)).await.unwrap();
        let mut cancelled = Box::pin(budget.acquire(Some(2 << 10)));
        assert!(matches!(poll!(&mut cancelled), Poll::Pending));
        drop(cancelled);
        let remaining = budget
            .acquire(Some(1 << 10))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(budget.semaphore.available_permits(), 0);
        drop(remaining);
        drop(held);
        assert_eq!(budget.semaphore.available_permits(), 10);
    }

    #[tokio::test]
    async fn body_clones_and_slices_hold_the_reservation_until_last_drop() {
        let budget = DownloadMemory::new(10);
        let body = Bytes::from_owner(BudgetedBody {
            body: Bytes::from_static(b"hello"),
            _memory: budget.acquire(Some(5 << 10)).await.unwrap(),
        });
        let clone = body.clone();
        let slice = body.slice(1..4);
        drop(body);
        drop(clone);
        assert_eq!(&slice[..], b"ell");
        assert_eq!(budget.semaphore.available_permits(), 0);
        drop(slice);
        assert_eq!(budget.semaphore.available_permits(), 10);
    }

    #[tokio::test]
    async fn a_closed_budget_reports_the_waiting_stage() {
        let budget = DownloadMemory::new(10);
        budget.semaphore.close();
        let error = budget.acquire(Some(1 << 10)).await.unwrap_err();
        assert_eq!(error.to_string(), "waiting for remote download memory");
    }
}
