//! One allowance for ordinary coordination waits in a compiler invocation.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{Duration, Instant};

const CAPACITY_WAIT: Duration = Duration::from_secs(30);

thread_local! {
    static CURRENT: RefCell<Option<Rc<Cell<Duration>>>> = const { RefCell::new(None) };
}

/// Reused daemon threads start a fresh allowance; nested invocations restore
/// their caller's allowance when they finish.
pub(crate) struct InvocationWaitScope {
    previous: Option<Rc<Cell<Duration>>>,
}

impl InvocationWaitScope {
    pub(crate) fn enter() -> Self {
        Self::with_budget(super::WAIT_TIMEOUT)
    }

    pub(super) fn with_budget(allowance: Duration) -> Self {
        Self {
            previous: CURRENT.with(|slot| slot.replace(Some(Rc::new(Cell::new(allowance))))),
        }
    }
}

impl Drop for InvocationWaitScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.replace(self.previous.take()));
    }
}

/// Debit only time spent coordinating, rather than key computation or the
/// compiler's own runtime. Keep the captured owner across nested scopes.
pub(crate) struct BudgetWait {
    pub(crate) timeout: Duration,
    budget: Option<Rc<Cell<Duration>>>,
    started: Instant,
}

impl BudgetWait {
    pub(super) fn capacity(fallback: Duration) -> Self {
        let mut wait = Self::start(fallback);
        if wait.scoped() {
            wait.timeout = wait.timeout.min(CAPACITY_WAIT);
        }
        wait
    }

    pub(crate) fn start(fallback: Duration) -> Self {
        let budget = CURRENT.with(|slot| slot.borrow().clone());
        Self {
            timeout: budget
                .as_ref()
                .map_or(fallback, |left| left.get().min(fallback)),
            budget,
            started: Instant::now(),
        }
    }

    pub(crate) fn scoped(&self) -> bool {
        self.budget.is_some()
    }
}

impl Drop for BudgetWait {
    fn drop(&mut self) {
        if let Some(left) = &self.budget {
            left.set(left.get().saturating_sub(self.started.elapsed()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_coordination_allowance_defaults_to_thirty_minutes() {
        let _scope = InvocationWaitScope::enter();
        let wait = BudgetWait::start(Duration::from_secs(1800));
        assert!(wait.scoped());
        assert_eq!(wait.timeout, Duration::from_secs(1800));
    }

    #[test]
    fn capacity_waits_are_capped_within_the_shared_coordination_allowance() {
        assert_eq!(
            BudgetWait::capacity(Duration::from_secs(1800)).timeout,
            Duration::from_secs(1800)
        );
        let _scope = InvocationWaitScope::enter();
        assert_eq!(
            BudgetWait::capacity(Duration::from_secs(1800)).timeout,
            Duration::from_secs(30)
        );
        assert_eq!(
            BudgetWait::capacity(Duration::from_secs(7)).timeout,
            Duration::from_secs(7)
        );
        let _short = InvocationWaitScope::with_budget(Duration::from_millis(50));
        assert_eq!(
            BudgetWait::capacity(Duration::from_secs(1800)).timeout,
            Duration::from_millis(50)
        );
    }

    #[test]
    fn shorter_explicit_limits_and_unscoped_waits_are_preserved() {
        let wait = BudgetWait::start(Duration::from_secs(17));
        assert!(!wait.scoped());
        assert_eq!(wait.timeout, Duration::from_secs(17));
        let _scope = InvocationWaitScope::with_budget(Duration::from_secs(30));
        assert_eq!(
            BudgetWait::start(Duration::from_secs(7)).timeout,
            Duration::from_secs(7)
        );
    }

    #[test]
    fn stages_share_the_allowance_and_nested_scopes_restore_their_owner() {
        let _scope = InvocationWaitScope::with_budget(Duration::from_secs(19));
        let mut wait = BudgetWait::start(Duration::from_secs(1800));
        wait.started = Instant::now() - Duration::from_secs(11);
        {
            let _nested = InvocationWaitScope::with_budget(Duration::from_secs(3));
            assert_eq!(
                BudgetWait::start(Duration::from_secs(1800)).timeout,
                Duration::from_secs(3)
            );
            drop(wait);
        }
        let remaining = BudgetWait::start(Duration::from_secs(1800));
        assert!(remaining.timeout <= Duration::from_secs(8));
        assert!(remaining.timeout > Duration::from_secs(6));
    }

    #[test]
    fn exhaustion_saturates_and_reused_threads_start_fresh() {
        {
            let _scope = InvocationWaitScope::with_budget(Duration::from_secs(1));
            let mut wait = BudgetWait::start(Duration::from_secs(1800));
            wait.started = Instant::now() - Duration::from_secs(2);
            drop(wait);
            assert_eq!(
                BudgetWait::start(Duration::from_secs(1800)).timeout,
                Duration::ZERO
            );
        }
        assert!(!BudgetWait::start(Duration::from_secs(1800)).scoped());
        let _scope = InvocationWaitScope::enter();
        assert_eq!(
            BudgetWait::start(Duration::from_secs(1800)).timeout,
            Duration::from_secs(1800)
        );
    }
}
