//! Invocation-owned workspace operations, independent of compiler and IPC code.

use std::{cell::RefCell, io, path::Path, sync::Arc};

pub enum Operation<'a> {
    Reflink(&'a Path, &'a Path),
    Copy(&'a Path, &'a Path, bool),
    Hardlink(&'a Path, &'a Path),
    Remove(&'a Path),
    CreateDirectories(&'a Path),
    Permissions(&'a Path, u32),
    Touch(&'a Path),
    Write(&'a Path, &'a [u8], u32),
    PrepareFile(&'a Path, &'a Path),
    PrepareBytes(&'a Path, &'a [u8]),
    Publish(u64, bool),
    Discard(u64),
}

pub enum Outcome {
    Done,
    Prepared {
        id: u64,
        bytes: u64,
        reflinked: bool,
    },
}

pub trait Workspace: Send + Sync {
    fn execute(&self, operation: Operation<'_>) -> io::Result<Outcome>;
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<dyn Workspace>>> = const { RefCell::new(None) };
}

pub struct Scope {
    previous: Option<Arc<dyn Workspace>>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl Scope {
    pub fn enter(workspace: Option<Arc<dyn Workspace>>) -> Self {
        Self {
            previous: CURRENT.with(|slot| slot.replace(workspace)),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.replace(self.previous.take()));
    }
}

pub fn current() -> Option<Arc<dyn Workspace>> {
    CURRENT.with(|slot| slot.borrow().clone())
}

pub fn dispatch(operation: Operation<'_>) -> Option<io::Result<()>> {
    current().map(|workspace| match workspace.execute(operation)? {
        Outcome::Done => Ok(()),
        Outcome::Prepared { .. } => Err(io::Error::other("unexpected prepared workspace file")),
    })
}

pub struct Prepared {
    owner: Arc<dyn Workspace>,
    id: u64,
    published: bool,
}

impl Prepared {
    pub fn new(
        owner: Arc<dyn Workspace>,
        operation: Operation<'_>,
    ) -> io::Result<(Self, u64, bool)> {
        let Outcome::Prepared {
            id,
            bytes,
            reflinked,
        } = owner.execute(operation)?
        else {
            return Err(io::Error::other("workspace did not prepare an artifact"));
        };
        Ok((
            Self {
                owner,
                id,
                published: false,
            },
            bytes,
            reflinked,
        ))
    }

    pub fn publish(mut self, replace: bool) -> io::Result<()> {
        match self.owner.execute(Operation::Publish(self.id, replace))? {
            Outcome::Done => {
                self.published = true;
                Ok(())
            }
            _ => Err(io::Error::other("workspace did not publish an artifact")),
        }
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        if !self.published {
            let _ = self.owner.execute(Operation::Discard(self.id));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Denied;
    impl Workspace for Denied {
        fn execute(&self, _operation: Operation<'_>) -> io::Result<Outcome> {
            Err(io::Error::from_raw_os_error(libc::EACCES))
        }
    }

    #[test]
    fn denied_client_operations_never_retry_in_the_daemon_and_scopes_restore() {
        let root = tempfile::tempdir().unwrap();
        let blob = root.path().join("blob");
        let target = root.path().join("target");
        std::fs::write(&blob, b"cached bytes").unwrap();
        let owner = Arc::new(Denied) as Arc<dyn Workspace>;
        {
            let _scope = Scope::enter(Some(owner));
            assert!(
                crate::link::link_to_target(&blob, &target, crate::link::LinkStrategy::Copy)
                    .is_err()
            );
            assert!(
                crate::link::write_restored(
                    &target,
                    b"transformed",
                    crate::link::LinkStrategy::Hardlink
                )
                .is_err()
            );
            assert!(crate::link::prepare_writable_target_from_file(&blob, &target).is_err());
            assert!(
                crate::link::prepare_writable_target_from_bytes(&target, b"transformed").is_err()
            );
            assert!(!target.exists());
            {
                let _local = Scope::enter(None);
                crate::link::write_restored(
                    &target,
                    b"nested",
                    crate::link::LinkStrategy::Hardlink,
                )
                .unwrap();
            }
            assert!(crate::link::touch_mtime_write_clock(&target).is_err());
            assert!(
                crate::link::write_restored(
                    &target,
                    b"changed",
                    crate::link::LinkStrategy::Hardlink
                )
                .is_err()
            );
            assert_eq!(std::fs::read(&target).unwrap(), b"nested");
        }
        crate::link::write_restored(
            &target,
            b"next request",
            crate::link::LinkStrategy::Hardlink,
        )
        .unwrap();
        assert_eq!(std::fs::read(target).unwrap(), b"next request");
    }
}
