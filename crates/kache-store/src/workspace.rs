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

    struct Stages {
        calls: std::sync::Mutex<Vec<String>>,
        fail_publish: bool,
    }

    impl Workspace for Stages {
        fn execute(&self, operation: Operation<'_>) -> io::Result<Outcome> {
            let mut calls = self.calls.lock().unwrap();
            match operation {
                Operation::PrepareBytes(_, bytes) => {
                    calls.push("prepare".into());
                    Ok(Outcome::Prepared {
                        id: 42,
                        bytes: bytes.len() as u64,
                        reflinked: false,
                    })
                }
                Operation::Publish(id, replace) => {
                    calls.push(format!("publish:{id}:{replace}"));
                    if self.fail_publish {
                        Err(io::ErrorKind::PermissionDenied.into())
                    } else {
                        Ok(Outcome::Done)
                    }
                }
                Operation::Discard(id) => {
                    calls.push(format!("discard:{id}"));
                    Ok(Outcome::Done)
                }
                _ => panic!("unexpected workspace operation"),
            }
        }
    }

    #[test]
    fn prepared_files_discard_only_unpublished_stages() {
        for (publish, replace, fail_publish, expected) in [
            (false, false, false, vec!["prepare", "discard:42"]),
            (true, false, false, vec!["prepare", "publish:42:false"]),
            (true, true, false, vec!["prepare", "publish:42:true"]),
            (
                true,
                true,
                true,
                vec!["prepare", "publish:42:true", "discard:42"],
            ),
        ] {
            let owner = Arc::new(Stages {
                calls: Default::default(),
                fail_publish,
            });
            let (stage, bytes, reflinked) = Prepared::new(
                owner.clone(),
                Operation::PrepareBytes(Path::new("output"), b"bytes"),
            )
            .unwrap();
            assert_eq!(bytes, 5);
            assert!(!reflinked);
            if publish {
                assert_eq!(stage.publish(replace).is_err(), fail_publish);
            } else {
                drop(stage);
            }
            assert_eq!(*owner.calls.lock().unwrap(), expected);
        }
    }

    struct RemoveError(io::ErrorKind);
    impl Workspace for RemoveError {
        fn execute(&self, operation: Operation<'_>) -> io::Result<Outcome> {
            match operation {
                Operation::Remove(_) => Err(self.0.into()),
                Operation::Reflink(_, _) | Operation::Permissions(_, _) => Ok(Outcome::Done),
                _ => panic!("unexpected restore fallback"),
            }
        }
    }

    #[test]
    fn clearing_an_absent_target_succeeds_but_other_client_errors_propagate() {
        for (kind, succeeds) in [
            (io::ErrorKind::NotFound, true),
            (io::ErrorKind::PermissionDenied, false),
        ] {
            let _scope = Scope::enter(Some(Arc::new(RemoveError(kind))));
            assert_eq!(
                crate::link::link_to_target(
                    Path::new("blob"),
                    Path::new("output"),
                    crate::link::LinkStrategy::Copy
                )
                .is_ok(),
                succeeds
            );
        }
    }

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
