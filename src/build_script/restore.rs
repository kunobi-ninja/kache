//! Prepare independent OUT_DIR files concurrently; publish on the caller so
//! restore counters and file-hash memoisation retain their invocation owner.

use anyhow::Result;
use kache_store::{link, workspace};
use std::path::PathBuf;

/// Overlap filesystem waits without creating a thread for every output file.
const MAX_WORKERS: usize = 8;

pub(super) fn prepare(files: &[(PathBuf, PathBuf)]) -> Result<Vec<link::PreparedWritableTarget>> {
    if files.len() <= 1 {
        return prepare_chunk(files);
    }
    let owner = workspace::current();
    std::thread::scope(|scope| {
        let workers = files
            .chunks(files.len().div_ceil(MAX_WORKERS))
            .map(|chunk| {
                let owner = owner.clone();
                scope.spawn(move || {
                    let _workspace = workspace::Scope::enter(owner);
                    prepare_chunk(chunk)
                })
            })
            .collect::<Vec<_>>();
        // The scope joins every worker even when a preparation fails. No
        // worker may write after the caller starts cleaning a rejected restore.
        workers
            .into_iter()
            .map(|worker| {
                worker
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect::<Result<Vec<_>>>()
            .map(|chunks| chunks.into_iter().flatten().collect())
    })
}

fn prepare_chunk(files: &[(PathBuf, PathBuf)]) -> Result<Vec<link::PreparedWritableTarget>> {
    files
        .iter()
        .map(|(source, target)| link::prepare_writable_target_from_file(source, target))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::thread::ThreadId;

    #[test]
    fn prepared_outputs_keep_their_order_bytes_and_caller_accounting() {
        for count in [0, 1, 17] {
            let root = tempfile::tempdir().unwrap();
            let targets = root.path().join("out");
            std::fs::create_dir(&targets).unwrap();
            let files = (0..count)
                .map(|index| {
                    let source = root.path().join(format!("blob-{index}"));
                    std::fs::write(&source, vec![index as u8; 1024 + index]).unwrap();
                    (source, targets.join(format!("file-{index}")))
                })
                .collect::<Vec<_>>();
            let _bytes = kache_store::opcounts::InvocationByteScope::enter();
            let prepared = prepare(&files).unwrap();
            assert_eq!(prepared.len(), count);
            assert_eq!(kache_store::opcounts::reflinked_bytes(), 0);
            assert_eq!(kache_store::opcounts::copied_bytes(), 0);
            for (artifact, (_, target)) in prepared.into_iter().zip(&files) {
                assert_eq!(artifact.target(), target);
                assert!(!target.exists(), "preparation must not publish the output");
                artifact.publish_replacing().unwrap();
            }
            assert_eq!(
                kache_store::opcounts::reflinked_bytes() + kache_store::opcounts::copied_bytes(),
                (0..count).map(|index| (1024 + index) as u64).sum::<u64>()
            );
            assert_eq!(kache_store::opcounts::hardlinked_bytes(), 0);
            for (index, (source, target)) in files.iter().enumerate() {
                let expected = vec![index as u8; 1024 + index];
                assert_eq!(std::fs::read(target).unwrap(), expected);
                std::fs::write(target, b"edited after restore").unwrap();
                assert_eq!(std::fs::read(source).unwrap(), expected);
            }
        }
    }

    #[test]
    fn a_failed_preparation_drops_all_stages_before_returning() {
        for missing in [0, 16] {
            let root = tempfile::tempdir().unwrap();
            let targets = root.path().join("out");
            std::fs::create_dir(&targets).unwrap();
            let files = (0..17)
                .map(|index| {
                    let source = root.path().join(format!("blob-{index}"));
                    if index != missing {
                        std::fs::write(&source, b"cached bytes").unwrap();
                    }
                    (source, targets.join(format!("file-{index}")))
                })
                .collect::<Vec<_>>();
            assert!(prepare(&files).is_err());
            assert_eq!(std::fs::read_dir(&targets).unwrap().count(), 0);
        }
    }

    struct Client {
        started: mpsc::Sender<ThreadId>,
        released: (Mutex<bool>, Condvar),
        prepared: Mutex<Vec<(u64, PathBuf, ThreadId)>>,
        discarded: Mutex<Vec<u64>>,
        fail_first: bool,
    }

    impl workspace::Workspace for Client {
        fn execute(
            &self,
            operation: workspace::Operation<'_>,
        ) -> std::io::Result<workspace::Outcome> {
            match operation {
                workspace::Operation::PrepareFile(source, target) => {
                    let id = source.to_str().unwrap().parse::<u64>().unwrap();
                    let thread = std::thread::current().id();
                    self.prepared
                        .lock()
                        .unwrap()
                        .push((id, target.into(), thread));
                    self.started.send(thread).unwrap();
                    let (released, wake) = &self.released;
                    let _released = wake
                        .wait_while(released.lock().unwrap(), |released| !*released)
                        .unwrap();
                    if self.fail_first && id == 0 {
                        return Err(std::io::ErrorKind::PermissionDenied.into());
                    }
                    Ok(workspace::Outcome::Prepared {
                        id,
                        bytes: id + 1,
                        reflinked: false,
                    })
                }
                workspace::Operation::Discard(id) => {
                    self.discarded.lock().unwrap().push(id);
                    Ok(workspace::Outcome::Done)
                }
                _ => panic!("unexpected client operation"),
            }
        }
    }

    fn client(fail_first: bool) -> (Arc<Client>, mpsc::Receiver<ThreadId>) {
        let (started, receiver) = mpsc::channel();
        (
            Arc::new(Client {
                started,
                released: (Mutex::new(false), Condvar::new()),
                prepared: Default::default(),
                discarded: Default::default(),
                fail_first,
            }),
            receiver,
        )
    }

    #[test]
    fn single_file_preparation_stays_on_the_caller() {
        let (client, _started) = client(false);
        *client.released.0.lock().unwrap() = true;
        let _workspace = workspace::Scope::enter(Some(client.clone()));
        let files = [(PathBuf::from("42"), PathBuf::from("out"))];
        drop(prepare(&files).unwrap());
        assert_eq!(
            *client.prepared.lock().unwrap(),
            [(42, PathBuf::from("out"), std::thread::current().id())]
        );
        assert_eq!(*client.discarded.lock().unwrap(), [42]);
    }

    #[test]
    fn parallel_preparation_inherits_the_client_and_joins_every_worker_on_error() {
        for fail_first in [false, true] {
            let (client, started) = client(fail_first);
            let files = (0..64)
                .map(|index| {
                    (
                        PathBuf::from(index.to_string()),
                        PathBuf::from(format!("out-{index}")),
                    )
                })
                .collect::<Vec<_>>();
            std::thread::scope(|scope| {
                let client = &client;
                let worker = scope.spawn(|| {
                    let _workspace = workspace::Scope::enter(Some(client.clone()));
                    prepare(&files)
                });
                // Every worker blocks in the client until all eight have
                // arrived. Always release them before asserting, even on timeout.
                let arrivals = (0..8)
                    .map(|_| started.recv_timeout(std::time::Duration::from_secs(5)))
                    .collect::<std::result::Result<Vec<_>, _>>();
                *client.released.0.lock().unwrap() = true;
                client.released.1.notify_all();
                let result = worker.join().unwrap();
                assert_eq!(
                    arrivals
                        .unwrap()
                        .into_iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len(),
                    8
                );
                if fail_first {
                    assert_eq!(
                        result
                            .err()
                            .unwrap()
                            .downcast_ref::<std::io::Error>()
                            .unwrap()
                            .kind(),
                        std::io::ErrorKind::PermissionDenied
                    );
                } else {
                    let prepared = result.unwrap();
                    assert_eq!(prepared.len(), 64);
                    for (artifact, (_, target)) in prepared.iter().zip(&files) {
                        assert_eq!(artifact.target(), target);
                    }
                    drop(prepared);
                }
            });
            let calls = client.prepared.lock().unwrap();
            assert_eq!(
                calls
                    .iter()
                    .map(|(_, _, thread)| thread)
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                8
            );
            let mut expected = calls
                .iter()
                .filter_map(|(id, target, _)| {
                    assert_eq!(target, &PathBuf::from(format!("out-{id}")));
                    (!fail_first || *id != 0).then_some(*id)
                })
                .collect::<Vec<_>>();
            expected.sort_unstable();
            let mut discarded = client.discarded.lock().unwrap().clone();
            discarded.sort_unstable();
            assert_eq!(
                discarded, expected,
                "every successful stage is discarded before return"
            );
        }
    }
}
