use anyhow::{Context, Result};
use kache_config::InvocationEnv;
use kache_protocol::execution::{
    EXECUTION_VERSION, ExecutionChannels, ExecutionEntry, ExecutionRequest, NativeString,
};
use std::ffi::OsString;
use std::fs::File;
#[cfg(unix)]
use std::os::fd::AsFd;
use std::path::Path;
use std::time::Instant;

/// Borrowed inputs remain owned by the client until completion or disconnect.
pub struct Execution<'a> {
    pub entry: ExecutionEntry,
    pub args: &'a [OsString],
    pub env: &'a InvocationEnv,
    pub started_at: Instant,
    pub executable: &'a Path,
    /// stdin, stdout, stderr, then an optional controlling-terminal file.
    pub files: &'a [File],
    pub terminal: bool,
    pub client_version: &'a str,
}

struct Prepared {
    request: ExecutionRequest,
    #[cfg(unix)]
    jobserver: kache_ipc::jobserver::Descriptors,
}

impl Execution<'_> {
    /// `None` permits local execution. Errors after Commit prohibit a retry.
    pub fn try_execute(&self, socket: &Path) -> Result<Option<i32>> {
        let prepared = match self.prepare() {
            Ok(prepared) => prepared,
            Err(error) => {
                tracing::debug!(%error, "compiler request cannot be transferred");
                return Ok(None);
            }
        };
        #[cfg(unix)]
        let descriptors = self
            .files
            .iter()
            .map(AsFd::as_fd)
            .chain(prepared.jobserver.files().iter().map(AsFd::as_fd))
            .collect::<Vec<_>>();
        #[cfg(unix)]
        {
            kache_ipc::execution::try_execute(socket, &prepared.request, &descriptors)
        }
        #[cfg(windows)]
        {
            kache_ipc::execution::try_execute(socket, &prepared.request)
        }
    }

    fn prepare(&self) -> Result<Prepared> {
        anyhow::ensure!(
            self.files.len() == 3 + usize::from(self.terminal),
            "invalid compiler stream manifest"
        );
        self.env.validate()?;
        anyhow::ensure!(
            self.executable.is_absolute(),
            "compiler executable must be absolute"
        );
        let env = self.env.vars_os().collect::<Vec<_>>();
        #[cfg(unix)]
        let (originals, jobserver) = kache_ipc::jobserver::capture(&env)?;
        #[cfg(unix)]
        let channels = ExecutionChannels::Unix {
            terminal: self.terminal,
            jobserver: originals,
        };
        #[cfg(windows)]
        let channels = {
            use std::os::windows::io::AsRawHandle;
            anyhow::ensure!(!self.terminal, "Windows terminals require local execution");
            ExecutionChannels::Windows {
                stdin: self.files[0].as_raw_handle() as usize as u64,
                stdout: self.files[1].as_raw_handle() as usize as u64,
                stderr: self.files[2].as_raw_handle() as usize as u64,
            }
        };
        let native_path = |path: &Path| NativeString::capture(path.as_os_str());
        Ok(Prepared {
            request: ExecutionRequest {
                version: EXECUTION_VERSION,
                entry: self.entry,
                client_version: self.client_version.to_owned(),
                client_pid: std::process::id(),
                #[cfg(unix)]
                creation_mask: crate::creation_mask(),
                #[cfg(windows)]
                creation_mask: None,
                argv: self
                    .args
                    .iter()
                    .map(|arg| NativeString::capture(arg))
                    .collect(),
                env: env
                    .iter()
                    .map(|(key, value)| (NativeString::capture(key), NativeString::capture(value)))
                    .collect(),
                cwd: native_path(self.env.cwd().context("missing client cwd")?),
                home: self.env.home_dir().map(native_path),
                temp: native_path(self.env.temp_dir()),
                cache: self.env.cache_dir().map(native_path),
                executable: native_path(self.executable),
                startup_micros: self
                    .started_at
                    .elapsed()
                    .as_micros()
                    .try_into()
                    .unwrap_or(u64::MAX),
                channels,
            },
            #[cfg(unix)]
            jobserver,
        })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn concurrent_clients_encode_only_their_owned_inputs() {
        let root = tempfile::tempdir().unwrap();
        let files = (0..3)
            .map(|index| File::create(root.path().join(index.to_string())).unwrap())
            .collect::<Vec<_>>();
        std::thread::scope(|threads| {
            let workers = [ExecutionEntry::Original, ExecutionEntry::Compiler]
                .into_iter()
                .enumerate()
                .map(|(index, entry)| {
                    let files = &files;
                    let root = root.path();
                    threads.spawn(move || {
                        let cwd = root.join(index.to_string());
                        let home = cwd.join("home");
                        let temp = cwd.join("tmp");
                        let cache = cwd.join("cache");
                        let value =
                            OsString::from_vec(vec![0xff, b'0' + u8::try_from(index).unwrap()]);
                        let args = [OsString::from("compiler"), value.clone()];
                        let env = InvocationEnv::from_parts(
                            [(OsString::from("CLIENT"), value.clone())],
                            Some(cwd.clone()),
                            Some(home.clone()),
                            temp.clone(),
                        )
                        .with_cache_dir(Some(cache.clone()));
                        let executable = root.join("kache");
                        let execution = Execution {
                            entry,
                            args: &args,
                            env: &env,
                            executable: &executable,
                            started_at: Instant::now(),
                            files,
                            terminal: false,
                            client_version: "fixture-release",
                        };
                        let Prepared {
                            request,
                            jobserver: descriptors,
                        } = execution.prepare().unwrap();
                        assert_eq!(request.entry, entry);
                        assert_eq!(request.client_version, "fixture-release");
                        assert_eq!(request.client_pid, std::process::id());
                        assert_eq!(request.argv[1].decode().unwrap(), value);
                        assert_eq!(request.env.len(), 1);
                        assert_eq!(request.env[0].0.decode().unwrap(), "CLIENT");
                        assert_eq!(request.env[0].1.decode().unwrap(), value);
                        assert_eq!(request.cwd.decode().unwrap(), cwd.as_os_str());
                        assert_eq!(request.home.unwrap().decode().unwrap(), home.as_os_str());
                        assert_eq!(request.temp.decode().unwrap(), temp.as_os_str());
                        assert_eq!(request.cache.unwrap().decode().unwrap(), cache.as_os_str());
                        assert_eq!(request.executable.decode().unwrap(), executable.as_os_str());
                        assert!(descriptors.files().is_empty());
                    })
                })
                .collect::<Vec<_>>();
            for worker in workers {
                worker.join().unwrap();
            }
        });
    }

    #[test]
    fn invalid_stream_manifest_keeps_local_fallback_available() {
        let root = tempfile::tempdir().unwrap();
        let env = InvocationEnv::from_parts(
            Vec::<(OsString, OsString)>::new(),
            Some(root.path().into()),
            None,
            root.path().into(),
        );
        let execution = Execution {
            entry: ExecutionEntry::Original,
            args: &[],
            env: &env,
            executable: &root.path().join("kache"),
            started_at: Instant::now(),
            files: &[],
            terminal: false,
            client_version: "fixture-release",
        };
        assert_eq!(
            execution.try_execute(&root.path().join("absent")).unwrap(),
            None
        );
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::io::AsRawHandle;

    #[test]
    fn request_borrows_owned_handles_and_preserves_native_windows_strings() {
        let root = tempfile::tempdir().unwrap();
        let files = (0..3)
            .map(|i| File::create(root.path().join(i.to_string())).unwrap())
            .collect::<Vec<_>>();
        let native = OsString::from_wide(&[b'x' as u16, 0xd800]);
        let args = [OsString::from("rustc"), native.clone()];
        let env = InvocationEnv::from_parts(
            [(OsString::from("NATIVE"), native.clone())],
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        let executable = std::env::current_exe().unwrap();
        let prepared = Execution {
            entry: ExecutionEntry::Original,
            args: &args,
            env: &env,
            started_at: Instant::now(),
            executable: &executable,
            files: &files,
            terminal: false,
            client_version: "fixture",
        }
        .prepare()
        .unwrap();
        assert_eq!(prepared.request.argv[1].decode().unwrap(), native);
        assert_eq!(prepared.request.env[0].1.decode().unwrap(), native);
        assert_eq!(prepared.request.creation_mask, None);
        assert_eq!(
            prepared.request.channels,
            ExecutionChannels::Windows {
                stdin: files[0].as_raw_handle() as usize as u64,
                stdout: files[1].as_raw_handle() as usize as u64,
                stderr: files[2].as_raw_handle() as usize as u64,
            }
        );
        for file in files {
            assert!(
                file.metadata().is_ok(),
                "request construction must not consume the handles"
            );
        }
    }
}
