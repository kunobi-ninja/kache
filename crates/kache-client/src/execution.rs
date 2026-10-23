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
        self.try_execute_with_trust(socket, false)
    }

    /// Both peers must opt in to daemon-side workspace accesses under the
    /// daemon's restrictions. Native children remain owned by the client.
    pub fn try_execute_with_trust(
        &self,
        socket: &Path,
        trusted_pipeline: bool,
    ) -> Result<Option<i32>> {
        let prepared = match self.prepare(trusted_pipeline) {
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

    fn prepare(&self, trusted_pipeline: bool) -> Result<Prepared> {
        #[cfg(not(target_os = "linux"))]
        let _ = trusted_pipeline;
        #[cfg(unix)]
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
        #[cfg(all(unix, not(target_os = "linux")))]
        let channels = ExecutionChannels::Unix {
            terminal: self.terminal,
            jobserver: originals,
        };
        #[cfg(target_os = "linux")]
        let channels = ExecutionChannels::UnixClient {
            terminal: self.terminal,
            jobserver: originals,
            trusted_pipeline,
        };
        #[cfg(windows)]
        let channels = {
            use std::os::windows::io::AsRawHandle;
            anyhow::ensure!(
                self.files.is_empty() || self.files.len() == 3,
                "invalid Windows compiler stream manifest"
            );
            let handle = |index: usize| {
                self.files
                    .get(index)
                    .map_or(0, |file| file.as_raw_handle() as usize as u64)
            };
            ExecutionChannels::Windows {
                stdin: handle(0),
                stdout: handle(1),
                stderr: handle(2),
                process_broker: true,
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
    fn accepted_request_transfers_owned_streams_and_returns_the_daemons_exit_code() {
        use kache_protocol::execution::{DescriptorTicket, ExecutionCommand, ExecutionReply};
        use std::io::{BufRead, Read, Seek, Write};
        use std::os::unix::net::{UnixDatagram, UnixListener};
        use std::time::Duration;
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let socket = root.path().join("control");
        let listener = UnixListener::bind(&socket).unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = root.path().join("descriptors");
        let datagram = UnixDatagram::bind(&endpoint).unwrap();
        datagram
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut files = (0..3)
            .map(|_| tempfile::tempfile().unwrap())
            .collect::<Vec<_>>();
        files[0].write_all(b"client-input").unwrap();
        files[0].rewind().unwrap();
        let env = InvocationEnv::from_parts(
            [("CLIENT", "one-invocation")],
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        std::thread::scope(|threads| {
            let server = threads.spawn(|| {
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(1))
                        }
                        Err(error) => panic!("client did not connect: {error}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let kache_protocol::Request::ExecuteV1(request) =
                    serde_json::from_str(&line).unwrap()
                else {
                    panic!("wrong execution request");
                };
                assert_eq!(request.client_version, "fixture-client");
                assert_eq!(request.client_pid, std::process::id());
                assert_eq!(request.env.len(), 1);
                assert_eq!(request.env[0].1.decode().unwrap(), "one-invocation");
                let mut reply = |reply: &ExecutionReply| {
                    serde_json::to_writer(&mut stream, reply).unwrap();
                    stream.write_all(b"\n").unwrap();
                };
                let ticket = DescriptorTicket {
                    endpoint: NativeString::capture(endpoint.as_os_str()),
                    key: [11; 16],
                };
                reply(&ExecutionReply::Ready {
                    version: EXECUTION_VERSION,
                    descriptors: Some(ticket.clone()),
                });
                let mut transferred = kache_ipc::descriptors::receive(
                    datagram.as_fd(),
                    &ticket.key,
                    if cfg!(target_os = "linux") { 5 } else { 3 },
                )
                .unwrap()
                .into_iter()
                .map(File::from)
                .collect::<Vec<_>>();
                let mut input = Vec::new();
                transferred[0].read_to_end(&mut input).unwrap();
                assert_eq!(input, b"client-input");
                transferred[1].write_all(b"daemon-output").unwrap();
                transferred[2].write_all(b"daemon-error").unwrap();
                reply(&ExecutionReply::Prepared);
                line.clear();
                reader.read_line(&mut line).unwrap();
                assert_eq!(
                    serde_json::from_str::<ExecutionCommand>(&line).unwrap(),
                    ExecutionCommand::Commit
                );
                reply(&ExecutionReply::Accepted);
                reply(&ExecutionReply::Complete { exit_code: 23 });
            });
            let result = Execution {
                entry: ExecutionEntry::Original,
                args: &["compiler".into()],
                env: &env,
                executable: &root.path().join("kache"),
                started_at: Instant::now(),
                files: &files,
                terminal: false,
                client_version: "fixture-client",
            }
            .try_execute(&socket);
            assert_eq!(result.unwrap(), Some(23));
            server.join().unwrap();
        });
        for (file, expected) in files[1..]
            .iter_mut()
            .zip([b"daemon-output", b"daemon-error".as_slice()])
        {
            file.rewind().unwrap();
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, expected);
        }
    }

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
                        } = execution.prepare(false).unwrap();
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
        .prepare(false)
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
                process_broker: true,
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
