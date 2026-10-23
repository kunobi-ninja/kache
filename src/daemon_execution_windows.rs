//! The daemon owns the pipeline; its frontend owns native Windows launches.

use super::*;
use crate::invocation::{
    InvocationContext, InvocationEnv,
    io::InvocationIo,
    process::Cancellation,
    windows_broker::{Broker, Call},
};
use kache_ipc::windows_handles::PeerProcess;
use kache_protocol::execution::{
    EXECUTION_VERSION, ExecutionChannels, ExecutionCommand, ExecutionEntry, ExecutionReply,
    ExecutionRequest, NativeString, windows::Reply,
};
use std::ffi::OsString;
use tokio::io::AsyncReadExt;

const PREPARE_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn probe(
    stream: &TokioStream,
    client_pid: u32,
    version: &str,
    client_trust: bool,
    daemon: &Daemon,
) -> kache_protocol::execution::ExecutionStatus {
    let eligible = version == daemon.version
        && PeerProcess::for_connection(stream, client_pid)
            .is_ok_and(|peer| peer.permits_delegation().unwrap_or(false));
    kache_protocol::execution::ExecutionStatus {
        protocol_version: EXECUTION_VERSION,
        eligible,
        client_trust_enabled: client_trust,
        daemon_trust_enabled: daemon.config.trust_daemon_execution,
        detail: if eligible {
            "client process and restore broker"
        } else {
            "local engine: release or process security context is incompatible"
        }
        .into(),
    }
}

struct Prepared {
    entry: ExecutionEntry,
    args: Vec<String>,
    env: InvocationEnv,
    executable: PathBuf,
    started_at: Instant,
}

impl Prepared {
    fn decode(request: ExecutionRequest, version: &str) -> Result<Self> {
        anyhow::ensure!(
            request.version == EXECUTION_VERSION && request.client_version == version,
            "compiler client and daemon execution versions differ"
        );
        anyhow::ensure!(
            request.creation_mask.is_none(),
            "compiler creation policy belongs to another platform"
        );
        anyhow::ensure!(
            matches!(
                request.channels,
                ExecutionChannels::Windows {
                    process_broker: true,
                    ..
                }
            ),
            "Windows compiler requires frontend-owned native process creation"
        );
        let vars = request
            .env
            .iter()
            .map(|(key, value)| Ok((key.decode()?, value.decode()?)))
            .collect::<Result<Vec<(OsString, OsString)>>>()?;
        anyhow::ensure!(
            vars.iter().all(|(key, _)| {
                use std::os::windows::ffi::OsStrExt;
                let key = key.encode_wide().collect::<Vec<_>>();
                !key.is_empty()
                    && (!key.contains(&u16::from(b'='))
                        || (key.len() == 3
                            && key[0] == u16::from(b'=')
                            && key[2] == u16::from(b':')
                            && u8::try_from(key[1]).is_ok_and(|drive| drive.is_ascii_alphabetic())))
            }),
            "invalid compiler environment key"
        );
        let env = InvocationEnv::from_parts(
            vars,
            Some(PathBuf::from(request.cwd.decode()?)),
            request
                .home
                .as_ref()
                .map(NativeString::decode)
                .transpose()?
                .map(PathBuf::from),
            PathBuf::from(request.temp.decode()?),
        )
        .with_cache_dir(
            request
                .cache
                .as_ref()
                .map(NativeString::decode)
                .transpose()?
                .map(PathBuf::from),
        );
        env.validate()?;
        anyhow::ensure!(
            env.cwd().is_some_and(Path::is_dir)
                && env.home_dir().is_none_or(Path::is_absolute)
                && env.cache_dir().is_none_or(Path::is_absolute),
            "compiler directories are unavailable"
        );
        let executable = PathBuf::from(request.executable.decode()?);
        anyhow::ensure!(
            executable.is_absolute(),
            "compiler frontend path must be absolute"
        );
        let args = request
            .argv
            .iter()
            .map(|arg| {
                arg.decode()?
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("compiler arguments require local execution"))
            })
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!args.is_empty(), "compiler argv is empty");
        anyhow::ensure!(
            request.entry != ExecutionEntry::Original
                || crate::original_compiler_request(&args, &env),
            "original entry is not a compiler invocation"
        );
        let started_at = Instant::now()
            .checked_sub(Duration::from_micros(request.startup_micros))
            .context("invalid compiler startup duration")?;
        Ok(Self {
            entry: request.entry,
            args,
            env,
            executable,
            started_at,
        })
    }
}

struct CancelOnDrop(Cancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn write_reply(stream: &TokioStream, reply: &ExecutionReply) -> Result<()> {
    let mut frame = serde_json::to_vec(reply)?;
    frame.push(b'\n');
    let mut writer = stream;
    writer.write_all(&frame).await?;
    writer.flush().await?;
    Ok(())
}

async fn decline(stream: &TokioStream, reason: impl ToString) -> Result<()> {
    tokio::time::timeout(
        PREPARE_TIMEOUT,
        write_reply(
            stream,
            &ExecutionReply::Declined {
                reason: reason.to_string(),
            },
        ),
    )
    .await?
}

pub(super) async fn handle(
    stream: &TokioStream,
    reader: &mut BufReader<&TokioStream>,
    request: ExecutionRequest,
    daemon: &Arc<Daemon>,
    lifecycle: &Arc<Lifecycle>,
) -> Result<()> {
    let peer = match PeerProcess::for_connection(stream, request.client_pid) {
        Ok(peer) if peer.permits_delegation().unwrap_or(false) => Arc::new(peer),
        _ => return decline(stream, "compiler security context requires local execution").await,
    };
    let prepared = match Prepared::decode(request, &daemon.version) {
        Ok(prepared) => prepared,
        Err(error) => return decline(stream, error).await,
    };
    let permit = match daemon.execution_workers.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return decline(stream, "compiler workers are busy").await,
    };
    tokio::time::timeout(PREPARE_TIMEOUT, async {
        write_reply(
            stream,
            &ExecutionReply::Ready {
                version: EXECUTION_VERSION,
                descriptors: None,
            },
        )
        .await?;
        write_reply(stream, &ExecutionReply::Prepared).await
    })
    .await??;
    let mut frame = Vec::new();
    let commit = tokio::select! {
        _ = lifecycle.draining() => return Ok(()),
        line = tokio::time::timeout(PREPARE_TIMEOUT, read_bounded_line(reader, &mut frame)) => line??,
    }.context("compiler frontend disconnected before Commit")?;
    anyhow::ensure!(
        serde_json::from_str::<ExecutionCommand>(&commit)? == ExecutionCommand::Commit,
        "compiler frontend did not commit"
    );
    peer.verify_connection(stream)?;
    anyhow::ensure!(
        peer.permits_delegation()?,
        "compiler security context changed before Commit"
    );
    tokio::time::timeout(
        PREPARE_TIMEOUT,
        write_reply(stream, &ExecutionReply::Accepted),
    )
    .await??;
    let cancellation = Cancellation::default();
    let _cancel = CancelOnDrop(cancellation.clone());
    let (send, mut calls) = tokio::sync::mpsc::channel(8);
    // At most 64 frontend-owned children can be pending. Their final drops
    // can happen on this async task, so cleanup has a nonblocking channel.
    let (finish, mut finished) = tokio::sync::mpsc::unbounded_channel();
    let broker = Arc::new(Broker::new(send, finish, peer, prepared.env.clone()));
    let context = InvocationContext {
        daemon: Some(Arc::clone(daemon)),
        env: prepared.env,
        started_at: prepared.started_at,
        shim_stem: prepared
            .executable
            .file_stem()
            .context("missing compiler frontend name")?
            .to_string_lossy()
            .into_owned(),
        io: InvocationIo::from_broker(broker),
        cancellation: Some(cancellation.clone()),
    };
    let activity = daemon.request_clock.begin_activity();
    let mut worker = tokio::task::spawn_blocking(move || {
        let _activity = activity;
        let _permit = permit;
        match prepared.entry {
            ExecutionEntry::Compiler => crate::run_compiler_invocation(&prepared.args, context),
            ExecutionEntry::Original => crate::run_original_compiler_invocation(
                &prepared.args,
                context,
                &prepared.executable,
            ),
        }
    });
    let mut unexpected = [0u8; 1];
    let result = loop {
        let call = tokio::select! {
            biased;
            disconnected = reader.read(&mut unexpected) => {
                let _ = disconnected;
                drop(calls);
                drop(finished);
                cancellation.cancel();
                let _ = worker.await;
                return Ok(());
            }
            // Reap completed owners before admitting another native spawn.
            Some(id) = finished.recv() => Call {
                command: kache_protocol::execution::windows::Command::Finish { id },
                result: None,
            },
            Some(call) = calls.recv() => call,
            result = &mut worker => break result,
        };
        if let Err(error) = service_call(stream, reader, call).await {
            drop(calls);
            drop(finished);
            cancellation.cancel();
            let _ = worker.await;
            return Err(error);
        }
    };
    let reply = match result {
        Ok(Ok(exit_code)) => ExecutionReply::Complete { exit_code },
        Ok(Err(error)) => ExecutionReply::Failed {
            message: format!("{error:#}"),
        },
        Err(error) => ExecutionReply::Failed {
            message: format!("compiler worker failed: {error}"),
        },
    };
    write_reply(stream, &reply).await
}

async fn service_call(
    stream: &TokioStream,
    reader: &mut BufReader<&TokioStream>,
    call: Call,
) -> Result<()> {
    write_reply(
        stream,
        &ExecutionReply::WindowsControl {
            command: call.command,
        },
    )
    .await?;
    let mut frame = Vec::new();
    let line = read_bounded_line(reader, &mut frame)
        .await?
        .context("compiler frontend disconnected during a native operation")?;
    let reply = serde_json::from_str::<Reply>(&line)?;
    if let Some(sender) = call.result {
        let _ = sender.send(Ok(reply));
    }
    Ok(())
}
