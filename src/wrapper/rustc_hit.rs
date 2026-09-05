use super::{
    BlobSource, Config, EntryMeta, EventResult, FileHashStats, RustcArgs, RustcCompiler, Store,
    clean_incremental_dir, log_event_with_hash_stats, print_progress, record_input_prediction,
    replay_cached_diagnostics, restore_from_cache,
};
use anyhow::Result;
use std::time::Instant;

/// Invocation state shared by every Rust cache-hit path.
pub(super) struct RustcHitContext<'a> {
    pub config: &'a Config,
    pub compiler: &'a RustcCompiler,
    pub args: &'a RustcArgs,
    pub crate_name: &'a str,
    pub event_root: &'a str,
    pub start: Instant,
    pub extra_inputs: Option<&'a crate::extra_inputs::ExtraInputsSnapshot>,
}

impl RustcHitContext<'_> {
    /// Report a hit only after all artifacts restore successfully. Prediction
    /// records belong to the store used for key computation, which can differ
    /// from the store that supplied the artifacts after a volume-shard miss.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn restore_and_finish(
        &self,
        blobs: BlobSource<'_>,
        meta: &EntryMeta,
        result: EventResult,
        cache_key: &str,
        key_ms: u64,
        key_hash_stats: FileHashStats,
        lookup_ms: u64,
        prediction_store: Option<&Store>,
    ) -> Result<()> {
        let restore_start = Instant::now();
        restore_from_cache(
            self.config,
            self.compiler,
            &blobs,
            self.args,
            meta,
            self.extra_inputs,
        )?;
        let restore_ms = restore_start.elapsed().as_millis() as u64;
        let elapsed = self.start.elapsed().as_millis() as u64;
        let size = meta.files.iter().map(|file| file.size).sum();
        log_event_with_hash_stats(
            self.config,
            self.event_root,
            self.crate_name,
            result,
            elapsed,
            meta.compile_time_ms,
            size,
            cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            restore_ms,
            0,
        );
        record_input_prediction(self.config, prediction_store, self.args, true);
        print_progress(self.crate_name, result, elapsed, size);
        replay_cached_diagnostics(meta, std::io::stdout(), std::io::stderr());
        clean_incremental_dir(self.config, self.args);
        Ok(())
    }
}
