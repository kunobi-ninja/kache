use super::{
    BlobSource, Config, EntryMeta, EventResult, FileHashStats, HitCompletion, RustcArgs,
    RustcCompiler, Store, clean_incremental_dir, record_input_prediction, restore_from_cache,
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
        HitCompletion {
            event_root: self.event_root,
            crate_name: self.crate_name,
            result,
            cache_key,
            start: self.start,
            key_ms,
            key_hash_stats,
            lookup_ms,
            restore_ms,
        }
        .report(self.config, meta);
        record_input_prediction(self.config, prediction_store, self.args, true);
        clean_incremental_dir(self.config, self.args);
        Ok(())
    }
}
