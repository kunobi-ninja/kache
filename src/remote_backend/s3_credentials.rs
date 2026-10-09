//! The S3 credential chain.
//!
//! Explicit keys come first, then the selected AWS profile. The remote's
//! profile setting selects one, and so does a non-empty `AWS_PROFILE`. When
//! the selected profile cannot supply credentials the chain stops: going on to
//! web identity, ECS or EC2 instance credentials would sign requests as an
//! identity the user did not choose. Without a selected profile the chain
//! tries the default profile and then those sources, as the AWS SDKs do.

use std::sync::{Arc, Mutex, MutexGuard};

use reqsign_aws_v4::{
    AssumeRoleWithWebIdentityCredentialProvider, Credential, ECSCredentialProvider,
    IMDSv2CredentialProvider, ProcessCredentialProvider, ProfileCredentialProvider,
    SSOCredentialProvider,
};
use reqsign_core::{Context as SigningContext, ProvideCredential, ProvideCredentialDyn};

use super::KacheCommandExecute;

const KACHE_KEYS: &str = "KACHE_S3_ACCESS_KEY and KACHE_S3_SECRET_KEY";
const ENVIRONMENT_KEYS: &str = "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY";

/// Why the chain stopped instead of trying the next credential source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CredentialFailure {
    /// One of `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` without the other.
    PartialEnvironmentKeys {
        present: &'static str,
        missing: &'static str,
    },
    /// The selected profile supplied no credentials, or failed to.
    Profile {
        name: String,
        selected_by: &'static str,
        problem: String,
    },
}

impl std::fmt::Display for CredentialFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartialEnvironmentKeys { present, missing } => {
                write!(f, "{present} is set without {missing}")
            }
            Self::Profile {
                name,
                selected_by,
                problem,
            } => write!(
                f,
                "cannot load AWS profile \"{name}\" (selected by {selected_by}): {problem}"
            ),
        }
    }
}

impl CredentialFailure {
    /// What to change so the remote can sign requests again.
    pub(crate) fn fix(&self) -> String {
        match self {
            Self::PartialEnvironmentKeys { .. } => {
                "set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither".to_string()
            }
            Self::Profile { selected_by, .. } => format!(
                "fix the profile, or unset {selected_by} so kache can use web identity, ECS or \
                 EC2 instance credentials"
            ),
        }
    }
}

/// What the chain last did. The backend reads a failure back to name it in
/// the error of a request the chain stopped, and `kache doctor` reads the
/// source.
#[derive(Debug, Default)]
pub(crate) struct CredentialStatus(Mutex<CredentialState>);

#[derive(Debug, Default)]
struct CredentialState {
    source: Option<String>,
    failure: Option<CredentialFailure>,
}

impl CredentialStatus {
    /// Record credentials from `source`. A new source is logged once, so the
    /// log shows when requests start signing as an instance or task role.
    pub(super) fn loaded(&self, source: &str) {
        let mut state = self.lock();
        state.failure = None;
        if state.source.as_deref() != Some(source) {
            tracing::info!("loaded S3 credentials from {source}");
            state.source = Some(source.to_string());
        }
    }

    /// Record a stopped chain. A new failure is logged once: the reqsign chain
    /// OpenDAL wraps around this one logs the error under its own target, which
    /// Kache's log filters drop, and signs nothing.
    pub(super) fn failed(&self, failure: &CredentialFailure) {
        let mut state = self.lock();
        if state.failure.as_ref() != Some(failure) {
            tracing::warn!("S3 requests will fail: {failure}");
            state.failure = Some(failure.clone());
        }
    }

    pub(super) fn failure(&self) -> Option<CredentialFailure> {
        self.lock().failure.clone()
    }

    pub(super) fn source(&self) -> Option<String> {
        self.lock().source.clone()
    }

    fn lock(&self) -> MutexGuard<'_, CredentialState> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// One credential source, and the name logs and `kache doctor` give it.
pub(super) struct CredentialSource {
    name: String,
    provider: Box<dyn ProvideCredentialDyn<Credential = Credential>>,
}

/// The name alone: the EC2 metadata provider keeps its session token in its
/// own `Debug`.
impl std::fmt::Debug for CredentialSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.name, f)
    }
}

impl CredentialSource {
    pub(super) fn new(
        name: impl Into<String>,
        provider: impl ProvideCredential<Credential = Credential>,
    ) -> Self {
        Self {
            name: name.into(),
            provider: Box::new(provider),
        }
    }
}

/// Sources that need no profile, in the order they are tried.
pub(super) fn ambient_sources(region: &str) -> Vec<CredentialSource> {
    vec![
        CredentialSource::new(
            "web identity token",
            AssumeRoleWithWebIdentityCredentialProvider::new().with_region(region.to_string()),
        ),
        CredentialSource::new(
            "ECS container credentials",
            ECSCredentialProvider::default(),
        ),
        CredentialSource::new("EC2 instance metadata", IMDSv2CredentialProvider::default()),
    ]
}

/// The sources that read the shared profile `profile`. Each is given the name
/// outright: left to itself, it reads an empty `AWS_PROFILE` as a profile
/// named "".
fn profile_sources(profile: &str) -> Vec<CredentialSource> {
    let name = |kind: &str| format!("AWS profile \"{profile}\" ({kind})");
    vec![
        CredentialSource::new(
            name("static keys"),
            ProfileCredentialProvider::new().with_profile(profile),
        ),
        CredentialSource::new(
            name("SSO"),
            SSOCredentialProvider::new().with_profile(profile),
        ),
        CredentialSource::new(
            name("credential_process"),
            ProcessCredentialProvider::new().with_profile(profile),
        ),
    ]
}

/// Kache's S3 credential chain.
pub(super) struct KacheCredentialProvider {
    /// `KACHE_S3_ACCESS_KEY` and `KACHE_S3_SECRET_KEY`, preferred over every
    /// other source.
    kache_keys: Option<Credential>,
    /// The remote's profile setting. Without it, a non-empty `AWS_PROFILE`
    /// selects the profile.
    profile: Option<String>,
    /// Tried after the default profile, and only when no profile is selected.
    ambient: Vec<CredentialSource>,
    status: Arc<CredentialStatus>,
}

impl KacheCredentialProvider {
    pub(super) fn new(
        kache_keys: Option<Credential>,
        profile: Option<String>,
        ambient: Vec<CredentialSource>,
        status: Arc<CredentialStatus>,
    ) -> Self {
        Self {
            kache_keys,
            profile,
            ambient,
            status,
        }
    }

    async fn resolve(
        &self,
        context: &SigningContext,
    ) -> Result<Option<(Credential, String)>, CredentialFailure> {
        if let Some(credential) = &self.kache_keys {
            return Ok(Some((credential.clone(), KACHE_KEYS.to_string())));
        }
        if let Some(credential) = environment_keys(context)? {
            return Ok(Some((credential, ENVIRONMENT_KEYS.to_string())));
        }
        let selected = selected_profile(self.profile.as_deref(), context);
        // A `credential_process` child inherits this process's environment.
        // In place of an empty `AWS_PROFILE`, which it could read as a
        // profile named "", it gets the default profile the chain reads.
        let child_profile = match &selected {
            Some(selected) => Some(selected.name.clone()),
            None => context
                .env_var("AWS_PROFILE")
                .filter(|name| name.is_empty())
                .map(|_| "default".to_string()),
        };
        let context = context.clone().with_command_execute(KacheCommandExecute {
            profile: child_profile,
        });
        match selected {
            Some(selected) => selected.resolve(&context).await.map(Some),
            None => {
                let default_profile = profile_sources("default");
                let sources = default_profile.iter().chain(&self.ambient);
                Ok(first_credential(&context, sources).await)
            }
        }
    }
}

/// Names only. OpenDAL's reqsign chain logs this before each attempt, at
/// debug level, and after an error, at warn.
impl std::fmt::Debug for KacheCredentialProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KacheCredentialProvider")
            .field("kache_keys", &self.kache_keys.is_some())
            .field("profile", &self.profile)
            .field("ambient", &self.ambient)
            .finish_non_exhaustive()
    }
}

impl ProvideCredential for KacheCredentialProvider {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        context: &SigningContext,
    ) -> reqsign_core::Result<Option<Self::Credential>> {
        match self.resolve(context).await {
            Ok(Some((credential, source))) => {
                self.status.loaded(&source);
                Ok(Some(credential))
            }
            Ok(None) => Ok(None),
            Err(failure) => {
                self.status.failed(&failure);
                Err(reqsign_core::Error::config_invalid(failure.to_string()))
            }
        }
    }
}

/// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with `AWS_SESSION_TOKEN`.
/// An empty variable counts as unset, so a pair of empty ones leaves the
/// later sources to sign. A key ID without its secret, or the reverse, is a
/// mistake to report rather than a reason to try the next source.
fn environment_keys(context: &SigningContext) -> Result<Option<Credential>, CredentialFailure> {
    const KEY_ID: &str = "AWS_ACCESS_KEY_ID";
    const SECRET: &str = "AWS_SECRET_ACCESS_KEY";
    let var = |name: &str| context.env_var(name).filter(|value| !value.is_empty());
    match (var(KEY_ID), var(SECRET)) {
        (Some(access_key_id), Some(secret_access_key)) => Ok(Some(Credential {
            access_key_id,
            secret_access_key,
            session_token: var("AWS_SESSION_TOKEN"),
            expires_in: None,
        })),
        (Some(_), None) => Err(CredentialFailure::PartialEnvironmentKeys {
            present: KEY_ID,
            missing: SECRET,
        }),
        (None, Some(_)) => Err(CredentialFailure::PartialEnvironmentKeys {
            present: SECRET,
            missing: KEY_ID,
        }),
        (None, None) => Ok(None),
    }
}

/// A profile the user selected, and the setting that selected it.
struct SelectedProfile {
    name: String,
    selected_by: &'static str,
}

/// The remote's profile setting wins over `AWS_PROFILE`, and an empty
/// `AWS_PROFILE` selects nothing.
fn selected_profile(configured: Option<&str>, context: &SigningContext) -> Option<SelectedProfile> {
    if let Some(name) = configured {
        return Some(SelectedProfile {
            name: name.to_string(),
            selected_by: "cache.remote.profile or KACHE_S3_PROFILE",
        });
    }
    context
        .env_var("AWS_PROFILE")
        .filter(|name| !name.is_empty())
        .map(|name| SelectedProfile {
            name,
            selected_by: "AWS_PROFILE",
        })
}

impl SelectedProfile {
    /// Credentials from this profile only. A source that fails stops the
    /// chain, and so does a profile that supplies nothing.
    async fn resolve(
        self,
        context: &SigningContext,
    ) -> Result<(Credential, String), CredentialFailure> {
        for source in profile_sources(&self.name) {
            match source.provider.provide_credential_dyn(context).await {
                Ok(Some(credential)) => return Ok((credential, source.name)),
                Ok(None) => {}
                Err(error) => return Err(self.failure(error_text(&error))),
            }
        }
        let problem = diagnose_profile(context, &self.name).await;
        Err(self.failure(problem))
    }

    fn failure(self, problem: String) -> CredentialFailure {
        CredentialFailure::Profile {
            name: self.name,
            selected_by: self.selected_by,
            problem,
        }
    }
}

/// The first source with credentials. A source that fails is skipped.
async fn first_credential<'a>(
    context: &SigningContext,
    sources: impl Iterator<Item = &'a CredentialSource>,
) -> Option<(Credential, String)> {
    for source in sources {
        match source.provider.provide_credential_dyn(context).await {
            Ok(Some(credential)) => return Some((credential, source.name.clone())),
            Ok(None) => {}
            Err(error) => tracing::debug!(
                "skipping S3 credentials from {}: {}",
                source.name,
                error_text(&error)
            ),
        }
    }
    None
}

/// Why the selected profile supplied nothing, from the files its sources read.
async fn diagnose_profile(context: &SigningContext, profile: &str) -> String {
    let config = AwsFile::read(context, "AWS_CONFIG_FILE", "~/.aws/config").await;
    let credentials =
        AwsFile::read(context, "AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials").await;
    profile_problem(profile, &config, &credentials)
}

/// A shared config or credentials file, as far as it could be read.
struct AwsFile {
    path: String,
    /// `Ok(None)` when the file does not exist. `Err` says why it cannot be read.
    contents: Result<Option<String>, String>,
}

impl AwsFile {
    /// Read the file `variable` names, or `default`, as the profile sources do.
    async fn read(context: &SigningContext, variable: &str, default: &str) -> Self {
        let configured = context
            .env_var(variable)
            .unwrap_or_else(|| default.to_string());
        let Some(path) = context.expand_home_dir(&configured) else {
            return Self {
                path: configured,
                contents: Err("no home directory".to_string()),
            };
        };
        let contents = match context.file_read(&path).await {
            Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
            Err(error) => match io_error(&error) {
                Some(io) if io.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Some(io) => Err(io.to_string()),
                None => Err(error_text(&error)),
            },
        };
        Self { path, contents }
    }

    fn section(&self, name: &str) -> Option<Vec<String>> {
        match &self.contents {
            Ok(Some(contents)) => section_keys(contents, name),
            Ok(None) | Err(_) => None,
        }
    }
}

/// Why `profile` has no credentials Kache can load.
fn profile_problem(profile: &str, config: &AwsFile, credentials: &AwsFile) -> String {
    for file in [config, credentials] {
        if let Err(error) = &file.contents {
            return format!("cannot read {}: {error}", file.path);
        }
    }
    // The config file names a profile `[profile NAME]`, except `[default]`.
    let config_section = match profile {
        "default" => "default".to_string(),
        name => format!("profile {name}"),
    };
    let in_config = config.section(&config_section);
    let in_credentials = credentials.section(profile);
    if in_config.is_none() && in_credentials.is_none() {
        return format!(
            "it is not defined in {} or {}",
            config.path, credentials.path
        );
    }
    let keys: Vec<String> = in_config
        .into_iter()
        .chain(in_credentials)
        .flatten()
        .collect();
    let has = |key: &str| keys.iter().any(|candidate| candidate == key);
    let problem = if has("sso_session") {
        "it uses an [sso-session] section, which kache does not support"
    } else if has("role_arn") && (has("source_profile") || has("credential_source")) {
        "it assumes a role through source_profile or credential_source, which kache does not \
         support"
    } else if has("web_identity_token_file") {
        "it reads a web identity token, which kache does not support in a profile"
    } else {
        "it has no aws_access_key_id and aws_secret_access_key, legacy SSO settings or \
         credential_process"
    };
    problem.to_string()
}

/// The keys of section `[name]`, or `None` when the file has no such section.
fn section_keys(contents: &str, name: &str) -> Option<Vec<String>> {
    let mut keys = None;
    let mut in_section = false;
    for line in contents.lines().map(str::trim) {
        if let Some(header) = line
            .strip_prefix('[')
            .and_then(|line| line.strip_suffix(']'))
        {
            in_section = header.trim() == name;
            if in_section {
                keys.get_or_insert_with(Vec::new);
            }
        } else if in_section && let Some((key, _)) = line.split_once('=') {
            keys.get_or_insert_with(Vec::new)
                .push(key.trim().to_string());
        }
    }
    keys
}

/// The I/O error under a reqsign file-read error.
fn io_error(error: &reqsign_core::Error) -> Option<&std::io::Error> {
    std::iter::successors(std::error::Error::source(error), |cause| cause.source())
        .find_map(|cause| cause.downcast_ref::<std::io::Error>())
}

/// A reqsign error with its causes, which its `Display` leaves out.
fn error_text(error: &reqsign_core::Error) -> String {
    let mut text = error.to_string();
    for cause in std::iter::successors(std::error::Error::source(error), |cause| cause.source()) {
        text.push_str(": ");
        text.push_str(&cause.to_string());
    }
    text.trim_end().to_string()
}

#[cfg(test)]
mod tests;
