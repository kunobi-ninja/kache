use super::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Reads files from disk, as the reader OpenDAL gives the chain does.
#[derive(Debug)]
struct DiskRead;

impl reqsign_core::FileRead for DiskRead {
    async fn file_read(&self, path: &str) -> reqsign_core::Result<Vec<u8>> {
        std::fs::read(path).map_err(|error| {
            reqsign_core::Error::unexpected("failed to read file").with_source(error)
        })
    }
}

/// Stands in for web identity, ECS and EC2: counts its calls and always has
/// credentials, as an instance role does.
#[derive(Debug, Clone, Default)]
struct InstanceRole(Arc<AtomicUsize>);

impl InstanceRole {
    fn calls(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ProvideCredential for InstanceRole {
    type Credential = Credential;

    async fn provide_credential(
        &self,
        _context: &SigningContext,
    ) -> reqsign_core::Result<Option<Credential>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Some(key("AKIAINSTANCE")))
    }
}

fn key(id: &str) -> Credential {
    Credential {
        access_key_id: id.to_string(),
        secret_access_key: "secret".to_string(),
        ..Default::default()
    }
}

/// The chain with `role` as its only source after the profiles.
fn chain(
    kache_keys: Option<Credential>,
    profile: Option<&str>,
    role: &InstanceRole,
) -> KacheCredentialProvider {
    KacheCredentialProvider::new(
        kache_keys,
        profile.map(str::to_string),
        vec![CredentialSource::new("instance role", role.clone())],
        Arc::default(),
    )
}

/// A home directory holding the AWS config and credentials files, which
/// `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` point at.
struct AwsHome(tempfile::TempDir);

impl AwsHome {
    /// `None` leaves that file out.
    fn new(config: Option<&str>, credentials: Option<&str>) -> Self {
        let home = Self(tempfile::tempdir().unwrap());
        if let Some(config) = config {
            std::fs::write(home.config(), config).unwrap();
        }
        if let Some(credentials) = credentials {
            std::fs::write(home.credentials(), credentials).unwrap();
        }
        home
    }

    fn path(&self) -> &Path {
        self.0.path()
    }

    fn config(&self) -> String {
        self.path().join("config").to_str().unwrap().to_string()
    }

    fn credentials(&self) -> String {
        self.path()
            .join("credentials")
            .to_str()
            .unwrap()
            .to_string()
    }

    /// A signing context over only this home and `vars`. Later variables win.
    fn context(&self, vars: &[(&str, &str)]) -> SigningContext {
        let files = [
            ("AWS_CONFIG_FILE".to_string(), self.config()),
            (
                "AWS_SHARED_CREDENTIALS_FILE".to_string(),
                self.credentials(),
            ),
        ];
        let vars = vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()));
        SigningContext::new()
            .with_file_read(DiskRead)
            .with_env(reqsign_core::StaticEnv {
                home_dir: Some(self.path().to_path_buf()),
                envs: files.into_iter().chain(vars).collect(),
            })
    }
}

/// The task's first case: a selected profile that does not exist stops the
/// chain with the profile named, and the instance role is never asked.
#[tokio::test]
async fn a_missing_selected_profile_stops_before_the_instance_role() {
    let home = AwsHome::new(Some("[profile other]\nregion = eu-west-1\n"), None);
    let cases = [
        (None, [("AWS_PROFILE", "missing")], "AWS_PROFILE"),
        // The remote's setting wins over AWS_PROFILE.
        (
            Some("missing"),
            [("AWS_PROFILE", "other")],
            "cache.remote.profile or KACHE_S3_PROFILE",
        ),
    ];
    for (configured, vars, selected_by) in cases {
        let role = InstanceRole::default();
        let chain = chain(None, configured, &role);
        let error = chain
            .provide_credential(&home.context(&vars))
            .await
            .expect_err("a missing profile must stop the chain");
        let expected = format!(
            "cannot load AWS profile \"missing\" (selected by {selected_by}): it is not \
             defined in {} or {}",
            home.config(),
            home.credentials()
        );
        assert_eq!(error.to_string(), expected);
        assert_eq!(role.calls(), 0, "no later source may be asked");
        assert_eq!(
            chain.status.failure().map(|failure| failure.to_string()),
            Some(expected)
        );
    }
}

/// Without a selected profile the chain goes on past the default profile to
/// web identity, ECS and EC2, as it always has. An empty `AWS_PROFILE`
/// selects nothing.
#[tokio::test]
async fn without_a_selected_profile_the_chain_reaches_the_instance_role() {
    let home = AwsHome::new(Some("[profile other]\nregion = eu-west-1\n"), None);
    for vars in [&[][..], &[("AWS_PROFILE", "")]] {
        let role = InstanceRole::default();
        let chain = chain(None, None, &role);
        let credential = chain
            .provide_credential(&home.context(vars))
            .await
            .unwrap()
            .expect("the instance role has credentials");
        assert_eq!(credential.access_key_id, "AKIAINSTANCE");
        assert_eq!(role.calls(), 1);
        assert_eq!(chain.status.source().as_deref(), Some("instance role"));
    }
}

/// A key ID without its secret, or the reverse, is refused instead of
/// skipped. An empty key ID counts as unset.
#[tokio::test]
async fn a_key_id_without_a_secret_is_refused() {
    let home = AwsHome::new(None, None);
    let key_id_only = "AWS_ACCESS_KEY_ID is set without AWS_SECRET_ACCESS_KEY";
    let cases = [
        (&[("AWS_ACCESS_KEY_ID", "AKIAHALF")][..], key_id_only),
        (
            &[
                ("AWS_ACCESS_KEY_ID", "AKIAHALF"),
                ("AWS_SECRET_ACCESS_KEY", ""),
            ],
            key_id_only,
        ),
        (
            &[("AWS_SECRET_ACCESS_KEY", "secret")],
            "AWS_SECRET_ACCESS_KEY is set without AWS_ACCESS_KEY_ID",
        ),
    ];
    for (vars, expected) in cases {
        let role = InstanceRole::default();
        let error = chain(None, None, &role)
            .provide_credential(&home.context(vars))
            .await
            .expect_err(expected);
        assert_eq!(error.to_string(), expected);
        assert_eq!(role.calls(), 0, "no later source may be asked");
    }

    let role = InstanceRole::default();
    let credential = chain(None, None, &role)
        .provide_credential(&home.context(&[("AWS_ACCESS_KEY_ID", "")]))
        .await
        .unwrap();
    assert_eq!(credential.unwrap().access_key_id, "AKIAINSTANCE");
}

/// Explicit keys still come before the selected profile, so a missing profile
/// does not matter while they are set.
#[tokio::test]
async fn explicit_keys_come_before_the_selected_profile() {
    let home = AwsHome::new(None, None);
    let role = InstanceRole::default();

    // Kache's own keys win; the half AWS pair is never read.
    let kache = chain(Some(key("AKIAKACHE")), None, &role);
    let credential = kache
        .provide_credential(&home.context(&[
            ("AWS_PROFILE", "missing"),
            ("AWS_ACCESS_KEY_ID", "AKIAHALF"),
        ]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.access_key_id, "AKIAKACHE");
    assert_eq!(
        kache.status.source().as_deref(),
        Some("KACHE_S3_ACCESS_KEY and KACHE_S3_SECRET_KEY")
    );

    let environment = chain(None, Some("missing"), &role);
    let credential = environment
        .provide_credential(&home.context(&[
            ("AWS_ACCESS_KEY_ID", "AKIAENV"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
        ]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.access_key_id, "AKIAENV");
    assert_eq!(
        environment.status.source().as_deref(),
        Some("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
    );
    assert_eq!(role.calls(), 0);
}

#[tokio::test]
async fn the_selected_profile_supplies_its_credentials() {
    let home = AwsHome::new(
        None,
        Some("[team]\naws_access_key_id = AKIATEAM\naws_secret_access_key = secret\n"),
    );
    let role = InstanceRole::default();
    let chain = chain(None, Some("team"), &role);
    let credential = chain
        .provide_credential(&home.context(&[("AWS_PROFILE", "other")]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(credential.access_key_id, "AKIATEAM");
    assert_eq!(
        chain.status.source().as_deref(),
        Some("AWS profile \"team\" (static keys)")
    );
    assert_eq!(role.calls(), 0);
}

/// A source that fails stops a selected profile, with the error and its
/// cause. Without a selected profile the chain skips it, as it always has.
#[tokio::test]
async fn a_broken_credentials_file_stops_only_a_selected_profile() {
    let home = AwsHome::new(None, Some("[default\n"));
    let role = InstanceRole::default();
    let error = chain(None, None, &role)
        .provide_credential(&home.context(&[("AWS_PROFILE", "team")]))
        .await
        .expect_err("a broken file must stop a selected profile")
        .to_string();
    let prefix = "cannot load AWS profile \"team\" (selected by AWS_PROFILE): failed to parse \
                  credentials file: ";
    assert!(
        error.starts_with(prefix) && error.len() > prefix.len(),
        "{error}"
    );
    assert_eq!(role.calls(), 0);

    let credential = chain(None, None, &role)
        .provide_credential(&home.context(&[]))
        .await
        .unwrap();
    assert_eq!(credential.unwrap().access_key_id, "AKIAINSTANCE");
    assert_eq!(role.calls(), 1);
}

/// A source of the selected profile that fails stops the chain with its
/// error. `sh` runs the script so no test executes a file it just wrote.
#[cfg(unix)]
#[tokio::test]
async fn a_failing_credential_process_stops_the_chain() {
    let home = AwsHome::new(None, None);
    let script = home.path().join("fail.sh");
    std::fs::write(&script, "echo boom >&2\nexit 3\n").unwrap();
    std::fs::write(
        home.config(),
        format!(
            "[profile build]\ncredential_process = sh {}\n",
            script.display()
        ),
    )
    .unwrap();
    let role = InstanceRole::default();
    let error = chain(None, None, &role)
        .provide_credential(&home.context(&[("AWS_PROFILE", "build")]))
        .await
        .expect_err("a failing credential_process must stop the chain");
    assert_eq!(
        error.to_string(),
        "cannot load AWS profile \"build\" (selected by AWS_PROFILE): credential process \
         failed with status 3: boom"
    );
    assert_eq!(role.calls(), 0);
}

#[tokio::test]
async fn an_expired_sso_token_stops_the_chain() {
    let home = AwsHome::new(
        Some(
            "[profile dev]\nsso_start_url = https://example.awsapps.com/start\n\
             sso_region = eu-west-1\nsso_account_id = 123456789012\nsso_role_name = Build\n",
        ),
        None,
    );
    // The token cache is named after the SHA-1 of the start URL.
    let cache = home.path().join(".aws/sso/cache");
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        cache.join("e8be5486177c5b5392bd9aa76563515b29358e6e.json"),
        r#"{"accessToken":"token","expiresAt":"2020-01-01T00:00:00Z"}"#,
    )
    .unwrap();
    let role = InstanceRole::default();
    let error = chain(None, Some("dev"), &role)
        .provide_credential(&home.context(&[]))
        .await
        .expect_err("an expired SSO token must stop the chain");
    assert_eq!(
        error.to_string(),
        "cannot load AWS profile \"dev\" (selected by cache.remote.profile or \
         KACHE_S3_PROFILE): No valid SSO token found. Please run 'aws sso login' first"
    );
    assert_eq!(role.calls(), 0);
}

#[tokio::test]
async fn an_unreadable_credentials_file_is_named() {
    let home = AwsHome::new(Some("[profile other]\n"), None);
    // A directory cannot be read as a file.
    let unreadable = home.path().to_str().unwrap();
    let error = chain(None, None, &InstanceRole::default())
        .provide_credential(&home.context(&[
            ("AWS_PROFILE", "team"),
            ("AWS_SHARED_CREDENTIALS_FILE", unreadable),
        ]))
        .await
        .unwrap_err()
        .to_string();
    let prefix = format!(
        "cannot load AWS profile \"team\" (selected by AWS_PROFILE): cannot read {unreadable}: "
    );
    assert!(error.starts_with(&prefix), "{error}");
    assert!(
        !error.contains("failed to read file"),
        "the I/O error stands alone: {error}"
    );
}

fn file(path: &str, contents: Option<&str>) -> AwsFile {
    AwsFile {
        path: path.to_string(),
        contents: Ok(contents.map(str::to_string)),
    }
}

#[test]
fn a_profile_kache_cannot_load_is_explained() {
    let role = "it assumes a role through source_profile or credential_source, which kache \
                does not support";
    let sso_session = "it uses an [sso-session] section, which kache does not support";
    let nothing = "it has no aws_access_key_id and aws_secret_access_key, legacy SSO settings \
                   or credential_process";
    let cases = [
        // Another section's keys do not count.
        (
            "team",
            "[profile base]\nsso_session = corp\n[profile team]\nrole_arn = arn:aws:iam::1:role/r\n\
             source_profile = base\n",
            role,
        ),
        (
            "team",
            "[profile team]\nrole_arn = arn:aws:iam::1:role/r\ncredential_source = Ec2InstanceMetadata\n",
            role,
        ),
        (
            "team",
            "[profile team]\nsso_session = corp\nsso_account_id = 1\n",
            sso_session,
        ),
        ("default", "[default]\nsso_session = corp\n", sso_session),
        (
            "team",
            "[profile team]\nrole_arn = arn:aws:iam::1:role/r\nweb_identity_token_file = /token\n",
            "it reads a web identity token, which kache does not support in a profile",
        ),
        // source_profile without role_arn assumes nothing.
        ("team", "[profile team]\nsource_profile = base\n", nothing),
        // A profile with no settings is still defined.
        (
            "team",
            "[profile team]\n[profile other]\nregion = eu-west-1\n",
            nothing,
        ),
        // The config file needs `[profile team]`.
        (
            "team",
            "[team]\nregion = eu-west-1\n",
            "it is not defined in /aws/config or /aws/credentials",
        ),
    ];
    let credentials = file("/aws/credentials", None);
    for (profile, config, expected) in cases {
        let config = file("/aws/config", Some(config));
        assert_eq!(
            profile_problem(profile, &config, &credentials),
            expected,
            "{profile}"
        );
    }

    // Defined in the credentials file alone, with half a key pair.
    let credentials = file(
        "/aws/credentials",
        Some("[team]\naws_access_key_id = AKIA\n"),
    );
    assert_eq!(
        profile_problem("team", &file("/aws/config", None), &credentials),
        nothing
    );

    let unreadable = AwsFile {
        path: "/aws/config".to_string(),
        contents: Err("Permission denied (os error 13)".to_string()),
    };
    assert_eq!(
        profile_problem("team", &unreadable, &file("/aws/credentials", None)),
        "cannot read /aws/config: Permission denied (os error 13)"
    );
}

#[test]
fn ambient_sources_try_web_identity_then_ecs_then_ec2() {
    let names: Vec<String> = ambient_sources("us-east-1")
        .into_iter()
        .map(|source| source.name)
        .collect();
    assert_eq!(
        names,
        [
            "web identity token",
            "ECS container credentials",
            "EC2 instance metadata"
        ]
    );
}

/// OpenDAL's reqsign chain logs this chain's `Debug` before each attempt, so
/// it names the sources and leaves out their state: the EC2 metadata source
/// keeps its session token there.
#[test]
fn debug_names_the_sources_without_their_state() {
    let chain = KacheCredentialProvider::new(
        Some(key("AKIAKACHE")),
        Some("team".to_string()),
        ambient_sources("us-east-1"),
        Arc::default(),
    );
    assert_eq!(
        format!("{chain:?}"),
        "KacheCredentialProvider { kache_keys: true, profile: Some(\"team\"), ambient: \
         [\"web identity token\", \"ECS container credentials\", \"EC2 instance metadata\"], .. }"
    );
}

#[test]
fn credentials_that_load_again_clear_the_failure() {
    let status = CredentialStatus::default();
    let failure = CredentialFailure::PartialEnvironmentKeys {
        present: "AWS_ACCESS_KEY_ID",
        missing: "AWS_SECRET_ACCESS_KEY",
    };
    status.failed(&failure);
    assert_eq!(status.failure(), Some(failure));
    status.loaded("EC2 instance metadata");
    assert_eq!(status.failure(), None);
    assert_eq!(status.source().as_deref(), Some("EC2 instance metadata"));
}

#[test]
fn each_failure_says_what_to_change() {
    let partial = CredentialFailure::PartialEnvironmentKeys {
        present: "AWS_SECRET_ACCESS_KEY",
        missing: "AWS_ACCESS_KEY_ID",
    };
    assert_eq!(
        partial.fix(),
        "set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither"
    );
    // On EC2, unsetting the selection is how to use the instance role.
    let profile = CredentialFailure::Profile {
        name: "team".to_string(),
        selected_by: "AWS_PROFILE",
        problem: "it is not defined".to_string(),
    };
    assert_eq!(
        profile.fix(),
        "fix the profile, or unset AWS_PROFILE so kache can use web identity, ECS or EC2 \
         instance credentials"
    );
}
