//! Credential providers are separate from the cached registry access tokens.

use super::*;
use std::path::PathBuf;

#[derive(Clone)]
pub(super) enum CredentialSource {
    Fixed(RegistryAuth),
    Docker(PathBuf),
}

impl CredentialSource {
    pub(super) fn from_environment() -> Result<Self> {
        fn value(name: &str) -> Result<Option<String>> {
            std::env::var_os(name)
                .map(|value| {
                    value
                        .into_string()
                        .map_err(|_| anyhow::anyhow!("{name} must be UTF-8"))
                })
                .transpose()
        }
        let auth = environment_credentials(
            value("KACHE_OCI_USERNAME")?,
            value("KACHE_OCI_PASSWORD")?,
            value("KACHE_OCI_TOKEN")?,
        )?;
        if let Some(auth) = auth {
            return Ok(Self::Fixed(auth));
        }
        let directory = std::env::var_os("DOCKER_CONFIG")
            .map(PathBuf::from)
            .or_else(|| dirs::home_dir().map(|home| home.join(".docker")))
            .context("cannot find Docker credential config directory")?;
        Ok(Self::Docker(directory))
    }

    pub(super) async fn load(&self, registry: &str) -> Result<RegistryAuth> {
        match self {
            Self::Fixed(auth) => Ok(auth.clone()),
            Self::Docker(directory) => {
                let directory = directory.clone();
                let registry = registry.to_string();
                // Reload config and rerun helpers during each token negotiation,
                // including after a rejected token in a long-lived daemon.
                tokio::task::spawn_blocking(move || load_credentials(&directory, &registry))
                    .await
                    .context("loading OCI credentials")?
            }
        }
    }
}

fn environment_credentials(
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
) -> Result<Option<RegistryAuth>> {
    match (username, password, token) {
        (None, None, None) => Ok(None),
        (None, None, Some(token)) if !token.is_empty() => Ok(Some(RegistryAuth::Bearer(token))),
        (Some(username), Some(password), None) if !username.is_empty() && !password.is_empty() => {
            Ok(Some(RegistryAuth::Basic(username, password)))
        }
        _ => anyhow::bail!(
            "set KACHE_OCI_USERNAME and KACHE_OCI_PASSWORD together, or KACHE_OCI_TOKEN alone; \
             OCI credential values must not be empty"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_credentials_require_one_complete_method_and_hide_values() {
        assert_eq!(environment_credentials(None, None, None).unwrap(), None);
        assert_eq!(
            environment_credentials(Some("user".into()), Some("secret".into()), None).unwrap(),
            Some(RegistryAuth::Basic("user".into(), "secret".into()))
        );
        assert_eq!(
            environment_credentials(None, None, Some("secret".into())).unwrap(),
            Some(RegistryAuth::Bearer("secret".into()))
        );
        for (user, password, token) in [
            (Some("user"), None, None),
            (None, Some("secret"), None),
            (Some(""), Some("secret"), None),
            (Some("user"), Some(""), None),
            (None, None, Some("")),
            (Some("user"), Some("secret"), Some("token-secret")),
            (Some("user"), None, Some("token-secret")),
        ] {
            let error = environment_credentials(
                user.map(str::to_string),
                password.map(str::to_string),
                token.map(str::to_string),
            )
            .unwrap_err();
            assert!(!format!("{error:#}").contains("secret"));
        }
    }
}
