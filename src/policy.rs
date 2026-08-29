//! Effective remote-write permission from the process environment.
//!
//! Configured `remote_readonly` still wins when it is on. This module only
//! *adds* read-only: untrusted CI must not publish, even if the job was given
//! a write credential. Local shells and non-CI processes are left as
//! configured so `kache sync --push` from a laptop still works.
//!
//! `KACHE_REMOTE_READONLY=0` does not turn this off. There is no override for
//! untrusted CI; drop the write credential instead.

/// Why remote writes were suppressed by CI context, if they were.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForcedReadonly {
    pub reason: String,
}

/// If this process is untrusted CI, remote writes must be suppressed.
pub(crate) fn forced_remote_readonly() -> Option<ForcedReadonly> {
    forced_remote_readonly_with(|name| std::env::var(name).ok())
}

pub(crate) fn forced_remote_readonly_with(
    get_env: impl Fn(&str) -> Option<String>,
) -> Option<ForcedReadonly> {
    if env_truthy(get_env("GITHUB_ACTIONS")) {
        return github_forced_readonly(&get_env);
    }
    if env_truthy(get_env("GITLAB_CI")) {
        return gitlab_forced_readonly(&get_env);
    }
    None
}

fn github_forced_readonly(get_env: &impl Fn(&str) -> Option<String>) -> Option<ForcedReadonly> {
    if is_trusted_github_writer(get_env) {
        return None;
    }
    let event = display_env(get_env("GITHUB_EVENT_NAME"));
    let ref_type = display_env(get_env("GITHUB_REF_TYPE"));
    Some(ForcedReadonly {
        reason: format!("GitHub Actions {event} ({ref_type}) is not a protected-branch push"),
    })
}

fn gitlab_forced_readonly(get_env: &impl Fn(&str) -> Option<String>) -> Option<ForcedReadonly> {
    if is_trusted_gitlab_writer(get_env) {
        return None;
    }
    let source = display_env(get_env("CI_PIPELINE_SOURCE"));
    Some(ForcedReadonly {
        reason: format!("GitLab CI {source} is not a protected-branch push"),
    })
}

fn is_trusted_github_writer(get_env: &impl Fn(&str) -> Option<String>) -> bool {
    get_env("GITHUB_EVENT_NAME").as_deref() == Some("push")
        && get_env("GITHUB_REF_TYPE").as_deref() == Some("branch")
        && env_truthy(get_env("GITHUB_REF_PROTECTED"))
}

fn is_trusted_gitlab_writer(get_env: &impl Fn(&str) -> Option<String>) -> bool {
    get_env("CI_PIPELINE_SOURCE").as_deref() == Some("push")
        && get_env("CI_COMMIT_TAG").is_none_or(|value| value.is_empty())
        && get_env("CI_MERGE_REQUEST_IID").is_none_or(|value| value.is_empty())
        && env_truthy(get_env("CI_COMMIT_REF_PROTECTED"))
}

fn env_truthy(value: Option<String>) -> bool {
    value.is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn display_env(value: Option<String>) -> String {
    match value {
        Some(value) if !value.is_empty() => value,
        _ => "unknown".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(vars: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
        |name| vars.get(name).cloned()
    }

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn github(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut vars = env(&[("GITHUB_ACTIONS", "true")]);
        vars.extend(env(extra));
        vars
    }

    fn gitlab(extra: &[(&str, &str)]) -> HashMap<String, String> {
        let mut vars = env(&[("GITLAB_CI", "true")]);
        vars.extend(env(extra));
        vars
    }

    #[test]
    fn local_shell_is_not_forced_readonly() {
        let vars = HashMap::new();
        assert_eq!(forced_remote_readonly_with(lookup(&vars)), None);
    }

    #[test]
    fn github_protected_branch_push_may_write() {
        let vars = github(&[
            ("GITHUB_EVENT_NAME", "push"),
            ("GITHUB_REF_TYPE", "branch"),
            ("GITHUB_REF_PROTECTED", "true"),
        ]);
        assert_eq!(forced_remote_readonly_with(lookup(&vars)), None);
    }

    #[test]
    fn github_pull_request_is_readonly() {
        let vars = github(&[
            ("GITHUB_EVENT_NAME", "pull_request"),
            ("GITHUB_REF_TYPE", "branch"),
            ("GITHUB_REF_PROTECTED", "false"),
        ]);
        let forced = forced_remote_readonly_with(lookup(&vars)).expect("PR is untrusted");
        assert!(forced.reason.contains("pull_request"), "{}", forced.reason);
    }

    #[test]
    fn github_unprotected_push_is_readonly() {
        let vars = github(&[
            ("GITHUB_EVENT_NAME", "push"),
            ("GITHUB_REF_TYPE", "branch"),
            ("GITHUB_REF_PROTECTED", "false"),
        ]);
        assert!(forced_remote_readonly_with(lookup(&vars)).is_some());
    }

    #[test]
    fn github_tag_push_is_readonly() {
        let vars = github(&[
            ("GITHUB_EVENT_NAME", "push"),
            ("GITHUB_REF_TYPE", "tag"),
            ("GITHUB_REF_PROTECTED", "true"),
        ]);
        let forced = forced_remote_readonly_with(lookup(&vars)).expect("tags are untrusted");
        assert!(forced.reason.contains("tag"), "{}", forced.reason);
    }

    #[test]
    fn github_workflow_dispatch_is_readonly() {
        let vars = github(&[
            ("GITHUB_EVENT_NAME", "workflow_dispatch"),
            ("GITHUB_REF_TYPE", "branch"),
            ("GITHUB_REF_PROTECTED", "true"),
        ]);
        assert!(forced_remote_readonly_with(lookup(&vars)).is_some());
    }

    #[test]
    fn gitlab_protected_push_may_write() {
        let vars = gitlab(&[
            ("CI_PIPELINE_SOURCE", "push"),
            ("CI_COMMIT_REF_PROTECTED", "true"),
        ]);
        assert_eq!(forced_remote_readonly_with(lookup(&vars)), None);
    }

    #[test]
    fn gitlab_merge_request_is_readonly() {
        let vars = gitlab(&[
            ("CI_PIPELINE_SOURCE", "merge_request_event"),
            ("CI_MERGE_REQUEST_IID", "42"),
            ("CI_COMMIT_REF_PROTECTED", "false"),
        ]);
        let forced = forced_remote_readonly_with(lookup(&vars)).expect("MR is untrusted");
        assert!(
            forced.reason.contains("merge_request_event"),
            "{}",
            forced.reason
        );
    }

    #[test]
    fn gitlab_tag_pipeline_is_readonly() {
        let vars = gitlab(&[
            ("CI_PIPELINE_SOURCE", "push"),
            ("CI_COMMIT_TAG", "v1.0.0"),
            ("CI_COMMIT_REF_PROTECTED", "true"),
        ]);
        assert!(forced_remote_readonly_with(lookup(&vars)).is_some());
    }

    #[test]
    fn gitlab_unprotected_push_is_readonly() {
        let vars = gitlab(&[
            ("CI_PIPELINE_SOURCE", "push"),
            ("CI_COMMIT_REF_PROTECTED", "false"),
        ]);
        assert!(forced_remote_readonly_with(lookup(&vars)).is_some());
    }

    #[test]
    fn github_actions_truthy_spellings() {
        for value in ["1", "TRUE", "Yes"] {
            let vars = env(&[
                ("GITHUB_ACTIONS", value),
                ("GITHUB_EVENT_NAME", "pull_request"),
            ]);
            assert!(
                forced_remote_readonly_with(lookup(&vars)).is_some(),
                "GITHUB_ACTIONS={value} should be detected"
            );
        }
    }

    #[test]
    fn display_env_treats_empty_as_unknown() {
        assert_eq!(display_env(None), "unknown");
        assert_eq!(display_env(Some(String::new())), "unknown");
        assert_eq!(display_env(Some("push".into())), "push");
    }

    #[test]
    fn github_empty_event_name_reason_says_unknown() {
        let vars = github(&[("GITHUB_EVENT_NAME", "")]);
        let forced = forced_remote_readonly_with(lookup(&vars)).expect("empty event is untrusted");
        assert!(
            forced.reason.contains("Actions unknown"),
            "empty GITHUB_EVENT_NAME must render as unknown, got {}",
            forced.reason
        );
    }

    #[test]
    fn gitlab_empty_pipeline_source_reason_says_unknown() {
        let vars = gitlab(&[("CI_PIPELINE_SOURCE", "")]);
        let forced = forced_remote_readonly_with(lookup(&vars)).expect("empty source is untrusted");
        assert!(
            forced.reason.contains("CI unknown"),
            "empty CI_PIPELINE_SOURCE must render as unknown, got {}",
            forced.reason
        );
    }
}
