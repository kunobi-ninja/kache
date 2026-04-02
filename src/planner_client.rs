use anyhow::{Context, Result};
use std::time::Duration;

#[cfg(test)]
use std::borrow::Cow;

use crate::config::PlannerConfig;
use kache_core::{BuildIntent, PrefetchPlan};

const PREFETCH_PLAN_PATH_V1: &str = "/v1/prefetch-plan";
const PREFETCH_PLAN_PATH_V2: &str = "/v2/prefetch-plan";

pub async fn resolve_prefetch_plan(req: &BuildIntent) -> Result<Option<PrefetchPlan>> {
    let Some(config) = crate::config::Config::load_planner_config() else {
        return Ok(None);
    };

    let plan = resolve_prefetch_plan_with_config(&config, req).await?;
    Ok(Some(plan))
}

pub async fn resolve_prefetch_plan_with_config(
    config: &PlannerConfig,
    req: &BuildIntent,
) -> Result<PrefetchPlan> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(config.timeout_ms))
        .build()
        .context("building planner client")?;

    let mut request = client.post(prefetch_plan_url(&config.endpoint)).json(req);
    if let Some(token) = config.token.as_deref() {
        request = request.bearer_auth(token);
    }

    let response = request
        .send()
        .await
        .context("requesting advisory prefetch plan")?
        .error_for_status()
        .context("planner returned an error status")?;

    response
        .json::<PrefetchPlan>()
        .await
        .context("decoding planner prefetch plan")
}

fn prefetch_plan_url(endpoint: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if trimmed.ends_with(PREFETCH_PLAN_PATH_V1) || trimmed.ends_with(PREFETCH_PLAN_PATH_V2) {
        trimmed.to_string()
    } else {
        format!("{trimmed}{PREFETCH_PLAN_PATH_V2}")
    }
}

#[cfg(test)]
fn expected_prefetch_plan_path(endpoint: &str) -> Cow<'static, str> {
    if endpoint
        .trim_end_matches('/')
        .ends_with(PREFETCH_PLAN_PATH_V1)
    {
        Cow::Borrowed(PREFETCH_PLAN_PATH_V1)
    } else {
        Cow::Borrowed(PREFETCH_PLAN_PATH_V2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_core::{PrefetchCandidate, PrefetchDisposition};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_response_server(
        body: String,
        expected_auth: Option<&str>,
        status_line: &str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_auth = expected_auth.map(str::to_string);
        let status = status_line.to_string();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]);
            assert!(request.starts_with(&format!(
                "POST {} HTTP/1.1",
                expected_prefetch_plan_path(&format!("http://{addr}"))
            )));

            match expected_auth {
                Some(token) => assert!(
                    request.contains(&format!("authorization: Bearer {token}"))
                        || request.contains(&format!("Authorization: Bearer {token}"))
                ),
                None => assert!(
                    !request.contains("authorization: Bearer")
                        && !request.contains("Authorization: Bearer")
                ),
            }

            let response = format!(
                "{status}\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn test_prefetch_plan_url_appends_path_once() {
        assert_eq!(
            prefetch_plan_url("https://planner.example.com"),
            "https://planner.example.com/v2/prefetch-plan"
        );
        assert_eq!(
            prefetch_plan_url("https://planner.example.com/v2/prefetch-plan"),
            "https://planner.example.com/v2/prefetch-plan"
        );
        assert_eq!(
            prefetch_plan_url("https://planner.example.com/v1/prefetch-plan"),
            "https://planner.example.com/v1/prefetch-plan"
        );
    }

    #[tokio::test]
    async fn test_resolve_prefetch_plan_with_config() {
        let body = serde_json::to_string(&PrefetchPlan {
            plan_id: Some("plan-1".into()),
            planner: Some("test".into()),
            disposition: PrefetchDisposition::Execute,
            candidates: vec![PrefetchCandidate {
                cache_key: "abc".into(),
                crate_name: "serde".into(),
            }],
        })
        .unwrap();
        let endpoint = spawn_response_server(body, Some("token-123"), "HTTP/1.1 200 OK").await;
        let config = PlannerConfig {
            endpoint,
            timeout_ms: 1000,
            token: Some("token-123".into()),
        };
        let req = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("ns".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = resolve_prefetch_plan_with_config(&config, &req)
            .await
            .unwrap();
        assert_eq!(plan.plan_id.as_deref(), Some("plan-1"));
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "abc");
        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
    }

    #[tokio::test]
    async fn test_resolve_prefetch_plan_with_config_errors_on_bad_status() {
        let endpoint = spawn_response_server(
            "{\"error\":\"nope\"}".to_string(),
            None,
            "HTTP/1.1 503 Service Unavailable",
        )
        .await;
        let config = PlannerConfig {
            endpoint,
            timeout_ms: 1000,
            token: None,
        };
        let req = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: None,
            cargo_lock_deps: vec![],
        };

        let err = resolve_prefetch_plan_with_config(&config, &req)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("error status"));
    }

    #[tokio::test]
    async fn test_resolve_prefetch_plan_with_do_nothing_disposition() {
        let body = serde_json::to_string(&PrefetchPlan {
            plan_id: Some("plan-2".into()),
            planner: Some("test".into()),
            disposition: PrefetchDisposition::DoNothing,
            candidates: vec![],
        })
        .unwrap();
        let endpoint = spawn_response_server(body, None, "HTTP/1.1 200 OK").await;
        let config = PlannerConfig {
            endpoint,
            timeout_ms: 1000,
            token: None,
        };
        let req = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: None,
            cargo_lock_deps: vec![],
        };

        let plan = resolve_prefetch_plan_with_config(&config, &req)
            .await
            .unwrap();
        assert_eq!(plan.disposition, PrefetchDisposition::DoNothing);
        assert!(plan.candidates.is_empty());
    }
}
