use crate::{client::Client as IncidentClient, helpers};
use chrono::Utc;
use network::public_rpc_monitor::check_syncing;
use reqwest::{Client, Url};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Spawn a background task monitoring the provided public RPC endpoint.
/// If an `IncidentClient` is provided, incidents will be created and resolved
/// when the endpoint is unhealthy or recovers.
pub fn spawn_public_rpc_monitor(
    url: Url,
    incident: Option<(IncidentClient, String)>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let client = Client::new();
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        let mut incident_id: Option<String> = None;
        loop {
            interval.tick().await;
            if let Some((ic, cid)) = &incident {
                check_once(&client, &url, Some((ic, cid)), &mut incident_id).await;
            } else {
                check_once(&client, &url, None, &mut incident_id).await;
            }
        }
    })
}

async fn check_once(
    client: &Client,
    url: &Url,
    incident: Option<(&IncidentClient, &String)>,
    incident_id: &mut Option<String>,
) {
    check_once_with_retry_delay(client, url, incident, incident_id, Duration::from_secs(15)).await;
}

async fn check_once_with_retry_delay(
    client: &Client,
    url: &Url,
    incident: Option<(&IncidentClient, &String)>,
    incident_id: &mut Option<String>,
    retry_delay: Duration,
) {
    let first = check_syncing(client, url).await;
    let negative = match first {
        Ok(false) => {
            info!(url = url.as_str(), "public rpc healthy");
            if let Some((ic, cid)) = incident {
                resolve_if_needed(ic, cid, incident_id).await;
            }
            false
        }
        Ok(true) => {
            warn!(url = url.as_str(), "public rpc syncing");
            true
        }
        Err(e) => {
            // Include error chain with debug formatting
            warn!(error = ?e, url = url.as_str(), "public rpc check failed");
            true
        }
    };

    if negative {
        tokio::time::sleep(retry_delay).await;
        match check_syncing(client, url).await {
            Ok(false) => {
                info!(url = url.as_str(), "public rpc recovered");
                if let Some((ic, cid)) = incident {
                    resolve_if_needed(ic, cid, incident_id).await;
                }
            }
            Ok(true) => {
                error!(url = url.as_str(), "public rpc still syncing");
                if let Some((ic, cid)) = incident {
                    open_if_needed(ic, cid, incident_id).await;
                }
            }
            Err(e) => {
                error!(error = ?e, url = url.as_str(), "public rpc check failed again");
                if let Some((ic, cid)) = incident {
                    open_if_needed(ic, cid, incident_id).await;
                }
            }
        }
    }
}

async fn open_if_needed(
    client: &IncidentClient,
    component_id: &str,
    incident_id: &mut Option<String>,
) {
    if incident_id.is_some() {
        return;
    }
    match client.open_incident(component_id).await {
        Ok(Some(id)) => {
            info!(incident_id = %id, "existing incident found, skipping creation");
            *incident_id = Some(id);
        }
        Ok(None) => {
            let body = helpers::build_incident_payload(
                component_id,
                "Public RPC Unavailable".to_owned(),
                "Public RPC endpoint is unreachable or syncing".to_owned(),
                Utc::now(),
            );
            match helpers::create_with_retry(client, true, &body).await {
                Ok(id) => {
                    info!(incident_id = %id, "created public rpc incident");
                    *incident_id = Some(id);
                }
                Err(e) => error!(error = %e, "failed to create incident"),
            }
        }
        Err(e) => error!(error = %e, "failed to query incidents"),
    }
}

async fn resolve_if_needed(
    client: &IncidentClient,
    component_id: &str,
    incident_id: &mut Option<String>,
) {
    let Some(id) = incident_id.clone() else {
        return;
    };

    if resolve(client, component_id, &id).await.is_ok() {
        *incident_id = None;
    }
}

async fn resolve(client: &IncidentClient, component_id: &str, id: &str) -> eyre::Result<()> {
    let body = helpers::build_resolve_payload(component_id);
    helpers::resolve_with_retry(client, true, id, &body).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito::{Matcher, Server};

    #[tokio::test]
    async fn keeps_incident_id_when_resolution_fails() {
        let mut server = Server::new_async().await;
        let rpc_mock = server
            .mock("POST", "/")
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":false}"#)
            .create_async()
            .await;

        let put_mock = server
            .mock("PUT", "/v1/page1/incidents/inc1")
            .match_header("authorization", "Bearer testkey")
            .match_header("content-type", "application/json")
            .with_status(500)
            .with_body(r#"{"error":"temporary failure"}"#)
            .create_async()
            .await;

        let incident_client = IncidentClient::with_base_url(
            "testkey".into(),
            "page1".into(),
            server.url().parse().unwrap(),
        );
        let rpc_client = Client::new();
        let rpc_url: Url = server.url().parse().unwrap();
        let component_id = "comp1".to_owned();
        let mut incident_id = Some("inc1".to_owned());

        check_once(
            &rpc_client,
            &rpc_url,
            Some((&incident_client, &component_id)),
            &mut incident_id,
        )
        .await;

        assert_eq!(incident_id.as_deref(), Some("inc1"));
        put_mock.assert_async().await;
        rpc_mock.assert_async().await;
    }

    #[tokio::test]
    async fn autoresolves_incident_when_retry_recovers_rpc() {
        let mut server = Server::new_async().await;
        let first_outage_mock = server
            .mock("POST", "/")
            .expect(1)
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":true}"#)
            .create_async()
            .await;
        let second_outage_mock = server
            .mock("POST", "/")
            .expect(1)
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":true}"#)
            .create_async()
            .await;
        let recovery_probe_mock = server
            .mock("POST", "/")
            .expect(1)
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":true}"#)
            .create_async()
            .await;
        let recovered_retry_mock = server
            .mock("POST", "/")
            .expect(1)
            .with_status(200)
            .with_body(r#"{"jsonrpc":"2.0","id":1,"result":false}"#)
            .create_async()
            .await;

        let open_incident_mock = server
            .mock("GET", "/v1/page1/incidents")
            .match_header("authorization", "Bearer testkey")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;
        let create_incident_mock = server
            .mock("POST", "/v1/page1/incidents")
            .match_header("authorization", "Bearer testkey")
            .match_header("content-type", "application/json")
            .with_status(200)
            .with_body(r#"{"id":"inc1"}"#)
            .create_async()
            .await;
        let resolve_incident_mock = server
            .mock("PUT", "/v1/page1/incidents/inc1")
            .match_header("authorization", "Bearer testkey")
            .match_header("content-type", "application/json")
            .with_status(200)
            .with_body("{}")
            .create_async()
            .await;

        let incident_client = IncidentClient::with_base_url(
            "testkey".into(),
            "page1".into(),
            server.url().parse().unwrap(),
        );
        let rpc_client = Client::new();
        let rpc_url: Url = server.url().parse().unwrap();
        let component_id = "comp1".to_owned();
        let mut incident_id = None;

        check_once_with_retry_delay(
            &rpc_client,
            &rpc_url,
            Some((&incident_client, &component_id)),
            &mut incident_id,
            Duration::ZERO,
        )
        .await;
        assert_eq!(incident_id.as_deref(), Some("inc1"));

        check_once_with_retry_delay(
            &rpc_client,
            &rpc_url,
            Some((&incident_client, &component_id)),
            &mut incident_id,
            Duration::ZERO,
        )
        .await;
        assert!(incident_id.is_none());

        first_outage_mock.assert_async().await;
        second_outage_mock.assert_async().await;
        recovery_probe_mock.assert_async().await;
        recovered_retry_mock.assert_async().await;
        open_incident_mock.assert_async().await;
        create_incident_mock.assert_async().await;
        resolve_incident_mock.assert_async().await;
    }
}
