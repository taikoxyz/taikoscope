use eyre::Result;
use reqwest::{Client as HttpClient, StatusCode, Url};
use serde::Deserialize;
use tracing::{debug, error};

use crate::monitor::{NewIncident, ResolveIncident};

#[derive(Deserialize)]
struct IncidentComponent {
    id: String,
    status: String,
    name: String,
}

#[derive(Deserialize)]
struct IncidentSummary {
    id: String,
    components: Vec<IncidentComponent>,
}

/// Client for interacting with the Instatus API.
#[derive(Debug, Clone)]
pub struct Client {
    http: HttpClient,
    base_url: Url,
    api_key: String,
    page_id: String,
}

impl Client {
    /// Create a new Instatus API client.
    pub fn new(api_key: String, page_id: String) -> Self {
        Self {
            http: HttpClient::new(),
            base_url: Url::parse("https://api.instatus.com").expect("valid base URL"),
            api_key,
            page_id,
        }
    }

    /// Create a client targeting a custom base URL (e.g. for tests).
    #[cfg(test)]
    pub fn with_base_url(api_key: String, page_id: String, base_url: Url) -> Self {
        Self { http: HttpClient::new(), api_key, page_id, base_url }
    }

    /// Authenticate the request.
    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        rb.bearer_auth(&self.api_key)
    }

    /// Build the incidents URL with open status filters.
    pub(crate) fn incidents_url_with_statuses(&self) -> Result<Url> {
        let mut url = self.base_url.join(&format!("v1/{}/incidents", self.page_id))?;
        let statuses = ["INVESTIGATING", "IDENTIFIED", "MONITORING"];
        let joined = statuses.join(",");
        url.query_pairs_mut().append_pair("status", &joined);
        Ok(url)
    }

    /// Create a new incident on Instatus.
    pub async fn create_incident(&self, body: &NewIncident) -> Result<String> {
        #[derive(Deserialize)]
        struct Resp {
            id: String,
        }
        let url = self.base_url.join(&format!("v1/{}/incidents", self.page_id)).unwrap();
        tracing::info!(
            page_id = %self.page_id,
            url = %url,
            incident_name = %body.name,
            "Creating incident"
        );
        let response = self.auth(self.http.post(url.clone())).json(body).send().await?;
        let resp = response.error_for_status()?;
        Ok(resp.json::<Resp>().await?.id)
    }

    /// Resolve an existing incident on Instatus.
    pub async fn resolve_incident(&self, id: &str, body: &ResolveIncident) -> Result<()> {
        let url = self.base_url.join(&format!("v1/{}/incidents/{}", self.page_id, id)).unwrap();
        tracing::info!(
            page_id = %self.page_id,
            incident_id = %id,
            url = %url,
            "Resolving incident"
        );
        let response = self.auth(self.http.put(url.clone())).json(body).send().await?;

        let status = response.status();

        if !status.is_success() {
            let response_text = match response.text().await {
                Ok(text) => text,
                Err(_) => "Could not read response body".into(),
            };

            // Check if this is a "no status page" error which is non-retryable
            if response_text.contains("No status page for that incident") {
                error!(
                    incident_id = %id,
                    page_id = %self.page_id,
                    status = %status,
                    url = %url,
                    body = %response_text,
                    "Incident belongs to different page - this is a configuration error"
                );
                return Err(eyre::eyre!("PAGE_MISMATCH: {}", response_text));
            }

            error!(status = %status, url = %url, body = %response_text, "Failed to resolve incident");
            return Err(eyre::eyre!("HTTP error {}: {}", status, response_text));
        }

        debug!(incident_id = %id, "Successfully resolved incident");
        Ok(())
    }

    /// Return open incident ID for `component_id`, if any.
    pub async fn open_incident(&self, component_id: &str) -> Result<Option<String>> {
        // Query any incidents that aren't RESOLVED (to catch MONITORING or IDENTIFIED too)
        let url = self.incidents_url_with_statuses()?;

        tracing::debug!(url = %url, "Querying incidents");

        let response = self.auth(self.http.get(url.clone())).send().await?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            tracing::error!(status = %status, body = %body, "Failed to get incidents");
            return Err(eyre::eyre!("HTTP error {}: {}", status, body));
        }

        let list = response.json::<Vec<IncidentSummary>>().await?;
        tracing::debug!(count = list.len(), "Found incidents in total");

        // find the first incident touching our component
        if let Some((incident_id, comp)) = list.into_iter().find_map(|inc| {
            inc.components.into_iter().find(|c| c.id == component_id).map(|comp| (inc.id, comp))
        }) {
            tracing::info!(
                incident_id = %incident_id,
                component_name = %comp.name,
                component_status = %comp.status,
                "Found open incident for component"
            );
            Ok(Some(incident_id))
        } else {
            tracing::debug!(component_id = %component_id, "No open incidents found for component");
            Ok(None)
        }
    }

    /// Check if an incident exists on the current page
    pub async fn incident_exists(&self, incident_id: &str) -> Result<bool> {
        let url =
            self.base_url.join(&format!("v1/{}/incidents/{}", self.page_id, incident_id)).unwrap();
        let response = self.auth(self.http.get(url.clone())).send().await?;

        let status = response.status();

        match status {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => {
                let body = response.text().await.unwrap_or_default();
                tracing::warn!(
                    incident_id = %incident_id,
                    page_id = %self.page_id,
                    status = %status,
                    body = %body,
                    "Unexpected response when checking incident existence"
                );
                Err(eyre::eyre!(
                    "HTTP error {} while checking incident existence for {}: {}",
                    status,
                    incident_id,
                    body
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::monitor::{ComponentStatus, IncidentState, NewIncident, ResolveIncident};

    use mockito::{Matcher, Server};
    use serde_json::json;
    use tokio;

    #[test]
    fn test_new_incident_serialization() {
        let payload = NewIncident {
            name: "No L2 head events - Possible Outage".to_owned(),
            message: "No L2 head event for 30s".to_owned(),
            status: IncidentState::Investigating,
            components: vec!["comp1".to_owned()],
            statuses: vec![ComponentStatus::major_outage("comp1")],
            notify: true,
            started: Some("2025-05-12T07:48:00Z".to_owned()),
        };
        let expected = json!({
            "name": "No L2 head events - Possible Outage",
            "message": "No L2 head event for 30s",
            "status": "INVESTIGATING",
            "components": ["comp1"],
            "statuses": [{"id": "comp1", "status": "MAJOROUTAGE"}],
            "notify": true,
            "started": "2025-05-12T07:48:00Z"
        });
        let actual = serde_json::to_value(&payload).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_resolve_incident_serialization() {
        let payload = ResolveIncident {
            status: IncidentState::Resolved,
            components: vec!["comp1".to_owned()],
            statuses: vec![ComponentStatus::operational("comp1")],
            notify: true,
            started: Some("2025-05-12T07:48:00Z".to_owned()),
        };
        let expected = json!({
            "status": "RESOLVED",
            "components": ["comp1"],
            "statuses": [{"id": "comp1", "status": "OPERATIONAL"}],
            "notify": true,
            "started": "2025-05-12T07:48:00Z"
        });
        let actual = serde_json::to_value(&payload).unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn create_incident_hits_correct_endpoint() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/page1/incidents")
            .match_header("authorization", "Bearer testkey")
            .match_header("content-type", "application/json")
            .with_status(200)
            .with_body(r#"{"id":"incident123"}"#)
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let payload = NewIncident {
            name: "Test incident".into(),
            message: "Testing".into(),
            status: IncidentState::Investigating,
            components: vec!["comp1".into()],
            statuses: vec![ComponentStatus::major_outage("comp1")],
            notify: true,
            started: Some("2025-05-12T00:00:00Z".into()),
        };
        let id = client.create_incident(&payload).await.unwrap();
        assert_eq!(id, "incident123");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn resolve_incident_hits_update_endpoint() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("PUT", "/v1/page1/incidents/incident123")
            .match_header("authorization", "Bearer testkey")
            .match_header("content-type", "application/json")
            .with_status(200)
            .with_body("{}")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let payload = ResolveIncident {
            status: IncidentState::Resolved,
            components: vec!["comp1".into()],
            statuses: vec![ComponentStatus::operational("comp1")],
            notify: true,
            started: Some("2025-05-12T07:48:00Z".to_owned()),
        };
        client.resolve_incident("incident123", &payload).await.unwrap();
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn find_open_incident_for_component_filters_correctly() {
        let mut server = Server::new_async().await;
        let body = json!([
            { "id": "inc1", "components": [ { "id": "compX", "name": "X", "status": "OPERATIONAL" } ] },
            { "id": "inc2", "components": [ { "id": "comp1", "name": "Target", "status": "MAJOROUTAGE" } ] }
        ])
        .to_string();

        let mock = server
            .mock("GET", "/v1/page1/incidents")
            // Use a regex to match any query - mockito has issues with array params
            .match_query(Matcher::Any)
            .with_status(200)
            .with_body(body)
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let id = client.open_incident("comp1").await.unwrap();
        assert_eq!(id, Some("inc2".into()));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn open_incident_returns_none_when_no_open_incidents() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/v1/page1/incidents")
            .match_query(Matcher::Any)
            .with_status(200)
            .with_body("[]")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let id = client.open_incident("comp1").await.unwrap();
        assert_eq!(id, None);
        mock.assert_async().await;
    }

    #[test]
    fn incidents_url_encodes_multiple_statuses() {
        let client = Client::with_base_url(
            "key".into(),
            "page1".into(),
            Url::parse("https://example.com/").unwrap(),
        );
        let url = client.incidents_url_with_statuses().unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.com/v1/page1/incidents?status=INVESTIGATING%2CIDENTIFIED%2CMONITORING"
        );
    }

    #[tokio::test]
    async fn create_incident_returns_err_on_http_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("POST", "/v1/page1/incidents")
            .with_status(400)
            .with_body("bad request")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let payload = NewIncident {
            name: "Test".into(),
            message: "Testing".into(),
            status: IncidentState::Investigating,
            components: Vec::new(),
            statuses: Vec::new(),
            notify: false,
            started: None,
        };
        let err = client.create_incident(&payload).await.unwrap_err();
        assert!(err.to_string().contains("400"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn resolve_incident_returns_err_on_http_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("PUT", "/v1/page1/incidents/incident123")
            .with_status(500)
            .with_body("server error")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let payload = ResolveIncident {
            status: IncidentState::Resolved,
            components: vec!["comp1".into()],
            statuses: vec![ComponentStatus::operational("comp1")],
            notify: true,
            started: None,
        };
        let err = client.resolve_incident("incident123", &payload).await.unwrap_err();
        assert!(err.to_string().contains("500"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn open_incident_returns_err_on_http_error() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/v1/page1/incidents")
            .match_query(Matcher::Any)
            .with_status(500)
            .with_body("server error")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let err = client.open_incident("comp1").await.unwrap_err();
        assert!(err.to_string().contains("500"));
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn incident_exists_returns_false_on_not_found() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/v1/page1/incidents/incident123")
            .match_header("authorization", "Bearer testkey")
            .with_status(404)
            .with_body("not found")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let exists = client.incident_exists("incident123").await.unwrap();
        assert!(!exists);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn incident_exists_returns_err_on_unexpected_status() {
        let mut server = Server::new_async().await;

        let mock = server
            .mock("GET", "/v1/page1/incidents/incident123")
            .match_header("authorization", "Bearer testkey")
            .with_status(500)
            .with_body("server error")
            .create_async()
            .await;

        let client =
            Client::with_base_url("testkey".into(), "page1".into(), server.url().parse().unwrap());
        let err = client.incident_exists("incident123").await.unwrap_err();
        assert!(err.to_string().contains("500"));
        mock.assert_async().await;
    }
}
