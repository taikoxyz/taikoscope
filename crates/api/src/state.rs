//! Shared state for API handlers and constants

use clickhouse_lib::ClickhouseReader;
// use network::http_retry; // no longer used for price fetch retries

use std::{
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use tokio::sync::RwLock;

use reqwest::Client;
use serde_json::Value;

/// Default maximum number of requests allowed during the rate limiting period.
pub const DEFAULT_MAX_REQUESTS: u64 = u64::MAX;
/// Default duration for the rate limiting window.
pub const DEFAULT_RATE_PERIOD: StdDuration = StdDuration::from_secs(1);
/// Maximum number of records returned by the `/block-transactions` endpoint.
pub const MAX_BLOCK_TRANSACTIONS_LIMIT: u64 = 50000;
/// Maximum number of records returned by table endpoints.
pub const MAX_TABLE_LIMIT: u64 = 50000;

/// Shared state for API handlers.
#[derive(Clone)]
pub struct ApiState {
    pub(crate) client: ClickhouseReader,
    pub(crate) http_client: Client,
    max_requests: u64,
    rate_period: StdDuration,
    price_cache: Arc<RwLock<CachedPrice>>,
}

#[derive(Debug)]
struct CachedPrice {
    value: f64,
    updated_at: Instant,
    backoff_until: Option<Instant>,
}

impl std::fmt::Debug for ApiState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiState").finish_non_exhaustive()
    }
}

impl ApiState {
    /// Create a new [`ApiState`].
    pub fn new(client: ClickhouseReader, max_requests: u64, rate_period: StdDuration) -> Self {
        // Default TTL: 5 minutes unless overridden via env
        let ttl = eth_price_ttl();
        Self {
            client,
            http_client: Client::new(),
            max_requests,
            rate_period,
            price_cache: Arc::new(RwLock::new(CachedPrice {
                value: 0.0,
                // Force initial fetch by setting updated_at before TTL
                updated_at: Instant::now() - ttl - StdDuration::from_secs(1),
                backoff_until: None,
            })),
        }
    }

    /// Maximum number of requests allowed per [`rate_period`].
    pub const fn max_requests(&self) -> u64 {
        self.max_requests
    }

    /// Time window for rate limiting.
    pub const fn rate_period(&self) -> StdDuration {
        self.rate_period
    }

    /// Get the current ETH price in USD, with caching and rate-limit aware backoff.
    ///
    /// Behavior:
    /// - Caches successful results for `ETH_PRICE_TTL_SECS` (default 300s).
    /// - On errors (including HTTP 429), serves the last cached value if available and sets a
    ///   backoff window (respecting Retry-After when present).
    /// - Only returns an error if there is no previously cached value.
    pub async fn eth_price(&self) -> eyre::Result<f64> {
        let now = Instant::now();
        let ttl = eth_price_ttl();

        // Fast path: within TTL or within backoff
        {
            let cache = self.price_cache.read().await;
            if let Some(until) = cache.backoff_until &&
                now < until &&
                cache.value > 0.0
            {
                return Ok(cache.value);
            }
            if now.duration_since(cache.updated_at) < ttl {
                return Ok(cache.value);
            }
        }

        // Attempt a single fetch; handle rate limits and serve stale on failure
        match try_fetch_eth_price_once(&self.http_client).await {
            FetchOutcome::Success(price) => {
                let mut cache = self.price_cache.write().await;
                *cache = CachedPrice { value: price, updated_at: now, backoff_until: None };
                Ok(price)
            }
            FetchOutcome::RateLimited(retry_after) => {
                let mut cache = self.price_cache.write().await;
                let backoff = retry_after.unwrap_or_else(|| StdDuration::from_secs(60));
                cache.backoff_until = Some(now + backoff);
                if cache.value > 0.0 {
                    tracing::warn!(
                        backoff_secs = backoff.as_secs_f64(),
                        "ETH price rate limited; serving stale value"
                    );
                    Ok(cache.value)
                } else {
                    Err(eyre::eyre!("ETH price fetch rate limited and no cached value"))
                }
            }
            FetchOutcome::OtherError(e) => {
                let mut cache = self.price_cache.write().await;
                // Short backoff on generic errors to avoid hammering provider
                let backoff = StdDuration::from_secs(30);
                cache.backoff_until = Some(now + backoff);
                if cache.value > 0.0 {
                    tracing::warn!(error = %e, "ETH price fetch failed; serving stale value");
                    Ok(cache.value)
                } else {
                    Err(e)
                }
            }
        }
    }
}

/// One-shot ETH price fetch with explicit rate-limit detection.
///
/// Tries `CoinGecko` first (historical primary source), then falls back to `Coinbase`
/// if `CoinGecko` returns an error. `CoinGecko`'s free tier blocks most datacenter
/// egress IPs (observed 2026-04-14 from GKE), so `Coinbase` acts as the durable
/// path when the API is deployed from a cloud provider.
async fn try_fetch_eth_price_once(client: &Client) -> FetchOutcome {
    match try_coingecko(client).await {
        FetchOutcome::Success(p) => FetchOutcome::Success(p),
        // Propagate rate limits from the primary source so the caller can respect Retry-After.
        FetchOutcome::RateLimited(retry_after) => match try_coinbase(client).await {
            FetchOutcome::Success(p) => FetchOutcome::Success(p),
            _ => FetchOutcome::RateLimited(retry_after),
        },
        FetchOutcome::OtherError(primary_err) => {
            tracing::warn!(error = %primary_err, "CoinGecko ETH price fetch failed; trying Coinbase fallback");
            match try_coinbase(client).await {
                FetchOutcome::Success(p) => FetchOutcome::Success(p),
                FetchOutcome::RateLimited(retry_after) => FetchOutcome::RateLimited(retry_after),
                FetchOutcome::OtherError(fallback_err) => FetchOutcome::OtherError(eyre::eyre!(
                    "both ETH price sources failed: coingecko={primary_err}; coinbase={fallback_err}"
                )),
            }
        }
    }
}

/// Fetch ETH/USD price from `CoinGecko`. Response shape: `{"ethereum":{"usd":<f64>}}`.
async fn try_coingecko(client: &Client) -> FetchOutcome {
    let url = std::env::var("ETH_PRICE_URL").unwrap_or_else(|_| {
        "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd".to_owned()
    });

    // Optional: use CoinGecko API key if configured
    let api_key = std::env::var("COINGECKO_API_KEY").ok();

    let req = client.get(&url);
    let req =
        if let Some(key) = api_key.as_deref() { req.header("x-cg-pro-api-key", key) } else { req };

    match req.send().await {
        Ok(resp) => {
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = parse_retry_after(resp.headers());
                return FetchOutcome::RateLimited(retry_after);
            }
            if let Err(e) = resp.error_for_status_ref() {
                return FetchOutcome::OtherError(eyre::Report::from(e));
            }
            match resp.json::<Value>().await {
                Ok(json) => {
                    let price =
                        json.get("ethereum").and_then(|e| e.get("usd")).and_then(|v| v.as_f64());
                    match price {
                        Some(p) => FetchOutcome::Success(p),
                        None => FetchOutcome::OtherError(eyre::eyre!("invalid coingecko response")),
                    }
                }
                Err(e) => FetchOutcome::OtherError(eyre::Report::from(e)),
            }
        }
        Err(e) => FetchOutcome::OtherError(eyre::Report::from(e)),
    }
}

/// Fetch ETH/USD price from `Coinbase`. Response shape:
/// `{"data":{"base":"ETH","currency":"USD","amount":"<decimal string>"}}`.
async fn try_coinbase(client: &Client) -> FetchOutcome {
    let url = std::env::var("ETH_PRICE_COINBASE_URL")
        .unwrap_or_else(|_| "https://api.coinbase.com/v2/prices/ETH-USD/spot".to_owned());

    match client.get(&url).send().await {
        Ok(resp) => {
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = parse_retry_after(resp.headers());
                return FetchOutcome::RateLimited(retry_after);
            }
            if let Err(e) = resp.error_for_status_ref() {
                return FetchOutcome::OtherError(eyre::Report::from(e));
            }
            match resp.json::<Value>().await {
                Ok(json) => {
                    let amount = json
                        .get("data")
                        .and_then(|d| d.get("amount"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<f64>().ok());
                    match amount {
                        Some(p) if p > 0.0 => FetchOutcome::Success(p),
                        _ => FetchOutcome::OtherError(eyre::eyre!("invalid coinbase response")),
                    }
                }
                Err(e) => FetchOutcome::OtherError(eyre::Report::from(e)),
            }
        }
        Err(e) => FetchOutcome::OtherError(eyre::Report::from(e)),
    }
}

/// Output of a single ETH price fetch attempt.
enum FetchOutcome {
    Success(f64),
    RateLimited(Option<StdDuration>),
    OtherError(eyre::Report),
}

fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<StdDuration> {
    use reqwest::header::RETRY_AFTER;
    if let Some(v) = headers.get(RETRY_AFTER) &&
        let Ok(s) = v.to_str()
    {
        // Retry-After can be seconds or an HTTP date; support seconds variant
        if let Ok(secs) = s.trim().parse::<u64>() {
            return Some(StdDuration::from_secs(secs.saturating_add(1)));
        }
    }
    None
}

fn eth_price_ttl() -> StdDuration {
    std::env::var("ETH_PRICE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(StdDuration::from_secs)
        .unwrap_or_else(|| StdDuration::from_secs(300))
}
