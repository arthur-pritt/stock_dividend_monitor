# ADR 002: Circuit Breaker Metadata and Multi-Layer Validation for Daily Price Cache

## Status
* **Status:** Accepted
* **Date:** 2026-06-23
* **Author:** Arthur Ndubi

## Context
The data pipeline fetches daily closing stock prices using the `yfinance` API after market close and caches them locally to achieve low-latency reads and minimize external API quotas. 

However, the caching mechanism introduced a critical reliability bug: if the pipeline executes before the upstream provider publishes final market data, the API returns `NaN` values. The pipeline structural logic previously treated the mere presence of a file containing the target date as a valid cache hit. Consequently, unvalidated `NaN` datasets poisoned the local cache, locking the system into a permanent failed state and preventing subsequent scheduled runs from recovering the data.

This highlighted two architectural vulnerabilities:
1. **Temporal Dependency Lag:** Date presence does not guarantee data completeness or validity.
2. **Stateless Cache Invalidation:** The pipeline had no mechanism to differentiate between a genuinely successful run and a placeholder file generated during upstream data unavailability.

## Decision
We will decouple pipeline execution state from the raw data asset. We reject the pattern of using the data file's modified time or maximum date as a proxy for pipeline health. 

Instead, we will implement a dual-layer architectural pattern consisting of a **Multi-Layer Validation Gatekeeper** and a **State Machine Circuit Breaker**:

1. **State Tracking Isolation:** A lightweight metadata file (`.json`) will sit alongside the CSV data file to explicitly log execution history using UTC timestamps, tracking `last_fetch_status`, `nan_ticker_count`, and `successful_ticker_count`.
2. **Three-Layer Data Validation:** * *Layer 1 (Temporal):* Verifies if the target trading date exists within the file.
   * *Layer 2 (Completeness):* Scans data density to ensure missing rows do not exceed a strict threshold (<= 10% `NaN` allowance).
   * *Layer 3 (Validity):* Verifies type integrity and ensures values fall within realistic economic ranges.
3. **Circuit Breaker (Cool-Off Window):** If an API sync results in an invalid or partial dataset (`partial_nan` or `failed`), the state machine calculates a mandatory 4-hour `retry_after` cool-off period. The pipeline will short-circuit during subsequent loops until this window expires, preventing API rate-limiting abuse during upstream vendor outages.

## Consequences

### Positive
* **Data Idempotency:** The pipeline can now safely self-heal. Stale or corrupted files are detected on the next run and overwritten once valid upstream data becomes available.
* **API Protection:** The 4-hour circuit breaker stops automated cron jobs from hammering external API endpoints fruitlessly during prolonged upstream delays.
* **Deterministic Monitoring:** The JSON metadata file provides an instant, low-overhead operational dashboard for monitoring pipeline health without parsing large historical dataframes.

### Negative / Trade-offs
* **Metadata Overhead:** We now have to maintain two tightly coupled files (`.csv` and `.json`) per data asset, introducing the risk of split-brain states if one file is manually deleted or modified without the other.
* **Staleness Tolerance:** During an upstream outage, the system deliberately tolerates a 4-hour data block lag, sacrificing absolute immediacy to guarantee system stability and resource preservation.


