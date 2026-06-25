# Dependance Aware Pipeline
* **Status:** Accepted
* **Date:** 2026-06-18
* **Author:** Arthur Ndubi


## Context

The stock-dividend_monitor pipeline depend on three files (fetch_stock_file, staging file, and classification file) for the business logic. 

Initially, I thought having these files behave independently would produce fresh data. Independently, each file checks against its own last modified time and fetches fresh data. This produced silent bugs, technical debts, and operational risks:

1. Discovered that I **used the wrong getatime** which only returns the last access time or when the files were not read. This brought stale data in all three files. The next two days I didn't get new data from the Nasdaq Exchange. In short, there was no update.

2. Discovered that even after fixing the first problem, the classification file failed to show or reflect the changes from staging files. In short, **two files managed to update the data, but one remained in that state because it checks its own file age.**

## Decision

To fix these two deeper architectural problems, I decided to use getmtime and make the classification file check staging files. So here's what I did:

- **Fix 1:** Used getmtime method that returns the last modified time or the time when the file was written. Getmtime provided the freshness signal I wanted. So the problem for the fetch stock file and the staging file was solved. However, with the classification file, the problem was still there.

- **Fix 2:** Since I am dealing with investment data that requires accurate and immediate decision dependencies, take priority. A stale classification file means:

- XOM shows as no_dividend_player
- Threshold calculations use the wrong formulae
- Signals say HOLD when it should SELL.
- I miss profit opportunity

This is not a technical problem. This is a financial decision made on the wrong data. Data freshness is not optional. It's a necessity in investment decision.

## Trade Off
Key trade-offs to understand are:

- **Full independences-** Each file manages itself, simpler to build but presents risks of stale data.
- **Full dependances-** Files know about each other; data is fresh, but it's slower and more complex.

The middle ground is extracting files, which pulls data from the API and should be independent-aware. Transform files that compute or process data should be dependency aware.

## Lesson
As a result of this, I came to learn about **dependence-aware pipelines.** Each file knows what it depends on and checks if its dependence is fresher than itself . All files update automatically with no silent bugs or stale data.


