# 🏢 Corporate Identity Resolution Engine

A scalable and asynchronous pipeline for company data extraction and an **Identity Resolution (Fuzzy Matching)** API, built to process degraded datasets at high speed.

This project represents the solution for the **Veridion Engineering Challenge**, covering the entire data lifecycle: from extraction from the "wild web" (WAF Evasion, Historical Fallback), through transformation and storage, to high-speed querying via Elasticsearch.

---

## 📊 Executive Summary (Results)

The system was tested on an initial set of **997 domains** ("dirty" data - including expired sites, Cloudflare-protected sites, or deprecated subdomains).

| Metric                  | Result                                                  |
| :---------------------- | :------------------------------------------------------ |
| **Extraction Coverage** | **85.46%** (619 Live sites, 233 recovered from Archive) |
| **Extraction Time**     | **< 8 minutes** (imposed limit: 10 minutes)             |
| **Phone Fill Rate**     | **62.84%** (Live Environments)                          |
| **API Match Rate**      | **100.0%** on the official test set                     |
| **API Throughput**      | **~700 req/sec**                                        |

---

## 🏗️ Architecture & Technical Decisions (The "Why")

### 1. Data Extraction (The Scraping Engine)

A naive approach (simple requests or aiohttp) would have resulted in ~60% coverage due to the modern web ecosystem. To achieve **85%+**, I implemented 4 advanced techniques:

- **WAF Evasion (`curl_cffi`):** Standard libraries use TLS fingerprints (JA3/JA4) that are easily blocked by Akamai, Cloudflare, or DataDome. I replaced the HTTP engine with `curl_cffi`, which perfectly impersonates the cryptographic fingerprint of a Google Chrome browser (v110), passing undetected.
- **The "Necromancer" Fallback (Wayback Machine API):** Approximately 30% of domains returned DNS errors or 404s. I implemented an asynchronous fallback route to the Internet Archive CDX API. If a site is dead, the system instantly retrieves the last valid historical snapshot and extracts the contact data.
- **Targeted Deep Crawling (Sniper BFS):** To stay within the 10-minute limit, the system does not perform a full crawl. It applies a scoring heuristic to URLs and asynchronously accesses (max 3 levels) only critical pages: `/contact`, `/about`, `/privacy`, `/terms`.
- **Semantic Data Parsing:** In addition to robust Regex patterns, the extraction uses Google's internal NLP engine (`phonenumbers.PhoneNumberMatcher`) to validate numbers and parses invisible `application/ld+json` (Schema.org) tags to extract 100% structured data.

### 2. Data Ingestion & Storage

The chosen database is **Elasticsearch (v8.x)**, the industry standard for full-text search and N-gram matching.

- **Bulk API:** Data is not inserted sequentially; instead, it uses bulk packets (`helpers.bulk`) for maximum I/O efficiency.
- **Custom Tokenizers & Analyzers:** Company names are processed through a `custom_ngram_tokenizer` (min_gram:3, max_gram:10). This allows finding "Veridion" even if the input is "Verid" or contains typos. For phone numbers, I implemented a `pattern_replace` char filter that automatically "strips" spaces, parentheses, and hyphens, storing them in a pure numeric format for perfect matching.

### 3. Identity Resolution API (The Matching Engine)

The API is exposed via an asynchronous **FastAPI** server. To achieve a 100% Match Rate, the engine uses an Elasticsearch `bool` (Should match) query based on a **Weighted Boosting** system:

- **domain** (Wildcard) ➔ **Boost: 10.0** (Strongest identity signal)
- **phone** (Clean Match) ➔ **Boost: 5.0**
- **facebook_profile** (Wildcard) ➔ **Boost: 3.0**
- **company_name** (N-Gram Exact) ➔ **Boost: 2.0**
- **company_name** (Fuzziness="AUTO") ➔ **Boost: 1.5** (Handles typos / Levenshtein distance).

---

## 🎯 Bonus: Measuring Match Accuracy

Finding a result is only the first step; confirming it is the correct company is the real challenge. To measure and monitor Match Accuracy in a production environment, I would implement the following framework:

1. **Deterministic Distance Calculation (Post-Retrieval):**
   Once ES returns a candidate, we calculate a similarity matrix between the input payload and the found document:
   - **Jaccard Similarity** on domains and social links.
   - **Levenshtein Distance** between the input company name and the returned one. A difference of <15% indicates a True Positive.

2. **Ground Truth Testing (Golden Dataset):**
   Creating a manually verified test subset (e.g., 500 companies). The algorithm would run daily against this set to track 3 Data Science metrics:
   - **Precision:** How many of the matches made are actually correct?
   - **Recall:** How many companies in the system were we able to find?
   - **F1-Score:** The harmonic mean between precision and recall.

3. **Confidence Score Thresholding:**
   Elasticsearch returns a raw score (BM25). That score can be normalized (0 to 100). Any response with a normalized score below 40% is automatically flagged for _Needs Human Review_ (Active Learning).

---

## 💻 How to Run the Project

### Prerequisites

- Docker and Docker Compose installed.

### 1. Set Up Infrastructure

This command will download Elasticsearch v8, start the database container, and the FastAPI container with all necessary libraries installed.

```bash
docker-compose up -d --build
```

### 2. Step 1: Data Extraction (Scraping)

```bash
docker exec -it company_resolution_api python src/scraper.py
```

### 3. Step 2: Data Indexing in Elasticsearch

```bash
docker exec -it company_resolution_api python src/indexer.py
```

### 4. Step 3: API Evaluation (The Test)

```bash
docker exec -it company_resolution_api python src/evaluate_api.py
```
