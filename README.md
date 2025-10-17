# Drift Funding Rate Ingestor

Fetches hourly **funding rates** from the [Drift](https://www.drift.trade/) perpetual futures API and stores them into a **MySQL 8.0** database with compression and idempotent upserts.  
Designed for continuous, ban-aware operation (handles rate limits, retries, and auto-backoff).

---

## 🚀 Features
- Automatically backfills missing hourly data for each market (`SOL-PERP`, `BTC-PERP`, `ETH-PERP`)
- Detects and recovers from temporary API bans (`HTTP 429` / `403`)
- Inserts or updates rows safely using MySQL’s `ON DUPLICATE KEY UPDATE`
- Supports InnoDB compression (`ROW_FORMAT=COMPRESSED`)
- Designed for 24/7 operation (loops continuously with polite pauses)
- Optional: plot data with `matplotlib`

---

## 🧰 Requirements
- Ubuntu 22.04+
- Python ≥ 3.8
- MySQL ≥ 8.0.30
- `innodb_file_per_table=ON` (required for InnoDB compression)

---

## ⚙️ Setup

```bash
# Clone the repository
git clone https://github.com/OpenJUP/drift-funding-rate-ingestor.git
cd drift-funding-rate-ingestor

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
