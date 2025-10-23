#!/usr/bin/env python3
import os, json, time, random, urllib.request, urllib.error, argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional

import mysql.connector  # pip install mysql-connector-python

# Load environment variables from .env file
try:
    from dotenv import load_dotenv  # pip install python-dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install with: pip install python-dotenv")
    print("Falling back to system environment variables...")

API_BASE_URL = "https://data.api.drift.trade/market"
MARKETS = ["SOL-PERP", "BTC-PERP", "ETH-PERP"]

# Fetch cadence / backoff
REQUEST_DELAY_SECONDS = 0.20
MAX_API_RETRIES = 3
TIMEOUT_SECS = 20
BAN_SLEEP_FALLBACK = 65     # if Retry-After header absent
DEFAULT_LOOKBACK_DAYS = 365
SLEEP_BETWEEN_FULL_PASSES = 300

# MySQL connection (TCP/IP) - Load from environment variables
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB   = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD")

# Validate required environment variables
if not all([MYSQL_HOST, MYSQL_DB, MYSQL_USER]):
    raise ValueError("Missing required environment variables. Please set MYSQL_HOST, MYSQL_DATABASE, and MYSQL_USER in your .env file")
if not MYSQL_PASS:
    print("Warning: MYSQL_PASSWORD is empty")

_stop = False
def _ts_utc_now_midnight():
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def db_connect():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=False
    )

def fetch_funding_rates(market: str, date_str: str) -> List[Dict]:
    y, m, d = date_str.split("-")
    url = f"{API_BASE_URL}/{market}/fundingRates/{y}/{m}/{d}?format=json"
    headers = {"User-Agent": "drift-funding-mysql-ingestor/1.0"}
    attempt = 1
    while True:
        try:
            print(f"[{market}] GET {url} (attempt {attempt})")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECS) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body)
            if not data.get("success", False):
                print(f"  -> API reported failure for {market} on {date_str}")
                return []
            recs = data.get("records", [])
            if len(recs) > 10000:
                print(f"  -> suspiciously large payload: {len(recs)}; skipping day")
                return []
            return recs

        except urllib.error.HTTPError as e:
            status = e.code
            retry_after = int(e.headers.get("Retry-After", "0") or "0")
            print(f"  -> HTTPError {status} for {market} {date_str}")
            if status in (429, 403):           # temporary ban / rate limited
                sleep_for = retry_after if retry_after > 0 else BAN_SLEEP_FALLBACK
                print(f"  -> sleeping {sleep_for}s due to ban/rate-limit")
                time.sleep(sleep_for)
                attempt += 1
                continue
            if attempt >= MAX_API_RETRIES:
                print("  -> giving up")
                return []
            backoff = (2 ** (attempt - 1)) + random.random()
            print(f"  -> retrying in {backoff:.2f}s")
            time.sleep(backoff); attempt += 1

        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            print(f"  -> transient error: {e}")
            if attempt >= MAX_API_RETRIES:
                print("  -> giving up")
                return []
            backoff = (2 ** (attempt - 1)) + random.random()
            print(f"  -> retrying in {backoff:.2f}s")
            time.sleep(backoff); attempt += 1

        time.sleep(REQUEST_DELAY_SECONDS)

def parse_row(r: Dict) -> Tuple[datetime, str, float, float, float]:
    ts = int(r["ts"])
    market = str(r["symbol"])
    funding_rate_raw = float(r["fundingRate"])
    oracle_price_raw = float(r["oraclePriceTwap"])
    mark_price_raw   = float(r["markPriceTwap"])
    if oracle_price_raw <= 0.0:
        raise ZeroDivisionError("oraclePriceTwap <= 0")
    # annualized APR in percent
    funding_rate_apr = 24 * 365.25 * 100_000 * (funding_rate_raw / 1e9) / (oracle_price_raw / 1e6)
    return (datetime.fromtimestamp(ts, tz=timezone.utc), market,
            float(funding_rate_apr), float(oracle_price_raw), float(mark_price_raw))

def get_latest_ts_per_market(conn) -> Dict[str, int]:
    sql = "SELECT market, UNIX_TIMESTAMP(MAX(time)) AS latest_ts FROM funding_rates GROUP BY market"
    out = {}
    with conn.cursor(dictionary=True) as cur:
        cur.execute(sql)
        for row in cur.fetchall():
            if row["latest_ts"] is not None:
                out[row["market"]] = int(row["latest_ts"])
    return out

def upsert_rows(conn, rows: List[Tuple[datetime, str, float, float, float]]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO funding_rates (time, market, funding_rate_apr, oracle_price, mark_price)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      funding_rate_apr = VALUES(funding_rate_apr),
      oracle_price     = VALUES(oracle_price),
      mark_price       = VALUES(mark_price);
    """  # VALUES() is valid in MySQL 8.x for this clause. :contentReference[oaicite:4]{index=4}
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    return len(rows)

def daterange(start_date: datetime, end_date: datetime):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)

def run_once(conn, force_start_date: Optional[datetime] = None) -> int:
    total = 0
    today_utc = _ts_utc_now_midnight()
    latest = get_latest_ts_per_market(conn)
    
    for market in MARKETS:
        print("=" * 80)
        print(f"[{market}] scanning for missing days")
        
        if force_start_date:
            # Backfill mode: use the forced start date
            start_day = force_start_date
            print(f"  BACKFILL MODE: forcing start from {force_start_date.strftime('%Y-%m-%d')}")
        elif market in latest:
            # Normal mode: continue from latest data
            last_dt = datetime.fromtimestamp(latest[market], tz=timezone.utc)
            last_day = last_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            start_day = last_day if last_day == today_utc else (last_day + timedelta(days=1))
            print(f"  latest in DB: {last_dt.isoformat()}")
        else:
            # No data yet: seed with default lookback
            start_day = today_utc - timedelta(days=DEFAULT_LOOKBACK_DAYS)
            print(f"  no data yet; seeding last {DEFAULT_LOOKBACK_DAYS} days")

        if start_day > today_utc:
            print("  up to date.")
            continue

        days = [d.strftime("%Y-%m-%d") for d in daterange(start_day, today_utc)]
        print(f"  fetching {len(days)} day(s): {days[0]} -> {days[-1]}")

        batch = []
        for day in days:
            recs = fetch_funding_rates(market, day)
            for r in recs:
                try:
                    batch.append(parse_row(r))
                except (KeyError, ValueError, ZeroDivisionError) as e:
                    print(f"  -> skipping bad record: {e}")
            time.sleep(REQUEST_DELAY_SECONDS + random.random() * 0.1)

        if batch:
            n = upsert_rows(conn, batch)
            total += n
            print(f"  upserted {n} rows for {market}")
        else:
            print("  no new rows")

    print("=" * 80)
    print(f"PASS COMPLETE: {total} rows inserted/updated")
    print("=" * 80)
    return total

def main():
    parser = argparse.ArgumentParser(description='Drift funding rate ingestor')
    parser.add_argument('--backfill-from', type=str, metavar='YYYY-MM-DD',
                        help='Backfill data starting from this date (format: YYYY-MM-DD). '
                             'If specified, runs once and exits instead of continuous mode.')
    parser.add_argument('--run-once', action='store_true', 
                        help='Run once and exit instead of continuous mode')
    
    args = parser.parse_args()
    
    force_start_date = None
    if args.backfill_from:
        try:
            force_start_date = datetime.strptime(args.backfill_from, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            print(f"Backfill mode: starting from {force_start_date.strftime('%Y-%m-%d')}")
        except ValueError:
            print("Error: Invalid date format. Use YYYY-MM-DD format.")
            return 1
    
    conn = db_connect()
    try:
        if args.backfill_from or args.run_once:
            # Single run mode
            changed = run_once(conn, force_start_date)
            print(f"Single run completed: {changed} rows processed")
        else:
            # Continuous mode (original behavior)
            while True:
                changed = run_once(conn)
                nap = SLEEP_BETWEEN_FULL_PASSES if changed == 0 else 30
                print(f"Sleeping {nap}s...\n")
                for _ in range(nap):
                    time.sleep(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
