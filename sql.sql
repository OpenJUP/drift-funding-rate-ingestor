CREATE TABLE IF NOT EXISTS funding_rates (
  time            TIMESTAMP NOT NULL,     -- store UTC seconds as timestamp
  market          VARCHAR(32) NOT NULL,   -- e.g. 'SOL-PERP'
  funding_rate_apr DOUBLE NOT NULL,       -- APR in percent
  oracle_price     DOUBLE NOT NULL,       -- raw (micro) price from API
  mark_price       DOUBLE NOT NULL,       -- raw (micro) mark TWAP
  inserted_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (time, market),
  KEY market_time (market, time DESC)
)
ENGINE=InnoDB
ROW_FORMAT=COMPRESSED
KEY_BLOCK_SIZE=8
-- If your FS supports page compression, you could use this instead:
-- COMPRESSION='zlib'
DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;