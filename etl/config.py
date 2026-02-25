from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
CURATED_DIR = BASE_DIR / "data" / "curated"
WAREHOUSE_DB = BASE_DIR / "warehouse.db"

SLA_THRESHOLD_MINUTES = 40
