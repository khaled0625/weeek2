import logging
import sys
from pathlib import Path

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_order_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
missingness_report,
add_missing_flags,
normalize_text,
apply_mapping
)
from bootcamp_data.quality import (
require_columns,
assert_non_empty
)
# from python_scripts.run_day1_load import ROOT
ROOT = Path(__file__).resolve().parents[1]
from bootcamp_data.transforms import enforce_schema
log = logging.getLogger(__name__)
def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    p = make_paths(ROOT)

    log.info("loading raw inputs")
    order_raw = read_order_csv(p.raw / "orders.csv")
    users = read_users_csv(p.raw / "users.csv")
    log.info("rows: order_raw=%s, users=%s", len(order_raw), len(users))

    require_columns(order_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users, ["user_id", "country", "signup_date"])
    assert_non_empty(order_raw, "order_raw")
    assert_non_empty(users, "users")

    orders = enforce_schema(order_raw)

    # Missingness artifact (do this early — before you “fix” missing values)
    rep = missingness_report(orders)
    reports_dir = ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    rep_path = reports_dir / "missingness_order.csv"
    rep.to_csv(rep_path, index=True)
    log.info("wrote missingness report : %s", rep_path)

    # Text normalization + controlled mapping
    status_norm = normalize_text(orders["status"])
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders.assign(status_clean= status_clean)
        .pipe(add_missing_flags, cols= ["amount", "quantity"])
    )

    # task 7

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")
    log.info("wrote processed output: %s", p.processed)



if __name__ == "__main__":
    main()