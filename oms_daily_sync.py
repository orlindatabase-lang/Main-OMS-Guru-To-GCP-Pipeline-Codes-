import requests
import pandas as pd
import time
import random
import sys
import uuid
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from google.cloud import bigquery

# ============================================================
# RUN ONCE IN BIGQUERY TO CREATE TABLE (partition + cluster):
#
# CREATE TABLE `orlinappareldataset.oms_raw.orders`
# (
#   invoice_id STRING, invoice_date DATETIME, order_date DATETIME,
#   shipment_date DATETIME, delivered_date DATETIME,
#   warehouse STRING, channel STRING, company STRING,
#   buyer_name STRING, buyer_address1 STRING, buyer_address2 STRING,
#   buyer_city STRING, buyer_state STRING, buyer_pincode STRING,
#   buyer_phone STRING, buyer_email STRING,
#   billing_name STRING, billing_address1 STRING, billing_address2 STRING,
#   billing_city STRING, billing_state STRING, billing_pincode STRING,
#   billing_phone STRING, billing_email STRING,
#   shipping_company STRING, shipment_tracker STRING,
#   order_type STRING, po_number STRING,
#   channel_order_id STRING, channel_sub_order_id STRING,
#   sku_code STRING, qty FLOAT64,
#   selling_price_per_item FLOAT64, shipping_charge_per_item FLOAT64,
#   promo_discounts FLOAT64, gift_wrap_charges FLOAT64,
#   transaction_charges FLOAT64, invoice_amount FLOAT64,
#   currency_code STRING, tax_rate FLOAT64, tax_amount FLOAT64,
#   igst_rate FLOAT64, igst_amount FLOAT64,
#   cgst_rate FLOAT64, cgst_amount FLOAT64,
#   sgst_rate FLOAT64, sgst_amount FLOAT64,
#   status STRING, settlement_amount FLOAT64,
#   gst_name STRING, gst_number STRING,
#   pack_log STRING, sku_upc STRING, listing_sku STRING,
#   load_date DATE
# )
# PARTITION BY DATE(order_date)
# CLUSTER BY channel_sub_order_id, status
# OPTIONS (require_partition_filter = false);
#
# ============================================================

# ============================================================
# 1. CONFIGURATION
# ============================================================
PROJECT_ID    = "orlinappareldataset"
DATASET_ID    = "oms_raw"
TABLE_ID      = "orders"
FETCH_DAYS    = 20   # temporarily 20 for one-time fix — change back to 16 after fix run
REFRESH_DAYS  = 20   # open-status refresh total lookback

EMAIL_FROM     = "sanket.orlin@gmail.com"
EMAIL_PASSWORD = "ouzw ixvb cwaa nwzr"
EMAIL_TO       = ["orlindatabase@gmail.com", "alpeshorlin@gmail.com"]

IST         = timezone(timedelta(hours=5, minutes=30))
CHUNK_HOURS = 2
MAX_WORKERS = 2
REQUEST_GAP = 1.0
DAY_SLEEP   = 5

URL     = "https://client.omsguru.com/order_api/orders"
HEADERS = {
    "Accept": "application/json",
    "oms-cid": "34455",
    "Authorization": "gHQ5KEc8PvAfZLDsGTk2r7yF1mowWiNI"
}

# Fetch warehouse list once at startup
_wh_resp       = requests.get("https://client.omsguru.com/order_api/warehouses", headers=HEADERS)
warehouse_list = _wh_resp.json()
status_id      = []

DATE_COLS    = ["invoice_date", "order_date", "shipment_date", "delivered_date"]
NUMERIC_COLS = [
    "qty", "selling_price_per_item", "shipping_charge_per_item", "promo_discounts",
    "gift_wrap_charges", "transaction_charges", "invoice_amount",
    "tax_rate", "tax_amount", "igst_rate", "igst_amount",
    "cgst_rate", "cgst_amount", "sgst_rate", "sgst_amount", "settlement_amount"
]

# Statuses that are NOT final — orders with these need periodic refresh
OPEN_STATUSES = [
    "Shipped", "In Transit", "New", "Packed", "Ready to ship",
    "Return Init", "Cancel Init", "Pending", "Processing"
]

# ~20% static columns — set at order creation, NEVER overwritten on UPDATE
STATIC_COLS = {
    "invoice_id", "order_date", "invoice_date",
    "channel_order_id", "channel_sub_order_id",
    "sku_code", "qty",
    "warehouse", "channel", "company", "order_type",
    "buyer_name", "buyer_address1", "buyer_address2",
    "buyer_city", "buyer_state", "buyer_pincode",
    "buyer_phone", "buyer_email",
    "billing_name", "billing_address1", "billing_address2",
    "billing_city", "billing_state", "billing_pincode",
    "billing_phone", "billing_email",
    "currency_code", "gst_name", "gst_number",
}

PARENT_COLS = [
    "invoice_id", "invoice_date", "order_date", "shipment_date", "delivered_date",
    "warehouse", "channel", "company",
    "buyer_name", "buyer_address1", "buyer_address2", "buyer_city", "buyer_state",
    "buyer_pincode", "buyer_phone", "buyer_email",
    "billing_name", "billing_address1", "billing_address2", "billing_city",
    "billing_state", "billing_pincode", "billing_phone", "billing_email",
    "shipping_company", "shipment_tracker", "order_type", "po_number",
]

ITEM_COLS = [
    "channel_order_id", "channel_sub_order_id", "sku_code", "qty",
    "selling_price_per_item", "shipping_charge_per_item", "promo_discounts",
    "gift_wrap_charges", "transaction_charges", "invoice_amount", "currency_code",
    "tax_rate", "tax_amount", "igst_rate", "igst_amount",
    "cgst_rate", "cgst_amount", "sgst_rate", "sgst_amount",
    "status", "settlement_amount", "gst_name", "gst_number",
    "pack_log", "sku_upc", "listing_sku",
]

_sem       = threading.Semaphore(2)
_ok        = threading.Event()
_ok.set()
print_lock = threading.Lock()
log_lines  = []

def log(msg):
    print(msg)
    log_lines.append(msg)

# ============================================================
# 2. EMAIL
# ============================================================
def send_email(mode, start_date, end_date, raw_fetched,
               inserted, updated, duration_seconds,
               is_error=False, refresh_summary=None):
    dm  = int(duration_seconds // 60)
    ds  = int(duration_seconds % 60)
    bg  = "#fde8e8" if is_error else "#e8f5e9"
    bdr = "#f5c6cb" if is_error else "#a5d6a7"
    icon     = "X" if is_error else "OK"
    st       = "Failed" if is_error else "Success"
    mode_lbl = "Daily Sync (2 AM)" if mode == "daily" else "Backfill + Refresh (11:59 PM)"

    def trow(i, label, value):
        bg2 = "#ffffff" if i % 2 == 0 else "#f5fdf5"
        return (f"<tr>"
                f"<td style='padding:10px 14px;border:1px solid {bdr};"
                f"background:{bg2};width:38%;'><b>{label}</b></td>"
                f"<td style='padding:10px 14px;border:1px solid {bdr};background:{bg2};'>{value}</td>"
                f"</tr>")

    main_rows = [
        ("Mode",                  mode_lbl),
        ("Period",                f"{start_date} to {end_date}"),
        ("API Records Fetched",   f"{raw_fetched:,}"),
        ("Inserted (New)",        f"<b style='color:green'>{inserted:,}</b>"),
        ("Updated (Changed)",     f"{updated:,}"),
        ("Duration",              f"{dm}m {ds}s"),
        ("Completed At",          datetime.now(IST).strftime("%d-%m-%Y %H:%M:%S IST")),
        ("BigQuery Table",        f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"),
    ]
    main_html = "".join(trow(i, l, v) for i, (l, v) in enumerate(main_rows))

    refresh_html = ""
    if refresh_summary:
        sr = refresh_summary
        r_rows = [
            ("Dates Scanned",     str(sr.get("dates_processed", 0))),
            ("Orders Re-fetched", f"{sr.get('total_fetched', 0):,}"),
            ("Rows Updated",      f"<b style='color:green'>{sr.get('total_updated', 0):,}</b>"),
            ("Errors",            str(len(sr.get("errors", [])))),
        ]
        r_html = "".join(trow(i, l, v) for i, (l, v) in enumerate(r_rows))
        refresh_html = (
            f"<h3 style='color:#333;margin-top:20px;'>"
            f"Open-Status Refresh (Days {FETCH_DAYS+1}-{REFRESH_DAYS})</h3>"
            f"<table style='width:100%;border-collapse:collapse;'>{r_html}</table>"
        )
        if sr.get("errors"):
            err_li = "".join(f"<li style='color:red;font-size:11px;'>{e}</li>"
                             for e in sr["errors"])
            refresh_html += f"<ul>{err_li}</ul>"

    log_html = ""
    if log_lines:
        log_trs = "".join(
            f"<tr><td style='padding:4px 10px;font-family:monospace;"
            f"font-size:11px;color:#444;border-bottom:1px solid #eee;'>{l}</td></tr>"
            for l in log_lines[-30:]
        )
        log_html = (
            "<h3 style='color:#333;margin-top:20px;'>Last 30 Log Lines</h3>"
            f"<table style='width:100%;background:#f9f9f9;"
            f"border:1px solid #ddd;border-collapse:collapse;'>{log_trs}</table>"
        )

    body = (
        f"<html><body style='font-family:Arial,sans-serif;background:#f0f0f0;padding:20px;'>"
        f"<div style='max-width:720px;margin:auto;background:{bg};"
        f"border:1px solid {bdr};border-radius:12px;padding:28px;'>"
        f"<h2 style='margin-top:0;color:#1b5e20;'>[{icon}] OMS Sync {st}</h2>"
        f"<table style='width:100%;border-collapse:collapse;margin-bottom:16px;'>{main_html}</table>"
        f"{refresh_html}{log_html}"
        f"<hr style='border:1px solid {bdr};margin:20px 0;'>"
        f"<small style='color:#777;'>OMS Pipeline — Auto Mailer | "
        f"{datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}</small>"
        f"</div></body></html>"
    )
    subject = (f"[{icon}] OMS {mode_lbl} | {start_date} to {end_date} | "
               f"{inserted:,} New | {dm}m {ds}s")
    try:
        msg = MIMEMultipart()
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        srv = smtplib.SMTP("smtp.gmail.com", 587)
        srv.starttls()
        srv.login(EMAIL_FROM, EMAIL_PASSWORD)
        srv.send_message(msg)
        srv.quit()
        log(f"  Email sent -> {', '.join(EMAIL_TO)}")
    except Exception as e:
        log(f"  Email failed: {e}")

# ============================================================
# 3. SINGLE REQUEST
# ============================================================
def _post(payload, timeout=45):
    rl_wait = 40
    for attempt in range(6):
        _ok.wait()
        with _sem:
            try:
                r = requests.post(URL, headers=HEADERS, data=payload, timeout=timeout)
            except requests.exceptions.Timeout:
                time.sleep(10 * (attempt + 1))
                continue
            except Exception:
                time.sleep(8 * (attempt + 1))
                continue

            if r.status_code == 429:
                _ok.clear()
                time.sleep(rl_wait + random.uniform(5, 20))
                rl_wait = int(rl_wait * 1.5)
                _ok.set()
                continue

            if r.status_code != 200:
                return None

            time.sleep(REQUEST_GAP)
            return r.json().get("data", [])
    return None

# ============================================================
# 4. FETCH CHUNK & DAY
# ============================================================
def fetch_chunk(start_ts, end_ts, depth=0):
    orders, last_id = [], 0
    while True:
        payload = {
            "start_order_date": start_ts,
            "end_order_date":   end_ts,
            "last_id":          last_id,
            "limit":            100,
            "warehouse_id ":    warehouse_list,
            "status_id":        status_id,
        }
        batch = _post(payload)
        if batch is None:
            break
        if len(orders) + len(batch) >= 900 and depth < 8:
            mid = start_ts + (end_ts - start_ts) // 2
            return (fetch_chunk(start_ts, mid, depth + 1) +
                    fetch_chunk(mid + 1, end_ts, depth + 1))
        if not isinstance(batch, list) or len(batch) == 0:
            break
        orders.extend(batch)
        last_id = batch[-1].get("last_id", 0)
        if len(batch) < 100:
            break
    return orders

def generate_chunks(start_ts, end_ts):
    step = int(CHUNK_HOURS * 3600)
    cur  = start_ts
    while cur < end_ts:
        yield cur, min(cur + step - 1, end_ts)
        cur += step

def fetch_day(day_start, day_end):
    chunks = list(generate_chunks(day_start, day_end))
    day_orders, done, total, t0 = [], 0, len(chunks), time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_chunk, s, e): (s, e) for s, e in chunks}
        for fut in as_completed(futs):
            result = fut.result()
            done  += 1
            with print_lock:
                day_orders.extend(result)
                elapsed = time.time() - t0
                filled  = int(25 * done / total)
                bar     = "#" * filled + "-" * (25 - filled)
                print(f"\r    [{bar}] {done}/{total} chunks | "
                      f"{len(day_orders):,} orders | {elapsed:.0f}s   ",
                      end="", flush=True)
    print()
    return day_orders

# ============================================================
# 5. DATE HELPERS
# ============================================================
def _day_timestamps(d):
    ts_s = int(datetime.combine(d, datetime.min.time())
                .replace(hour=0,  minute=0,  second=0,  tzinfo=IST).timestamp())
    ts_e = int(datetime.combine(d, datetime.min.time())
                .replace(hour=23, minute=59, second=59, tzinfo=IST).timestamp())
    return ts_s, ts_e

def get_yesterday():
    d = (datetime.now(IST) - timedelta(days=1)).date()
    ts_s, ts_e = _day_timestamps(d)
    return [(ts_s, ts_e, d.strftime("%d-%m-%Y"))]

def get_last_n_days(n):
    end_d   = datetime.now(IST).date()
    start_d = end_d - timedelta(days=n)
    d = start_d
    while d <= end_d:
        ts_s, ts_e = _day_timestamps(d)
        yield ts_s, ts_e, d.strftime("%d-%m-%Y")
        d += timedelta(days=1)

# ============================================================
# 6. PROCESS RAW DATA
# ============================================================
def convert_ts(series):
    n = pd.to_numeric(series, errors="coerce")
    m = n.dropna().median()
    if not pd.isna(m) and m > 1e11:
        n = n / 1000
    result = pd.to_datetime(n, unit="s", errors="coerce", utc=True)
    return result.dt.tz_convert(IST).dt.tz_localize(None).astype("datetime64[us]")

def process(all_raw):
    df = pd.DataFrame(all_raw)
    log(f"  Raw records     : {len(df):,}")

    if "order_items" not in df.columns:
        return df

    parent_cols = [c for c in df.columns if c != "order_items"]
    df = df.explode("order_items").reset_index(drop=True)
    items_df = pd.json_normalize(df["order_items"].dropna())
    items_df.index = df["order_items"].dropna().index
    df = pd.concat(
        [df[parent_cols].reset_index(drop=True),
         items_df.reset_index(drop=True)],
        axis=1
    )
    log(f"  After flatten   : {len(df):,}")

    for col in DATE_COLS:
        if col in df.columns:
            df[col] = convert_ts(df[col])

    all_desired = PARENT_COLS + ITEM_COLS
    for col in all_desired:
        if col not in df.columns:
            df[col] = ""
    extra = [c for c in df.columns if c not in all_desired]
    df = df[all_desired + extra]

    df["channel_sub_order_id"] = df["channel_sub_order_id"].astype(str).str.strip()
    df["channel_order_id"]     = df["channel_order_id"].astype(str).str.strip()

    # OMS API now sends channel_order_id blank and puts the value in channel_sub_order_id.
    # Restore channel_order_id from channel_sub_order_id when blank.
    blank_mask = df["channel_order_id"].isin(["", "nan", "None"])
    df.loc[blank_mask, "channel_order_id"] = df.loc[blank_mask, "channel_sub_order_id"]

    df = df[df["channel_sub_order_id"].notna() & (df["channel_sub_order_id"] != "")]

    before = len(df)
    df = df.drop_duplicates(subset=["channel_sub_order_id"], keep="last")
    if len(df) != before:
        log(f"  Deduped         : {before - len(df):,} removed")

    df["load_date"] = datetime.now(IST).strftime("%Y-%m-%d")

    for col in df.columns:
        if col not in DATE_COLS and col not in ["load_date"] + NUMERIC_COLS:
            df[col] = df[col].astype(str).replace({"nan": "", "<NA>": ""})

    log(f"  Final rows      : {len(df):,}")
    return df

# ============================================================
# 7. BIGQUERY UPSERT
#    STATIC cols  (~20%) -> INSERT only, never overwritten
#    DYNAMIC cols (~80%) -> INSERT + UPDATE on every MERGE MATCHED
# ============================================================
def _dynamic_cols(df):
    return [c for c in df.columns if c not in STATIC_COLS]

def _bq_date_schema(df):
    schema = []
    mapping = {"invoice_date": "DATETIME", "order_date": "DATETIME",
               "shipment_date": "DATETIME", "delivered_date": "DATETIME"}
    for col, dtype in mapping.items():
        if col in df.columns:
            schema.append(bigquery.SchemaField(col, dtype))
    return schema

def _load_temp(bq, df, temp_tbl):
    job = bq.load_table_from_dataframe(
        df, temp_tbl,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            schema=_bq_date_schema(df)
        )
    )
    job.result()

def save_to_bigquery(df, order_date_str=None):
    log("  Uploading to BigQuery...")
    bq        = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    temp_tbl  = f"{PROJECT_ID}.{DATASET_ID}.temp_{uuid.uuid4().hex[:8]}"

    before = bq.query(f"SELECT COUNT(*) AS cnt FROM `{table_ref}`") \
               .to_dataframe()["cnt"][0]

    _load_temp(bq, df, temp_tbl)
    log(f"  Loaded temp table: {len(df):,} rows")

    dynamic_cols = _dynamic_cols(df)
    update_set   = ",\n              ".join(
        f"target.{c} = source.{c}" for c in dynamic_cols
    )

    # Partition filter: only scan this date's partition, not full table
    partition_filter = ""
    if order_date_str:
        try:
            d = datetime.strptime(order_date_str, "%d-%m-%Y").strftime("%Y-%m-%d")
            partition_filter = f"\n    AND DATE(target.order_date) = DATE('{d}')"
        except Exception:
            pass

    bq.query(f"""
    MERGE `{table_ref}` AS target
    USING (
        SELECT * REPLACE (
            CAST(invoice_date   AS DATETIME) AS invoice_date,
            CAST(order_date     AS DATETIME) AS order_date,
            CAST(shipment_date  AS DATETIME) AS shipment_date,
            CAST(delivered_date AS DATETIME) AS delivered_date
        )
        FROM `{temp_tbl}`
    ) AS source
    ON  target.channel_sub_order_id = source.channel_sub_order_id
        {partition_filter}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ROW
    """).result()

    bq.delete_table(temp_tbl, not_found_ok=True)

    after    = bq.query(f"SELECT COUNT(*) AS cnt FROM `{table_ref}`") \
                 .to_dataframe()["cnt"][0]
    inserted = int(after - before)
    updated  = len(df) - inserted
    log(f"  BQ rows: {before:,} -> {after:,} | +{inserted:,} new | {updated:,} updated")
    return inserted, updated

# ============================================================
# 8. OPEN-STATUS REFRESH
#    Covers days {FETCH_DAYS+1} to {REFRESH_DAYS} (days 17-20)
#    Finds open-status orders in that window, re-fetches, updates
#    ALL dynamic columns (80%) — never touches static cols (20%).
# ============================================================
def run_open_status_refresh():
    log("\n" + "=" * 60)
    log(f"  OPEN-STATUS REFRESH (Days {FETCH_DAYS + 1} to {REFRESH_DAYS})")
    log("=" * 60)

    bq        = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    today     = datetime.now(IST).date()

    refresh_end   = today - timedelta(days=FETCH_DAYS + 1)
    refresh_start = today - timedelta(days=REFRESH_DAYS)

    if refresh_start > refresh_end:
        log("  Refresh window is empty — nothing to do.")
        return {"dates_processed": 0, "total_fetched": 0, "total_updated": 0, "errors": []}

    status_list = ", ".join(f"'{s}'" for s in OPEN_STATUSES)

    open_dates = list(bq.query(f"""
        SELECT DATE(order_date) AS order_date_only, COUNT(*) AS cnt
        FROM `{table_ref}`
        WHERE status IN ({status_list})
          AND order_date IS NOT NULL
          AND DATE(order_date) BETWEEN DATE('{refresh_start.isoformat()}')
                                   AND DATE('{refresh_end.isoformat()}')
        GROUP BY order_date_only
        ORDER BY order_date_only ASC
    """).result())

    if not open_dates:
        log(f"  No open-status orders between {refresh_start} and {refresh_end}.")
        return {"dates_processed": 0, "total_fetched": 0, "total_updated": 0, "errors": []}

    log(f"  Found {len(open_dates)} date(s) to refresh:")
    for r in open_dates:
        log(f"     {r.order_date_only}  ->  {r.cnt:,} open orders")

    total_fetched = 0
    total_updated = 0
    errors        = []

    for r in open_dates:
        order_date = r.order_date_only
        date_str   = order_date.strftime("%d-%m-%Y")
        log(f"\n  Refreshing {date_str} ({r.cnt:,} open orders)...")

        try:
            ts_s, ts_e = _day_timestamps(order_date)
            raw        = fetch_day(ts_s, ts_e)
            if not raw:
                log(f"     API returned 0 orders — skipping.")
                continue

            total_fetched += len(raw)
            df = process(raw)
            if df.empty:
                log(f"     No valid rows after processing.")
                continue

            dynamic_cols = _dynamic_cols(df)
            slim_cols    = (["channel_sub_order_id"] +
                            [c for c in dynamic_cols if c in df.columns])
            slim_df      = df[slim_cols].copy()
            slim_df["load_date"] = datetime.now(IST).strftime("%Y-%m-%d")

            temp_tbl = f"{PROJECT_ID}.{DATASET_ID}.ref_temp_{uuid.uuid4().hex[:8]}"
            _load_temp(bq, slim_df, temp_tbl)

            update_set = ",\n              ".join(
                f"target.{c} = source.{c}"
                for c in dynamic_cols if c in slim_df.columns
            )

            # Partition-aware: only touch this date's partition
            bq.query(f"""
            MERGE `{table_ref}` AS target
            USING (
                SELECT * REPLACE (
                    CAST(shipment_date  AS DATETIME) AS shipment_date,
                    CAST(delivered_date AS DATETIME) AS delivered_date
                )
                FROM `{temp_tbl}`
            ) AS source
            ON  target.channel_sub_order_id = source.channel_sub_order_id
            AND DATE(target.order_date) = DATE('{order_date.isoformat()}')
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            """).result()

            bq.delete_table(temp_tbl, not_found_ok=True)
            total_updated += len(slim_df)
            log(f"     {len(slim_df):,} rows refreshed for {date_str}")

        except Exception as e:
            msg = f"{date_str}: {e}"
            log(f"     ERROR: {msg}")
            errors.append(msg)

        time.sleep(DAY_SLEEP)

    log(f"\n  Refresh complete — {len(open_dates)} dates | "
        f"{total_fetched:,} fetched | {total_updated:,} updated | "
        f"{len(errors)} errors")
    return {
        "dates_processed": len(open_dates),
        "total_fetched":   total_fetched,
        "total_updated":   total_updated,
        "errors":          errors,
    }

# ============================================================
# 9. MAIN
#    Triggered by Cloud Scheduler via environment variable MODE:
#      MODE=daily    -> 2 AM   -> fetch yesterday only
#      MODE=backfill -> 11:59PM -> last 16 days + open-status refresh
# ============================================================
if __name__ == "__main__":
    import os
    # Accept mode from env var (Cloud Run) or command-line arg (local testing)
    mode       = os.environ.get("MODE") or (sys.argv[1] if len(sys.argv) > 1 else "daily")
    start_time = datetime.now(IST)
    t_grand    = time.time()

    log("=" * 60)

    # ----------------------------------------------------------
    # DAILY MODE  (2 AM)
    # ----------------------------------------------------------
    if mode == "daily":
        log("  OMS Daily Sync — Yesterday -> BigQuery")
        days       = get_yesterday()
        start_date = days[0][2]
        end_date   = days[-1][2]
        log(f"  Started : {start_time.strftime('%d-%m-%Y %H:%M:%S IST')}")
        log("=" * 60)

        total_raw = total_inserted = total_updated = 0

        try:
            for ds, de, label in days:
                log(f"\n  Fetching {label}...")
                t0     = time.time()
                orders = fetch_day(ds, de)
                log(f"     {len(orders):,} orders in {time.time()-t0:.0f}s")
                if not orders:
                    log("     No data.")
                    continue
                total_raw += len(orders)
                df = process(orders)
                ins, upd = save_to_bigquery(df, order_date_str=label)
                total_inserted += ins
                total_updated  += upd

            elapsed = time.time() - t_grand
            log(f"\n  DAILY DONE at {datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}")
            send_email(mode, start_date, end_date, total_raw,
                       total_inserted, total_updated, elapsed)

        except Exception as e:
            elapsed = time.time() - t_grand
            log(f"\n  CRITICAL ERROR: {e}")
            send_email(mode, start_date, end_date, total_raw,
                       0, 0, elapsed, is_error=True)

    # ----------------------------------------------------------
    # BACKFILL MODE  (11:59 PM)
    # ----------------------------------------------------------
    else:
        log(f"  OMS Backfill — Last {FETCH_DAYS} Days + Open-Status Refresh")
        days       = list(get_last_n_days(FETCH_DAYS))
        start_date = days[0][2]
        end_date   = days[-1][2]
        log(f"  Started : {start_time.strftime('%d-%m-%Y %H:%M:%S IST')}")
        log(f"  Days    : {len(days)}")
        log("=" * 60)

        total_raw = total_inserted = total_updated = 0
        refresh_summary = None

        try:
            for idx, (ds, de, label) in enumerate(days, 1):
                log(f"\n  Day {idx}/{len(days)} — {label}")
                t0     = time.time()
                orders = fetch_day(ds, de)
                log(f"     {len(orders):,} orders in {time.time()-t0:.0f}s")

                if not orders:
                    log(f"     No data — skipping.")
                    if idx < len(days):
                        time.sleep(DAY_SLEEP)
                    continue

                total_raw += len(orders)
                df = process(orders)
                ins, upd = save_to_bigquery(df, order_date_str=label)
                total_inserted += ins
                total_updated  += upd
                log(f"     {ins:,} inserted | {upd:,} updated")

                if idx < len(days):
                    time.sleep(DAY_SLEEP)

            elapsed = time.time() - t_grand
            log(f"\n  16-DAY SYNC DONE in {int(elapsed//60)}m {int(elapsed%60)}s")
            log(f"  Total Raw     : {total_raw:,}")
            log(f"  Total Inserted: {total_inserted:,}")
            log(f"  Total Updated : {total_updated:,}")

            refresh_summary = run_open_status_refresh()

            elapsed = time.time() - t_grand
            log(f"\n  BACKFILL DONE at {datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}")
            send_email(mode, start_date, end_date, total_raw,
                       total_inserted, total_updated, elapsed,
                       refresh_summary=refresh_summary)

        except Exception as e:
            elapsed = time.time() - t_grand
            log(f"\n  CRITICAL ERROR: {e}")
            send_email(mode, start_date, end_date, total_raw,
                       0, 0, elapsed, is_error=True,
                       refresh_summary=refresh_summary)
