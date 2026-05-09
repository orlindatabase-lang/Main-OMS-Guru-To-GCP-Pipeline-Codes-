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
# 1. CONFIGURATION
# ============================================================
PROJECT_ID     = "orlinappareldataset"
DATASET_ID     = "oms_raw"
TABLE_ID       = "orders"
FETCH_DAYS     = 16

EMAIL_FROM     = "sanket.orlin@gmail.com"
EMAIL_PASSWORD = "ouzw ixvb cwaa nwzr"
EMAIL_TO       = ["orlindatabase@gmail.com", "alpeshorlin@gmail.com"]

URL     = "https://client.omsguru.com/order_api/orders"
HEADERS = {"Accept":"application/json","oms-cid":"34455",
           "Authorization":"gHQ5KEc8PvAfZLDsGTk2r7yF1mowWiNI"}

DATE_COLS   = ["invoice_date","order_date","shipment_date","delivered_date"]
IST         = timezone(timedelta(hours=5, minutes=30))
CHUNK_HOURS = 2
MAX_WORKERS = 2
REQUEST_GAP = 1.0
DAY_SLEEP   = 5

# Open statuses that need periodic refresh
OPEN_STATUSES = [
    'Shipped', 'In Transit', 'New', 'Packed',
    'Ready to ship', 'Return Init', 'Cancel Init',
    'Pending', 'Processing'
]

# Only these fields are updated during open-status refresh (non-destructive)
STATUS_UPDATE_FIELDS = [
    "status", "shipment_tracker", "shipping_company",
    "shipment_date", "delivered_date",
    "transaction_charges", "invoice_amount", "settlement_amount"
]

_sem        = threading.Semaphore(2)
_ok         = threading.Event()
_ok.set()
print_lock  = threading.Lock()
log_lines   = []

def log(msg):
    print(msg)
    log_lines.append(msg)


# ============================================================
# 2. EMAIL
# ============================================================
def send_email_notification(mode, start_date, end_date, raw_fetched,
                             inserted, updated, duration_seconds,
                             is_error=False, status_refresh_summary=None):
    duration_min = int(duration_seconds // 60)
    duration_sec = int(duration_seconds % 60)
    bg_color     = "#fde8e8" if is_error else "#e8f5e9"
    border_color = "#f5c6cb" if is_error else "#a5d6a7"
    icon         = "❌" if is_error else "✅"
    status_text  = "Failed" if is_error else "Success"
    mode_label   = "Daily Sync (2 AM)" if mode == "daily" else "Backfill Sync (11:59 PM)"

    log_rows = "".join([
        f"<tr><td style='padding:5px 10px;border-bottom:1px solid #eee;"
        f"font-family:monospace;font-size:11px;color:#444;'>{line}</td></tr>"
        for line in log_lines[-25:]
    ])
    log_html = f"""
    <h3 style="color:#333;margin-top:24px;">Execution Logs (Last 25 lines)</h3>
    <table style="width:100%;border-collapse:collapse;background:#f9f9f9;
      border:1px solid #ddd;border-radius:6px;overflow:hidden;">
      {log_rows}
    </table>
    """ if log_lines else ""

    # Open-status refresh section (only in backfill mode)
    refresh_html = ""
    if status_refresh_summary:
        sr = status_refresh_summary
        r_rows = [
            ("Dates Processed",  str(sr.get('dates_processed', 0))),
            ("Orders Re-fetched", f"{sr.get('total_fetched', 0):,}"),
            ("Status Updated",    f"<span style='color:green;font-weight:bold;'>{sr.get('total_updated', 0):,}</span>"),
            ("Errors",            str(len(sr.get('errors', [])))),
        ]
        r_table = ""
        for i, (lbl, val) in enumerate(r_rows):
            bg = "#ffffff" if i % 2 == 0 else "#f5fdf5"
            r_table += (f"<tr>"
                        f"<td style='padding:9px 14px;border:1px solid {border_color};background:{bg};width:38%;'><strong>{lbl}</strong></td>"
                        f"<td style='padding:9px 14px;border:1px solid {border_color};background:{bg};'>{val}</td>"
                        f"</tr>")
        refresh_html = f"""
        <h3 style="color:#333;margin-top:24px;">Open-Status Refresh</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">{r_table}</table>
        """
        if sr.get('errors'):
            err_rows = "".join(
                f"<tr><td style='padding:4px 10px;font-family:monospace;font-size:11px;color:red;'>{e}</td></tr>"
                for e in sr['errors']
            )
            refresh_html += f"<h4 style='color:red;'>Refresh Errors:</h4><table style='width:100%;background:#fff5f5;border:1px solid #f5c6cb;'>{err_rows}</table>"

    rows = [
        ("Mode",                    mode_label),
        ("Fetched Period",          f"{start_date} to {end_date}"),
        ("Raw Fetched from API",    f"{raw_fetched:,}"),
        ("Inserted (New Orders)",   f"<span style='color:green;font-weight:bold;'>{inserted:,}</span>"),
        ("Updated (Status Change)", f"{updated:,}"),
        ("Time Taken",              f"{duration_min}m {duration_sec}s"),
        ("Completed At",            datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')),
        ("BigQuery Table",          f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"),
    ]

    table_rows = ""
    for i, (label, value) in enumerate(rows):
        bg = "#ffffff" if i % 2 == 0 else "#f5fdf5"
        table_rows += (f"<tr>"
                       f"<td style='padding:11px 14px;border:1px solid {border_color};background:{bg};width:38%;'><strong>{label}</strong></td>"
                       f"<td style='padding:11px 14px;border:1px solid {border_color};background:{bg};'>{value}</td>"
                       f"</tr>")

    body = f"""
    <html><body style="font-family:Arial,sans-serif;padding:24px;background:#f0f0f0;">
    <div style="max-width:720px;margin:auto;background:{bg_color};
      border:1px solid {border_color};border-radius:14px;padding:32px;">
      <h2 style="color:#1b5e20;margin-top:0;font-size:22px;">{icon} OMS Sync {status_text}!</h2>
      <table style="width:100%;border-collapse:collapse;margin-bottom:20px;">{table_rows}</table>
      {refresh_html}
      {log_html}
      <hr style="border:1px solid {border_color};margin:24px 0;">
      <small style="color:#777;">OMS Pipeline — Auto Mailer | {datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}</small>
    </div>
    </body></html>
    """

    subject = (f"{icon} OMS {mode_label} ({start_date} to {end_date}) | "
               f"{inserted:,} New | {duration_min}m {duration_sec}s")

    try:
        msg = MIMEMultipart()
        msg["From"]    = EMAIL_FROM
        msg["To"]      = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "html"))
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"  Email sent: {subject}")
    except Exception as e:
        print(f"  Email failed: {e}")


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
                time.sleep(10*(attempt+1)); continue
            except Exception:
                time.sleep(8*(attempt+1)); continue
            if r.status_code == 429:
                _ok.clear()
                time.sleep(rl_wait + random.uniform(5, 20))
                rl_wait = int(rl_wait * 1.5)
                _ok.set(); continue
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
        }
        batch = _post(payload)
        if batch is None: break
        if len(orders)+len(batch) >= 900 and depth < 8:
            mid = start_ts+(end_ts-start_ts)//2
            return (fetch_chunk(start_ts, mid, depth+1) +
                    fetch_chunk(mid+1, end_ts, depth+1))
        if not isinstance(batch, list) or len(batch) == 0: break
        orders.extend(batch)
        last_id = batch[-1].get("last_id", 0)
        if len(batch) < 100: break
    return orders

def generate_chunks(start_ts, end_ts):
    step = int(CHUNK_HOURS*3600); cur = start_ts
    while cur < end_ts:
        yield cur, min(cur+step-1, end_ts); cur += step

def fetch_day(day_start, day_end):
    chunks = list(generate_chunks(day_start, day_end))
    day_orders, done, total, t0 = [], 0, len(chunks), time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_chunk, s, e):(s, e) for s, e in chunks}
        for fut in as_completed(futs):
            result = fut.result(); done += 1
            with print_lock:
                day_orders.extend(result)
            elapsed = time.time()-t0
            filled  = int(25*done/total)
            bar     = "#"*filled+"-"*(25-filled)
            print(f"\r    [{bar}] {done}/{total} chunks | "
                  f"{len(day_orders):,} orders | {elapsed:.0f}s   ",
                  end="", flush=True)
    print(); return day_orders


# ============================================================
# 5. DATE RANGES
# ============================================================
def get_yesterday():
    d    = (datetime.now(IST)-timedelta(days=1)).date()
    ts_s = int(datetime.combine(d, datetime.min.time())
               .replace(hour=0, minute=0, second=0, tzinfo=IST).timestamp())
    ts_e = int(datetime.combine(d, datetime.min.time())
               .replace(hour=23, minute=59, second=59, tzinfo=IST).timestamp())
    return [(ts_s, ts_e, d.strftime("%d-%m-%Y"))]

def get_last_16_days():
    end_dt   = datetime.now(IST)
    start_dt = end_dt - timedelta(days=FETCH_DAYS)
    d = start_dt.date()
    e = end_dt.date()
    while d <= e:
        ts_s = int(datetime.combine(d, datetime.min.time())
                   .replace(hour=0, minute=0, second=0, tzinfo=IST).timestamp())
        ts_e = int(datetime.combine(d, datetime.min.time())
                   .replace(hour=23, minute=59, second=59, tzinfo=IST).timestamp())
        yield ts_s, ts_e, d.strftime("%d-%m-%Y")
        d += timedelta(days=1)

def date_to_ist_timestamps(d):
    """Convert a date object to IST midnight start/end unix timestamps."""
    ts_s = int(datetime.combine(d, datetime.min.time())
               .replace(hour=0, minute=0, second=0, tzinfo=IST).timestamp())
    ts_e = int(datetime.combine(d, datetime.min.time())
               .replace(hour=23, minute=59, second=59, tzinfo=IST).timestamp())
    return ts_s, ts_e


# ============================================================
# 6. PROCESS DATA
# ============================================================
def convert_ts(series):
    n = pd.to_numeric(series, errors="coerce")
    m = n.dropna().median()
    if not pd.isna(m) and m > 1e11: n = n/1000
    result = pd.to_datetime(n, unit="s", errors="coerce", utc=True)
    result = result.dt.tz_convert(IST).dt.tz_localize(None)
    return result.astype("datetime64[us]")

PARENT_COLS = ["invoice_id","invoice_date","order_date","shipment_date","delivered_date",
               "warehouse","channel","company","buyer_name","buyer_address1","buyer_address2",
               "buyer_city","buyer_state","buyer_pincode","buyer_phone","buyer_email",
               "billing_name","billing_address1","billing_address2","billing_city",
               "billing_state","billing_pincode","billing_phone","billing_email",
               "shipping_company","shipment_tracker","order_type","po_number"]

ITEM_COLS   = ["channel_order_id","channel_sub_order_id","sku_code","qty",
               "selling_price_per_item","shipping_charge_per_item","promo_discounts",
               "gift_wrap_charges","transaction_charges","invoice_amount","currency_code",
               "tax_rate","tax_amount","igst_rate","igst_amount","cgst_rate","cgst_amount",
               "sgst_rate","sgst_amount","status","settlement_amount","gst_name",
               "gst_number","pack_log","sku_upc","listing_sku"]

NUMERIC_COLS = ["qty","selling_price_per_item","shipping_charge_per_item","promo_discounts",
                "gift_wrap_charges","transaction_charges","invoice_amount","tax_rate",
                "tax_amount","igst_rate","igst_amount","cgst_rate","cgst_amount",
                "sgst_rate","sgst_amount","settlement_amount"]

def process(all_raw):
    df = pd.DataFrame(all_raw)
    log(f"  Raw records     : {len(df):,}")
    if "order_items" not in df.columns:
        return df
    parent_cols = [c for c in df.columns if c != "order_items"]
    df = df.explode("order_items").reset_index(drop=True)
    items_df = pd.json_normalize(df["order_items"].dropna())
    items_df.index = df["order_items"].dropna().index
    df = pd.concat([df[parent_cols].reset_index(drop=True),
                    items_df.reset_index(drop=True)], axis=1)
    log(f"  After flatten   : {len(df):,}")
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = convert_ts(df[col])
    all_desired = PARENT_COLS + ITEM_COLS
    for col in all_desired:
        if col not in df.columns: df[col] = ""
    extra = [c for c in df.columns if c not in all_desired]
    df = df[all_desired + extra]
    df["channel_sub_order_id"] = df["channel_sub_order_id"].astype(str).str.strip()
    df = df[df["channel_sub_order_id"].notna() & (df["channel_sub_order_id"] != "")]
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["channel_sub_order_id"], keep="last")
    after_dedup  = len(df)
    if before_dedup != after_dedup:
        log(f"  Deduped: {before_dedup - after_dedup:,} removed")
    df["load_date"] = datetime.now(IST).date()
    for col in df.columns:
        if col not in DATE_COLS and col not in ["load_date"] + NUMERIC_COLS:
            df[col] = df[col].astype(str).replace("nan", "").replace("<NA>", "")
    log(f"  Final rows      : {len(df):,}")
    return df


# ============================================================
# 7. SAVE TO BIGQUERY WITH UPSERT (full — existing logic unchanged)
# ============================================================
def save_to_bigquery(df):
    log("  Uploading to BigQuery...")
    bq         = bigquery.Client(project=PROJECT_ID)
    table_ref  = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.temp_{uuid.uuid4().hex[:8]}"

    before_count = bq.query(
        f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
    ).to_dataframe()["cnt"][0]

    date_schema = [
        bigquery.SchemaField("invoice_date",   "DATETIME"),
        bigquery.SchemaField("order_date",     "DATETIME"),
        bigquery.SchemaField("shipment_date",  "DATETIME"),
        bigquery.SchemaField("delivered_date", "DATETIME"),
        bigquery.SchemaField("load_date",      "DATE"),
    ]

    job = bq.load_table_from_dataframe(
        df, temp_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            schema=date_schema))
    job.result()
    log(f"  Loaded to temp table: {len(df):,} rows")

    bq.query(f"""
    MERGE `{table_ref}` AS target
    USING (
      SELECT * REPLACE (
        CAST(invoice_date   AS DATETIME) AS invoice_date,
        CAST(order_date     AS DATETIME) AS order_date,
        CAST(shipment_date  AS DATETIME) AS shipment_date,
        CAST(delivered_date AS DATETIME) AS delivered_date
      )
      FROM `{temp_table}`
    ) AS source
    ON target.channel_sub_order_id = source.channel_sub_order_id
    WHEN MATCHED THEN UPDATE SET
      target.invoice_id               = source.invoice_id,
      target.invoice_date             = source.invoice_date,
      target.order_date               = source.order_date,
      target.shipment_date            = source.shipment_date,
      target.delivered_date           = source.delivered_date,
      target.warehouse                = source.warehouse,
      target.channel                  = source.channel,
      target.company                  = source.company,
      target.buyer_name               = source.buyer_name,
      target.buyer_address1           = source.buyer_address1,
      target.buyer_address2           = source.buyer_address2,
      target.buyer_city               = source.buyer_city,
      target.buyer_state              = source.buyer_state,
      target.buyer_pincode            = source.buyer_pincode,
      target.buyer_phone              = source.buyer_phone,
      target.buyer_email              = source.buyer_email,
      target.billing_name             = source.billing_name,
      target.billing_address1         = source.billing_address1,
      target.billing_address2         = source.billing_address2,
      target.billing_city             = source.billing_city,
      target.billing_state            = source.billing_state,
      target.billing_pincode          = source.billing_pincode,
      target.billing_phone            = source.billing_phone,
      target.billing_email            = source.billing_email,
      target.shipping_company         = source.shipping_company,
      target.shipment_tracker         = source.shipment_tracker,
      target.order_type               = source.order_type,
      target.channel_order_id         = source.channel_order_id,
      target.sku_code                 = source.sku_code,
      target.qty                      = source.qty,
      target.selling_price_per_item   = source.selling_price_per_item,
      target.shipping_charge_per_item = source.shipping_charge_per_item,
      target.promo_discounts          = source.promo_discounts,
      target.gift_wrap_charges        = source.gift_wrap_charges,
      target.transaction_charges      = source.transaction_charges,
      target.invoice_amount           = source.invoice_amount,
      target.currency_code            = source.currency_code,
      target.tax_rate                 = source.tax_rate,
      target.tax_amount               = source.tax_amount,
      target.igst_rate                = source.igst_rate,
      target.igst_amount              = source.igst_amount,
      target.cgst_rate                = source.cgst_rate,
      target.cgst_amount              = source.cgst_amount,
      target.sgst_rate                = source.sgst_rate,
      target.sgst_amount              = source.sgst_amount,
      target.status                   = source.status,
      target.settlement_amount        = source.settlement_amount,
      target.gst_name                 = source.gst_name,
      target.gst_number               = source.gst_number,
      target.pack_log                 = source.pack_log,
      target.sku_upc                  = source.sku_upc,
      target.listing_sku              = source.listing_sku,
      target.po_number                = source.po_number,
      target.load_date                = source.load_date
    WHEN NOT MATCHED THEN INSERT ROW
    """).result()

    bq.delete_table(temp_table, not_found_ok=True)

    after_count = bq.query(
        f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
    ).to_dataframe()["cnt"][0]

    inserted = int(after_count - before_count)
    updated  = len(df) - inserted
    log(f"  UPSERT complete!")
    log(f"  BQ Before : {before_count:,}")
    log(f"  BQ After  : {after_count:,}")
    log(f"  Inserted  : {inserted:,}")
    log(f"  Updated   : {updated:,}")
    return inserted, updated


# ============================================================
# 8. OPEN-STATUS REFRESH  ← NEW
#    Runs after the 16-day backfill.
#    Finds all order_dates outside the 16-day window that still
#    have open statuses, re-fetches them from OMS, and updates
#    only the 8 status-related fields (non-destructive MERGE).
# ============================================================
def run_open_status_refresh():
    log("\n" + "="*60)
    log("  STEP 3: Open-Status Refresh")
    log("="*60)

    bq        = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    today       = datetime.now(IST).date()
    cutoff_date = today - timedelta(days=FETCH_DAYS)
    status_list = ", ".join(f"'{s}'" for s in OPEN_STATUSES)

    # Get distinct order dates with open statuses outside the rolling window
    query = f"""
        SELECT
            DATE(order_date) AS order_date_only,
            COUNT(*)         AS cnt
        FROM `{table_ref}`
        WHERE status IN ({status_list})
          AND order_date IS NOT NULL
          AND DATE(order_date) < DATE('{cutoff_date.isoformat()}')
        GROUP BY order_date_only
        ORDER BY order_date_only ASC
        LIMIT 30
    """

    log("  Querying BQ for open-status order dates outside 16-day window...")
    open_dates = list(bq.query(query).result())

    if not open_dates:
        log("  No open-status orders outside the 16-day window. Nothing to refresh.")
        return {"dates_processed": 0, "total_fetched": 0, "total_updated": 0, "errors": []}

    log(f"  Found {len(open_dates)} date(s) with open-status orders:")
    for r in open_dates:
        log(f"     {r.order_date_only}  ->  {r.cnt:,} open orders")

    total_fetched = 0
    total_updated = 0
    errors        = []

    for r in open_dates:
        order_date = r.order_date_only   # datetime.date
        date_str   = order_date.strftime("%d-%m-%Y")
        log(f"\n  Refreshing {date_str} ({r.cnt:,} open orders)...")

        try:
            ts_s, ts_e = date_to_ist_timestamps(order_date)
            raw_orders = fetch_day(ts_s, ts_e)

            if not raw_orders:
                log(f"     API returned 0 orders for {date_str} — skipping.")
                continue

            total_fetched += len(raw_orders)

            # Reuse existing process() pipeline
            df = process(raw_orders)
            if df.empty:
                log(f"     No valid rows after processing for {date_str}")
                continue

            # Build slim dataframe: only key + STATUS_UPDATE_FIELDS
            slim_cols = ["channel_sub_order_id"] + [
                f for f in STATUS_UPDATE_FIELDS if f in df.columns
            ] + ["load_date"]
            slim_df = df[slim_cols].copy()
            slim_df["load_date"] = datetime.now(IST).date()

            # Load slim dataframe into a temp table
            temp_table  = f"{PROJECT_ID}.{DATASET_ID}.status_temp_{uuid.uuid4().hex[:8]}"
            date_schema = []
            if "shipment_date"  in slim_df.columns:
                date_schema.append(bigquery.SchemaField("shipment_date",  "DATETIME"))
            if "delivered_date" in slim_df.columns:
                date_schema.append(bigquery.SchemaField("delivered_date", "DATETIME"))

            job = bq.load_table_from_dataframe(
                slim_df, temp_table,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=True,
                    schema=date_schema))
            job.result()

            # Build the UPDATE SET clause for only the 8 fields + load_date
            update_fields = [f for f in STATUS_UPDATE_FIELDS if f in slim_df.columns] + ["load_date"]
            update_set    = ",\n              ".join(
                f"target.{f} = source.{f}" for f in update_fields
            )

            # Status-only MERGE — does NOT touch any other columns
            merge_sql = f"""
            MERGE `{table_ref}` AS target
            USING (
              SELECT * REPLACE (
                CAST(shipment_date  AS DATETIME) AS shipment_date,
                CAST(delivered_date AS DATETIME) AS delivered_date
              )
              FROM `{temp_table}`
            ) AS source
            ON target.channel_sub_order_id = source.channel_sub_order_id
            WHEN MATCHED THEN UPDATE SET
              {update_set}
            """
            bq.query(merge_sql).result()
            bq.delete_table(temp_table, not_found_ok=True)

            total_updated += len(slim_df)
            log(f"     {len(slim_df):,} rows status-updated for {date_str}")

        except Exception as e:
            err_msg = f"{date_str}: {e}"
            log(f"     ERROR: {err_msg}")
            errors.append(err_msg)

        time.sleep(DAY_SLEEP)

    log(f"\n  Open-Status Refresh complete!")
    log(f"  Dates processed : {len(open_dates)}")
    log(f"  Total fetched   : {total_fetched:,}")
    log(f"  Total updated   : {total_updated:,}")
    log(f"  Errors          : {len(errors)}")

    return {
        "dates_processed": len(open_dates),
        "total_fetched":   total_fetched,
        "total_updated":   total_updated,
        "errors":          errors,
    }


# ============================================================
# 9. MAIN
# ============================================================
if __name__ == "__main__":
    mode       = sys.argv[1] if len(sys.argv) > 1 else "daily"
    start_time = datetime.now(IST)
    t_grand    = time.time()

    log("="*60)

    if mode == "daily":
        # ── DAILY MODE: fetch yesterday only (unchanged) ──
        log("  OMS 2AM Daily Sync — Yesterday -> BigQuery")
        days       = get_yesterday()
        start_date = days[0][2]
        end_date   = days[-1][2]
        log(f"  Started : {start_time.strftime('%d-%m-%Y %H:%M:%S IST')}")
        log(f"  Days    : {len(days)}")
        log("="*60)

        all_raw = []
        try:
            for idx, (ds, de, label) in enumerate(days, 1):
                log(f"\n  Day {idx}/{len(days)} — {label}")
                t0     = time.time()
                orders = fetch_day(ds, de)
                all_raw.extend(orders)
                log(f"     {len(orders):,} orders in {time.time()-t0:.0f}s | "
                    f"Total: {len(all_raw):,}")

            elapsed = time.time() - t_grand
            log(f"\n  Fetch done: {len(all_raw):,} records in "
                f"{int(elapsed//60)}m {int(elapsed%60)}s")

            if not all_raw:
                log("  No data found.")
                send_email_notification(mode, start_date, end_date, 0, 0, 0, elapsed)
            else:
                df = process(all_raw)
                inserted, updated = save_to_bigquery(df)
                elapsed = time.time() - t_grand
                log(f"\n  DONE at {datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}")
                send_email_notification(mode, start_date, end_date,
                                        len(all_raw), inserted, updated, elapsed)

        except Exception as e:
            elapsed = time.time() - t_grand
            log(f"\n  CRITICAL ERROR: {e}")
            send_email_notification(mode, start_date, end_date,
                                    len(all_raw) if all_raw else 0,
                                    0, 0, elapsed, is_error=True)

    else:
        # ── BACKFILL MODE ──
        # Step 1+2 : last 16 days, one day at a time (unchanged)
        # Step 3   : open-status refresh for older orders  (NEW)

        log("  OMS 11:59PM Backfill — Last 16 Days + Open-Status Refresh")
        days       = list(get_last_16_days())
        start_date = days[0][2]
        end_date   = days[-1][2]
        log(f"  Started : {start_time.strftime('%d-%m-%Y %H:%M:%S IST')}")
        log(f"  Days    : {len(days)}")
        log("="*60)

        total_inserted         = 0
        total_updated          = 0
        total_raw              = 0
        status_refresh_summary = None

        try:
            # Steps 1 & 2: rolling 16-day sync
            for idx, (ds, de, label) in enumerate(days, 1):
                log(f"\n  Day {idx}/{len(days)} — {label}")
                t0     = time.time()
                orders = fetch_day(ds, de)
                log(f"     {len(orders):,} orders in {time.time()-t0:.0f}s")

                if not orders:
                    log(f"     No data for {label} — skipping.")
                    if idx < len(days): time.sleep(DAY_SLEEP)
                    continue

                total_raw += len(orders)
                df = process(orders)
                ins, upd = save_to_bigquery(df)
                total_inserted += ins
                total_updated  += upd
                log(f"     {ins:,} inserted | {upd:,} updated")

                if idx < len(days): time.sleep(DAY_SLEEP)

            elapsed = time.time() - t_grand
            log(f"\n  16-DAY SYNC DONE in {int(elapsed//60)}m {int(elapsed%60)}s")
            log(f"  Total Raw     : {total_raw:,}")
            log(f"  Total Inserted: {total_inserted:,}")
            log(f"  Total Updated : {total_updated:,}")

            # Step 3: open-status refresh
            status_refresh_summary = run_open_status_refresh()

            elapsed = time.time() - t_grand
            log(f"\n  BACKFILL DONE at {datetime.now(IST).strftime('%d-%m-%Y %H:%M:%S IST')}")

            send_email_notification(mode, start_date, end_date,
                                    total_raw, total_inserted, total_updated,
                                    elapsed,
                                    status_refresh_summary=status_refresh_summary)

        except Exception as e:
            elapsed = time.time() - t_grand
            log(f"\n  CRITICAL ERROR: {e}")
            send_email_notification(mode, start_date, end_date,
                                    total_raw, 0, 0, elapsed,
                                    is_error=True,
                                    status_refresh_summary=status_refresh_summary)
