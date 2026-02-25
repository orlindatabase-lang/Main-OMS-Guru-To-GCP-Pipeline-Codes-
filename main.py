import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import functions_framework

PROJECT_ID = "orlinappareldataset"
DATASET_ID = "oms_raw"
TABLE_ID = "orders"
URL = "https://client.omsguru.com/order_api/orders"
HEADERS = {
    "Accept": "application/json",
    "oms-cid": "34455",
    "Authorization": "K5HVJrC1jDdR3BYUMlobAG42fs9FOzwt"
}
FETCH_DAYS = 16
LIMIT = 100
RETRY_ATTEMPTS = 5
THROTTLE_WAIT = 60
SEND_EMAIL = True
EMAIL_FROM = "sanket.orlin@gmail.com"
EMAIL_PASSWORD = "ouzw ixvb cwaa nwzr"
EMAIL_TO = ["orlindatabase@gmail.com", "alpeshorlin@gmail.com"]

def send_email(subject, body, success=True):
    if not SEND_EMAIL:
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)
        msg["Subject"] = subject
        color = "#d4edda" if success else "#f8d7da"
        title = "✅ OMS Sync Success" if success else "❌ OMS Sync Failed"
        html = f"""<html><body style="font-family: Arial;"><div style="background:{color}; padding:20px;"><h2>{title}</h2><p>{body}</p><hr><small>Automated OMS Sync</small></div></body></html>"""
        msg.attach(MIMEText(html, "html"))
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"Email error: {e}")

def fetch_orders(start_ts, end_ts):
    all_orders = []
    last_id = 0
    previous_last_id = -1
    page = 1
    while True:
        payload = {"start_order_date": start_ts, "end_order_date": end_ts, "last_id": last_id, "limit": LIMIT}
        response = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = requests.post(URL, headers=HEADERS, data=payload, timeout=60)
                if response.status_code == 200:
                    json_resp = response.json()
                    if json_resp.get("error") == -4:
                        print(f"    Throttled - waiting {THROTTLE_WAIT}s...")
                        time.sleep(THROTTLE_WAIT)
                        continue
                    break
                elif response.status_code == 429:
                    print(f"    HTTP 429 - waiting {THROTTLE_WAIT * (attempt + 1)}s...")
                    time.sleep(THROTTLE_WAIT * (attempt + 1))
                else:
                    time.sleep(10)
            except Exception as ex:
                print(f"    Request error: {ex}")
                time.sleep(10)
        if response is None or response.status_code != 200:
            raise Exception("OMS API failed after retries")
        json_resp = response.json()
        if json_resp.get("error") not in [0, None, ""]:
            print(f"    API error: {json_resp.get('message')}")
            break
        data = json_resp.get("data", [])
        if not data:
            break
        all_orders.extend(data)
        previous_last_id = last_id
        last_id = data[-1].get("last_id", 0)
        if last_id == previous_last_id:
            break
        print(f"    Page {page}: Total fetched: {len(all_orders)}")
        page += 1
        time.sleep(2)
        if len(data) < LIMIT:
            break
    return all_orders

def safe_int(val):
    try:
        if val is None or str(val).strip() == '':
            return None
        return int(float(str(val)))
    except:
        return None

def safe_str(val):
    try:
        if val is None:
            return None
        return str(val).strip()
    except:
        return None

def upsert_to_bigquery(df, bq):
    if df.empty:
        return 0
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    temp_table = f"{PROJECT_ID}.{DATASET_ID}.orders_temp_upsert"

    # Fix INT64 columns
    int_cols = ["invoice_date", "order_date", "shipment_date", "delivered_date",
                "billing_name", "billing_address1", "billing_address2",
                "billing_phone", "billing_email"]
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_int)

    # Fix STRING columns - convert any numeric values to strings
    str_cols = ["billing_city", "billing_state", "billing_pincode", 
                "buyer_city", "buyer_state", "buyer_pincode",
                "buyer_address1", "buyer_address2", "buyer_name"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_str)

    job = bq.load_table_from_dataframe(df, temp_table, job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True))
    job.result()
    print(f"    Temp table loaded: {len(df)} records")

    merge_query = f"""
    MERGE `{table_ref}` AS target
    USING `{temp_table}` AS source
    ON target.channel_sub_order_id = source.channel_sub_order_id
    WHEN MATCHED THEN
        UPDATE SET
            status = source.status,
            shipment_date = source.shipment_date,
            delivered_date = source.delivered_date,
            shipment_tracker = source.shipment_tracker,
            settlement_amount = source.settlement_amount,
            invoice_amount = source.invoice_amount,
            load_date = source.load_date
    WHEN NOT MATCHED THEN
        INSERT ROW
    """
    bq.query(merge_query).result()
    bq.delete_table(temp_table, not_found_ok=True)
    return len(df)

def sync_oms():
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    start_date = (now - timedelta(days=FETCH_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = now.replace(hour=23, minute=59, second=59, microsecond=0)
    START_TS = int(start_date.timestamp())
    END_TS = int(end_date.timestamp())
    print(f"OMS SYNC: {start_date.date()} to {end_date.date()}")
    try:
        orders = fetch_orders(START_TS, END_TS)
        if not orders:
            send_email("OMS Sync No Data", f"No orders for {start_date.date()} to {end_date.date()}", success=True)
            return "No data"
        df = pd.DataFrame(orders)
        print(f"Total fetched: {len(df)}")
        if "order_items" in df.columns:
            df = df.explode("order_items").reset_index(drop=True)
            items = pd.json_normalize(df["order_items"])
            df = pd.concat([df.drop(columns=["order_items"]), items], axis=1)
        if "channel_sub_order_id" not in df.columns:
            raise Exception("channel_sub_order_id missing")
        df["channel_sub_order_id"] = df["channel_sub_order_id"].astype(str).str.strip()
        df = df[df["channel_sub_order_id"].notna() & (df["channel_sub_order_id"] != "")]
        before = len(df)
        df = df.drop_duplicates(subset=["channel_sub_order_id"], keep="last")
        print(f"Duplicates removed: {before - len(df)}, Unique: {len(df)}")
        if "invoice_date" in df.columns:
            def convert_date(x):
                try:
                    val = int(x)
                    if val == 0:
                        return now.date()
                    if len(str(val)) > 10:
                        return datetime.fromtimestamp(val / 1e9, tz=IST).date()
                    return datetime.fromtimestamp(val, tz=IST).date()
                except:
                    return now.date()
            df["load_date"] = df["invoice_date"].apply(convert_date)
        else:
            df["load_date"] = now.date()
        bq = bigquery.Client(project=PROJECT_ID)
        upsert_to_bigquery(df, bq)
        count_query = f"SELECT COUNT(DISTINCT channel_sub_order_id) as total FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
        total = list(bq.query(count_query).result())[0].total
        print(f"SYNC COMPLETE - Total in BigQuery: {total:,}")
        send_email("OMS Sync Successful", f"Sync Time: {now}<br>Orders synced: {len(df)}<br>Total in BigQuery: {total:,}", success=True)
        return f"Success: {len(df)} orders synced"
    except Exception as e:
        print(f"SYNC FAILED: {e}")
        send_email("OMS Sync Failed", str(e), success=False)
        raise

if __name__ == "__main__":
    sync_oms()

@functions_framework.http
def sync_oms_data(request):
    result = sync_oms()
    return str(result), 200
