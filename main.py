import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time

# ========================================
# EMAIL CONFIGURATION
# ========================================
SEND_EMAIL = True
EMAIL_FROM = "sanket.orlin@gmail.com"
EMAIL_PASSWORD = "ouzw ixvb cwaa nwzr"
EMAIL_TO = ["orlindatabase@gmail.com", "alpeshorlin@gmail.com"]

# ========================================
# SYNC CONFIGURATION
# ========================================
DAYS_TO_FETCH = 20  # Fetch last 2 days (today + yesterday) for cross-check
RETRY_ATTEMPTS = 5  # Number of retry attempts for API calls
THROTTLE_WAIT = 60  # Seconds to wait when API is throttled


def send_email(subject, body, is_success=True, is_warning=False):
    """Send email notification"""
    if not SEND_EMAIL:
        return

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = ", ".join(EMAIL_TO)
        msg['Subject'] = subject

        if is_success and not is_warning:
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
            <div style="background-color: #d4edda; padding: 20px; border-left: 5px solid #28a745;">
                <h2 style="color: #155724;">✅ OMS Sync Successful</h2>
                <p style="color: #155724;">{body}</p>
                <hr>
                <small>Automated message from OMS Daily Sync</small>
            </div>
            </body>
            </html>
            """
        elif is_warning:
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
            <div style="background-color: #fff3cd; padding: 20px; border-left: 5px solid #ffc107;">
                <h2 style="color: #856404;">⚠️ OMS Sync Warning</h2>
                <p style="color: #856404;">{body}</p>
                <hr>
                <small>Automated message from OMS Daily Sync</small>
            </div>
            </body>
            </html>
            """
        else:
            html_body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
            <div style="background-color: #f8d7da; padding: 20px; border-left: 5px solid #dc3545;">
                <h2 style="color: #721c24;">❌ OMS Sync Failed</h2>
                <p style="color: #721c24;">{body}</p>
                <hr>
                <small>Automated message from OMS Daily Sync</small>
            </div>
            </body>
            </html>
            """

        msg.attach(MIMEText(html_body, 'html'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"Email sent: {subject}")

    except Exception as e:
        print(f"Failed to send email: {str(e)}")


def fetch_orders_from_api(start_ts, end_ts):
    """Fetch orders from OMS Guru API with retry logic and throttle handling"""
    
    URL = "https://client.omsguru.com/order_api/orders"
    HEADERS = {
        "Accept": "application/json",
        "oms-cid": "34455",
        "Authorization": "ycCKJEB2F0AXSR6VsxqDOpUjbLfPiMnZ"
    }
    
    all_orders = []
    last_id = 0
    page = 0
    
    while True:
        payload = {
            "start_order_date": start_ts,
            "end_order_date": end_ts,
            "last_id": last_id,
            "limit": 100
        }
        
        response = None
        
        # Retry logic with throttle handling
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = requests.post(URL, headers=HEADERS, data=payload, timeout=60)
                
                if response.status_code == 200:
                    break
                elif response.status_code == 429:
                    # API throttled - wait and retry
                    wait_time = THROTTLE_WAIT * (attempt + 1)
                    print(f"\n  ⏳ API throttled. Waiting {wait_time} seconds (attempt {attempt + 1}/{RETRY_ATTEMPTS})...")
                    time.sleep(wait_time)
                else:
                    print(f"\n  Attempt {attempt + 1} failed: {response.status_code}")
                    time.sleep(10)
                    
            except Exception as e:
                print(f"\n  Attempt {attempt + 1} error: {str(e)}")
                time.sleep(10)
        
        # Check if all retries failed
        if response is None or response.status_code != 200:
            error_text = response.text if response else "No response"
            raise Exception(f"API failed after {RETRY_ATTEMPTS} attempts: {error_text}")
        
        data = response.json().get("data", [])
        
        if not data:
            break
        
        all_orders.extend(data)
        last_id = data[-1].get("last_id", 0)
        page += 1
        print(f"  Page {page}: {len(all_orders)} orders fetched...", end='\r')
        
        # Small delay between pages to avoid throttling
        time.sleep(1)
        
        if len(data) < 100:
            break
    
    print(f"\n  Total orders fetched: {len(all_orders)}")
    return all_orders


def get_existing_order_ids(bq_client, project_id, dataset_id, table_id, start_date, end_date):
    """Get existing channel_sub_order_ids from BigQuery for the date range"""
    try:
        query = f"""
        SELECT DISTINCT channel_sub_order_id
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE load_date >= '{start_date}' AND load_date <= '{end_date}'
        AND channel_sub_order_id IS NOT NULL
        """
        result = bq_client.query(query).to_dataframe()
        return set(result['channel_sub_order_id'].astype(str).unique())
    except Exception as e:
        if "Not found: Table" in str(e):
            return set()
        raise


def sync_oms_data():
    """Fetch OMS data and load to BigQuery with deduplication"""
    
    try:
        # BigQuery Config
        PROJECT_ID = "orlinappareldataset"
        DATASET_ID = "oms_raw"
        TABLE_ID = "orders"
        
        bq_client = bigquery.Client(project=PROJECT_ID)
        
        # ========================================
        # DATE RANGE: TODAY + YESTERDAY (2 days for cross-check)
        # ========================================
        # Use IST timezone (UTC+5:30) for India
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        
        # Today's date range (start of today to current time)
        today = now.replace(hour=23, minute=59, second=59, microsecond=0)
        yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Fetch from yesterday 00:00:00 to today 23:59:59 (2 days)
        start_date = yesterday
        end_date = today
        
        START_TS = int(start_date.timestamp())
        END_TS = int(end_date.timestamp())
        
        print("=" * 60)
        print(f"OMS DAILY SYNC (Same-Day Sync)")
        print(f"Current Time (IST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fetching: {start_date.date()} to {end_date.date()}")
        print(f"Configuration: {DAYS_TO_FETCH} days, {RETRY_ATTEMPTS} retries, {THROTTLE_WAIT}s throttle wait")
        print("=" * 60)
        
        # ========================================
        # FETCH DATA FROM API
        # ========================================
        print(f"\nStep 1: Fetching orders from OMS API...")
        all_orders = fetch_orders_from_api(START_TS, END_TS)
        
        if not all_orders:
            msg = f"No orders found for {start_date.date()} to {end_date.date()}"
            print(f"⚠️ {msg}")
            send_email(
                subject="⚠️ OMS Sync - No Data",
                body=f"Date Range: {start_date.date()} to {end_date.date()}<br>No orders found.",
                is_warning=True
            )
            return msg
        
        # ========================================
        # PROCESS DATA
        # ========================================
        print(f"\nStep 2: Processing {len(all_orders)} orders...")
        df = pd.DataFrame(all_orders)
        
        # Flatten order_items
        if "order_items" in df.columns:
            df = df.explode("order_items").reset_index(drop=True)
            items_df = pd.json_normalize(df["order_items"])
            df = pd.concat([df.drop(columns=["order_items"]), items_df], axis=1)
        
        # Convert date columns to numeric
        date_cols = ["order_date", "invoice_date", "shipment_date", "delivered_date"]
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        original_count = len(df)
        print(f"  Total records after flattening: {original_count}")
        
        # ========================================
        # SET LOAD_DATE BASED ON ACTUAL ORDER DATE
        # ========================================
        print(f"\nStep 3: Setting load_date based on actual order date...")
        
        # Convert order_date (Unix timestamp in seconds) to date in IST
        df["load_date"] = pd.to_datetime(
            df["order_date"],
            unit='s',
            errors='coerce'
        ).dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.date
        
        # For records with invalid order_date, use today's date as fallback
        fallback_date = now.date()
        df["load_date"] = df["load_date"].fillna(fallback_date)
        
        # Fix any dates that are too old (before 2020) or too far in future
        min_valid_date = pd.Timestamp('2020-01-01').date()
        max_valid_date = (now + timedelta(days=1)).date()
        
        invalid_dates_mask = (df["load_date"] < min_valid_date) | (df["load_date"] > max_valid_date)
        invalid_dates_count = invalid_dates_mask.sum()
        
        if invalid_dates_count > 0:
            print(f"  Fixed {invalid_dates_count} records with invalid order_date")
            df.loc[invalid_dates_mask, "load_date"] = fallback_date
        
        # Show date distribution
        date_counts = df["load_date"].value_counts().sort_index()
        print(f"  Orders by load_date:")
        for date, count in date_counts.items():
            print(f"    {date}: {count}")
        
        # ========================================
        # DATA QUALITY CHECKS
        # ========================================
        print(f"\nStep 4: Data Quality Checks...")
        
        # Check for channel_sub_order_id
        if 'channel_sub_order_id' not in df.columns:
            raise Exception("channel_sub_order_id column not found in API response")
        
        # Remove records with NULL channel_sub_order_id
        null_sub_order_count = df['channel_sub_order_id'].isna().sum()
        empty_sub_order_count = (df['channel_sub_order_id'] == '').sum()
        
        if null_sub_order_count > 0 or empty_sub_order_count > 0:
            print(f"  Removing {null_sub_order_count + empty_sub_order_count} records with NULL/empty channel_sub_order_id")
            df = df[df['channel_sub_order_id'].notna() & (df['channel_sub_order_id'] != '')]
        
        # Remove records with NULL invoice_id
        null_invoice_count = 0
        if 'invoice_id' in df.columns:
            null_invoice_count = df['invoice_id'].isna().sum()
            if null_invoice_count > 0:
                print(f"  Removing {null_invoice_count} records with NULL invoice_id")
                df = df[df['invoice_id'].notna() & (df['invoice_id'] != '')]
        
        # ========================================
        # DEDUPLICATION (within fetched data)
        # ========================================
        print(f"\nStep 5: Deduplication...")
        
        before_dedup = len(df)
        df['channel_sub_order_id'] = df['channel_sub_order_id'].astype(str).str.strip()
        df = df.drop_duplicates(subset=['channel_sub_order_id'], keep='last')
        after_dedup = len(df)
        
        print(f"  Duplicates removed (within fetch): {before_dedup - after_dedup}")
        
        # ========================================
        # CHECK EXISTING DATA IN BIGQUERY
        # ========================================
        print(f"\nStep 6: Checking existing data in BigQuery...")
        
        # Get unique load_dates in the data
        unique_dates = df["load_date"].unique()
        min_date = min(unique_dates)
        max_date = max(unique_dates)
        
        existing_ids = get_existing_order_ids(
            bq_client, PROJECT_ID, DATASET_ID, TABLE_ID,
            min_date, max_date
        )
        
        print(f"  Existing orders in BigQuery for date range: {len(existing_ids)}")
        
        # Filter out orders that already exist
        df_new = df[~df['channel_sub_order_id'].isin(existing_ids)]
        
        new_orders_count = len(df_new)
        skipped_count = len(df) - new_orders_count
        
        print(f"  New orders to load: {new_orders_count}")
        print(f"  Skipped (already exists): {skipped_count}")
        
        if new_orders_count == 0:
            msg = f"No new orders to load. All {len(df)} orders already exist in BigQuery."
            print(f"✅ {msg}")
            send_email(
                subject="✅ OMS Sync - No New Orders",
                body=f"""
                <strong>Sync Time (IST):</strong> {now.strftime('%Y-%m-%d %H:%M:%S')}<br>
                <strong>Date Range:</strong> {start_date.date()} to {end_date.date()}<br>
                <strong>Orders Fetched:</strong> {len(df)}<br>
                <strong>New Orders:</strong> 0<br>
                <strong>Status:</strong> All orders already in BigQuery
                """,
                is_success=True
            )
            return msg
        
        # ========================================
        # LOAD TO BIGQUERY
        # ========================================
        print(f"\nStep 7: Loading {new_orders_count} new orders to BigQuery...")
        
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        
        load_job = bq_client.load_table_from_dataframe(df_new, table_ref, job_config=job_config)
        load_job.result()
        
        # ========================================
        # SUCCESS SUMMARY
        # ========================================
        unique_orders = df_new['channel_sub_order_id'].nunique()
        
        # Get date distribution for new orders
        new_date_counts = df_new["load_date"].value_counts().sort_index()
        date_summary = ", ".join([f"{date}: {count}" for date, count in new_date_counts.items()])
        
        print("\n" + "=" * 60)
        print("✅ SYNC COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"  Sync Time (IST): {now.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Date Range Fetched: {start_date.date()} to {end_date.date()}")
        print(f"  Orders Fetched from API: {original_count}")
        print(f"  Invalid Dates Fixed: {invalid_dates_count}")
        print(f"  NULL/Empty Removed: {null_sub_order_count + empty_sub_order_count + null_invoice_count}")
        print(f"  Duplicates Removed: {before_dedup - after_dedup}")
        print(f"  Already in BigQuery: {skipped_count}")
        print(f"  New Orders Loaded: {new_orders_count}")
        print(f"  Orders by Date: {date_summary}")
        print("=" * 60)
        
        # Send success email
        send_email(
            subject="✅ OMS Daily Sync Successful",
            body=f"""
            <strong>Sync Time (IST):</strong> {now.strftime('%Y-%m-%d %H:%M:%S')}<br>
            <strong>Date Range Fetched:</strong> {start_date.date()} to {end_date.date()}<br>
            <strong>Orders Fetched:</strong> {original_count}<br>
            <strong>Invalid Dates Fixed:</strong> {invalid_dates_count}<br>
            <strong>NULL/Empty Removed:</strong> {null_sub_order_count + empty_sub_order_count + null_invoice_count}<br>
            <strong>Duplicates Removed:</strong> {before_dedup - after_dedup}<br>
            <strong>Already in BigQuery:</strong> {skipped_count}<br>
            <strong>New Orders Loaded:</strong> {new_orders_count}<br>
            <strong>Orders by Date:</strong> {date_summary}<br>
            <strong>Table:</strong> {table_ref}
            """,
            is_success=True
        )
        
        return f"Success: {new_orders_count} new orders loaded"
    
    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        print(f"\n❌ {error_msg}")
        
        send_email(
            subject="❌ OMS Sync Failed",
            body=f"<strong>Error:</strong> {error_msg}",
            is_success=False
        )
        
        return error_msg


if __name__ == "__main__":
    result = sync_oms_data()

    print(f"\nFinal Result: {result}")
