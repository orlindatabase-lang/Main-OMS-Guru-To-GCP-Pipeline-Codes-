import pandas as pd
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Email Config
SEND_EMAIL = True
EMAIL_FROM = "sanket.orlin@gmail.com"
EMAIL_PASSWORD = "ouzw ixvb cwaa nwzr"
EMAIL_TO = ["orlindatabase@gmail.com", "alpeshorlin@gmail.com"]

def send_report_email(subject, body):
    """Send verification report email"""
    if not SEND_EMAIL:
        return
    
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_FROM
        msg['To'] = ", ".join(EMAIL_TO)
        msg['Subject'] = subject
        
        html_body = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <div style="background-color: #e7f3ff; padding: 20px; border-left: 5px solid #0066cc;">
            <h2 style="color: #0066cc;">📊 Weekly Data Verification Report</h2>
            {body}
            <hr>
            <small>Automated report from OMS Daily Sync</small>
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


def verify_weekly_data():
    """Verify last 7 days of data"""
    
    PROJECT_ID = "orlinappareldataset"
    client = bigquery.Client(project=PROJECT_ID)
    
    # Last 7 days
    end_date = datetime.now(timezone.utc) - timedelta(days=1)
    start_date = end_date - timedelta(days=7)
    
    print("=" * 60)
    print("WEEKLY DATA VERIFICATION")
    print(f"Date Range: {start_date.date()} to {end_date.date()}")
    print("=" * 60)
    
    # Query daily counts
    query = f"""
    SELECT 
        load_date,
        COUNT(*) as total_records,
        COUNT(DISTINCT channel_sub_order_id) as unique_orders,
        COUNTIF(status = 'Delivered') as delivered,
        COUNTIF(status LIKE '%Return%') as returns,
        COUNTIF(status LIKE '%Cancel%') as cancelled
    FROM `{PROJECT_ID}.oms_raw.orders`
    WHERE load_date >= '{start_date.date()}' AND load_date <= '{end_date.date()}'
    GROUP BY load_date
    ORDER BY load_date
    """
    
    result = client.query(query).to_dataframe()
    
    print("\nDaily Order Summary:")
    print(result.to_string(index=False))
    
    # Calculate statistics
    avg_orders = result['unique_orders'].mean()
    min_orders = result['unique_orders'].min()
    max_orders = result['unique_orders'].max()
    total_orders = result['unique_orders'].sum()
    
    print(f"\nStatistics:")
    print(f"  Total Orders: {total_orders:,}")
    print(f"  Daily Average: {avg_orders:,.0f}")
    print(f"  Min Day: {min_orders:,}")
    print(f"  Max Day: {max_orders:,}")
    
    # Check for anomalies (days with less than 50% of average)
    low_days = result[result['unique_orders'] < avg_orders * 0.5]
    
    # Check for missing days
    expected_days = set()
    current = start_date
    while current <= end_date:
        expected_days.add(current.date())
        current += timedelta(days=1)
    
    actual_days = set(result['load_date'].tolist())
    missing_days = expected_days - actual_days
    
    # Build report
    issues = []
    
    if len(missing_days) > 0:
        issues.append(f"⚠️ Missing days: {', '.join(str(d) for d in sorted(missing_days))}")
    
    if len(low_days) > 0:
        for _, row in low_days.iterrows():
            issues.append(f"⚠️ Low orders on {row['load_date']}: {row['unique_orders']} (avg: {avg_orders:.0f})")
    
    # Print results
    if issues:
        print("\n⚠️ ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue}")
        status = "⚠️ Issues Found"
    else:
        print("\n✅ No issues found!")
        status = "✅ All Good"
    
    # Send email report
    table_html = result.to_html(index=False)
    
    email_body = f"""
    <p><strong>Date Range:</strong> {start_date.date()} to {end_date.date()}</p>
    <p><strong>Status:</strong> {status}</p>
    
    <h3>Daily Summary:</h3>
    {table_html}
    
    <h3>Statistics:</h3>
    <ul>
        <li>Total Orders: {total_orders:,}</li>
        <li>Daily Average: {avg_orders:,.0f}</li>
        <li>Min Day: {min_orders:,}</li>
        <li>Max Day: {max_orders:,}</li>
    </ul>
    """
    
    if issues:
        email_body += "<h3>⚠️ Issues:</h3><ul>"
        for issue in issues:
            email_body += f"<li>{issue}</li>"
        email_body += "</ul>"
    
    send_report_email(
        subject=f"📊 OMS Weekly Report - {status}",
        body=email_body
    )
    
    return result


if __name__ == "__main__":
    verify_weekly_data()