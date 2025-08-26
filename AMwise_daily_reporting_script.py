#!/usr/bin/env python3
"""
Daily Campaign Reporting Script
Extracts campaign analytics from Supabase databases and writes to Google Sheets
"""

import logging
import psycopg2
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== EMBEDDED CREDENTIALS AND CONFIGURATION ==========

# Database connection strings
REPORTING_DB_URL = "postgresql://postgres.auzoezucrrhrtmaucbbg:SB0dailyreporting@aws-1-us-east-2.pooler.supabase.com:6543/postgres"
CLIENTS_DB_URL = "postgresql://postgres.onnbdclahsxoqdfgdsbm:clentsDB0pw@aws-1-ap-south-1.pooler.supabase.com:6543/postgres"

# Google Service Account credentials (embedded directly)
# NOTE: Replace this with your actual service account JSON credentials
service_account_info = {
  "type": "service_account",
  "project_id": "mythic-fire-420811",
  "private_key_id": "a4e3ef79c3051cafa8fa224dac69bf9336060361",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDiRKVIjU+eLfbi\nDHCkfI0O6toiRwoR0e+ynEyyCGxA8zN1SdmxMr6f2IMvjIQpQtWlgvdZf5XS2+Nm\nfJo4M0Wogl/nJhz31Y+Ku0KcPqJ5LsBWuTyB+KWGE2/briVNccZWmfhfEBDoBmbs\n19NC28qWlgVc68qZrcHJn3AHjaJ/HpFeJN1qMB8MR6JR/gXni1A1bJcC5OHhqgd0\nzM5P9cLY1VuqezhEWKWHermNd7uGHKnI3Zdmv/ERhAU1nhLAeemfT/5Rv8iH2g4X\nz9G5IXp65QNrGLCd4ggamGhLCErLRS7EdWhh++aVBpDHgUVjbaQ+YqCB4eWZeu1b\nsswxETOFAgMBAAECggEADbzmEyZfSebtCTMA/ipWGdcuXivq2Nj4ASPGb/3HwtbG\no6L+UxOraW2eI3DYOmAa3tUkOvxz2+JVZ3d+bpXwWgeWa+SkwshmPMxbV5IMT9LD\n7fq1CYglAU5kpCrUjCPxtfgxvBCFPO0x6m89M19AaOMGxCpIegsHy9Erc7id/909\nm90ML+x4LfjdQcHpszHA7FR3Tnh51Kqza64wdWD7av0cQ24RhRYy/5E6c2CbkRyw\n7VM7IpBeiN6R4Hdeg4FrjyG7nkKbFFTbOSDwwJK8r18uecOes7jkLaMfXBIKTx7M\nFIzmSjs1cK4kHKWD1NQLxqJgeIbvadhvlfA+1O5imQKBgQD26KLX8cqmuwsyhqUS\nvc+I18pYlIoLoXTyTTv4+4eF3nqZjVxo5gupJ9Mg2VKJLLdkizmTM6iREHdho3Tr\nML0uG8sEzU6wnPGx37/MDdRXveOMkZXJuqd2P/lWF7gFvphIZPIPBnPvY8dTfvHJ\nE9yxFsEiq+bwkzubUq3nG314+QKBgQDqmXN/4Kd49uZJRTotp22tcYrTzYnaUlZk\nfo6/4URR69xlYYPzOzZqE7DhmG26lsGFYhZ79B3MBTP8fKa1Vxfgf50YcTI1o8Q0\ne0MeSEWsEXCVRKhSMeEh5sL4lQaMRTRFs+Lgoe5rya/c6nBZIlyW38HYk+3N3O0e\nd4ncYdQd7QKBgQCNgo16SHkGECN9xM+tKx5b5plxJUjtG49EI+HgdICayATqJqu0\n70v1mf6WUBfOyNMfC/Bmnm/ZHF/flOg4t4lleMZlrSmRbZHUiVGKqM5vr0RQV0xK\n/vBlhIrpvdRZboAm1bwpwmAF7uDZyOLYhMqysEDnFzDX5vp9reg/kXDbOQKBgFnk\nYrVlR8a6FJOOyzQjK4uCLkfqQiA93Iy1Uc2Ea8FYNyNBsmXJEpii4uwOlD0i9xQ8\n+ZCVgbVjaQAeY2Ko9KU5QODUvwB+t/fEI3u/BbNhG1qW7EhShImQ+rR1pgSpn9X1\nj8GzSsBSj+h+jH4bBI9rPcPXKw/uz40VEOY5NiYhAoGAPMKrbR22knzNmAmYTM4/\n8yC9s0bKeRHsb4bXlX4+uvWYrnZpQYRRKttuf/8VlYoCoI6Bu8gNNIYQE1LNYzB1\nFYxgyb2xGMVLTc9gfgYYT758Zv5MjOcjTJtey5u28R5jFbEU0o8s6Bw+GVFcZ8rq\n2JEdTowimgk6GczNCZiXGOk=\n-----END PRIVATE KEY-----\n",
  "client_email": "active-clients-stats-am@mythic-fire-420811.iam.gserviceaccount.com",
  "client_id": "108943857688064451039",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/active-clients-stats-am%40mythic-fire-420811.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Account Manager to Google Sheet ID mapping
account_manager_sheets = {
    "Carl": "1Fx82mA0GjvmdwXIsvgKf92p_W133dniP82OGr1LoXIU",
    "Gaius": "1wbz2evFpJwfPhC2uhqqUrg-_ybh2waqV9Y5P3UG-nzs",
    "Ram Prakash": "15MJhaktorXvTwqEVZp8Om77ecEi1MSLMs1uFR0tv6iI",  # Updated to match DB
    "Dhanraj": "1uAYJnh1Y5c8L8sflOcULgI1Hw8eEjchY3GSs9tgAI0U",  # Mapped to Omesh's sheet
    "Benjie": "1ZEiwmSdFnlybhIBpi4lEh0Pe1AOZw087EXjFyU-fk1A"  # Can map to any available sheet
}

# Output column headers (must match exact order)
OUTPUT_COLUMNS = [
    "client_name", "id", "user_id", "created_at", "status", "name", "start_date", 
    "end_date", "sent_count", "unique_sent_count", "open_count", "unique_open_count",
    "click_count", "unique_click_count", "reply_count", "block_count", "total_count",
    "drafted_count", "bounce_count", "unsubscribed_count", "client_email",
    "ln_connection_req_pending_count", "ln_connection_req_accepted_count",
    "ln_connection_req_skipped_sent_msg_count", "positive_reply_count"
]

# ========== DATABASE FUNCTIONS ==========

def fetch_clients():
    """Fetch client assignments from clients database"""
    logger.info("Fetching client assignments...")
    
    try:
        conn = psycopg2.connect(CLIENTS_DB_URL)
        cursor = conn.cursor()
        
        query = """
        SELECT client_id, client_code, assigned_account_manager_name
        FROM public.clients
        WHERE assigned_account_manager_name IS NOT NULL
        """
        
        cursor.execute(query)
        clients = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(clients)} client assignments")
        return clients
        
    except Exception as e:
        logger.error(f"Error fetching clients: {e}")
        raise

def calculate_client_summary(client_campaigns, client_name):
    """Calculate summary totals for a specific client"""
    if not client_campaigns:
        return [0] * len(OUTPUT_COLUMNS)
    
    # Initialize summary with specific values
    summary = ["--"] * len(OUTPUT_COLUMNS)
    
    # Set summary row identifiers
    summary[0] = f"{client_name} - Summary"  # client_name
    summary[1] = "--"  # id
    summary[2] = "--"  # user_id
    summary[3] = "--"  # created_at
    summary[4] = "--"  # status
    summary[5] = "--"  # name
    summary[6] = "--"  # start_date
    summary[7] = "--"  # end_date
    
    # Sum numerical columns
    numerical_indices = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24]
    
    for idx in numerical_indices:
        summary[idx] = 0
    
    # Collect client emails
    client_emails = set()
    
    for row in client_campaigns:
        # Sum numerical values
        for idx in numerical_indices:
            try:
                summary[idx] += float(row[idx]) if row[idx] else 0
            except (ValueError, TypeError):
                continue
        
        # Collect client emails (if available)
        if row[20]:  # client_email column
            client_emails.add(row[20])
    
    # Set client emails (comma-separated)
    summary[20] = ",".join(sorted(client_emails)) if client_emails else ""
    
    return summary

def format_campaign_data_by_client(campaigns, client_mapping, am_name):
    """Format campaign data grouped by client with client summaries"""
    logger.info(f"Formatting data by client for AM: {am_name}")
    
    # Group campaigns by client
    client_campaigns = defaultdict(list)
    
    for campaign in campaigns:
        (campaign_date_key, campaign_id, parent_campaign_id, campaign_name, 
         client_name, status, start_date, end_date, total_sent, 
         new_leads_reached, replies_count, positive_reply, bounce_count) = campaign
        
        # Get client_id from mapping
        client_id = client_mapping.get(am_name, {}).get(client_name, 0)
        
        # Map available data to output format
        row_data = {
            "client_name": client_name or "",
            "id": campaign_id or 0,
            "user_id": 0,  # Not available
            "created_at": "",  # Not available
            "status": status or "",
            "name": campaign_name or "",
            "start_date": start_date.strftime('%Y-%m-%d') if start_date else "",
            "end_date": end_date.strftime('%Y-%m-%d') if end_date else "",
            "sent_count": total_sent or 0,
            "unique_sent_count": new_leads_reached or 0,
            "open_count": 0,  # Not available
            "unique_open_count": 0,  # Not available
            "click_count": 0,  # Not available
            "unique_click_count": 0,  # Not available
            "reply_count": replies_count or 0,
            "block_count": 0,  # Not available
            "total_count": 0,  # Not available
            "drafted_count": 0,  # Not available
            "bounce_count": bounce_count or 0,
            "unsubscribed_count": 0,  # Not available
            "client_email": "",  # Not available
            "ln_connection_req_pending_count": 0,  # Not available
            "ln_connection_req_accepted_count": 0,  # Not available
            "ln_connection_req_skipped_sent_msg_count": 0,  # Not available
            "positive_reply_count": positive_reply or 0
        }
        
        # Convert to ordered list matching OUTPUT_COLUMNS
        row = [row_data[col] for col in OUTPUT_COLUMNS]
        client_campaigns[client_name].append(row)
    
    # Build final output with client summaries
    formatted_rows = []
    
    for client_name, campaigns_list in client_campaigns.items():
        # Add client campaigns
        formatted_rows.extend(campaigns_list)
        
        # Add client summary row
        client_summary = calculate_client_summary(campaigns_list, client_name)
        formatted_rows.append(client_summary)
        
        # Add empty row for separation
        formatted_rows.append([""] * len(OUTPUT_COLUMNS))
    
    # Remove the last empty row if it exists
    if formatted_rows and all(cell == "" for cell in formatted_rows[-1]):
        formatted_rows.pop()
    
    logger.info(f"Formatted {len([row for client_list in client_campaigns.values() for row in client_list])} campaign rows + {len(client_campaigns)} summary rows for {am_name}")
    return formatted_rows

def discover_campaign_columns():
    """Discover available columns in campaign_reporting table"""
    logger.info("Discovering available columns in campaign_reporting table...")
    
    try:
        conn = psycopg2.connect(REPORTING_DB_URL)
        cursor = conn.cursor()
        
        query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = 'public' 
        AND table_name = 'campaign_reporting'
        ORDER BY ordinal_position
        """
        
        cursor.execute(query)
        columns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info("Available columns in campaign_reporting:")
        for col_name, col_type in columns:
            logger.info(f"  - {col_name}: {col_type}")
        
        return columns
        
    except Exception as e:
        logger.error(f"Error discovering columns: {e}")
        raise

def check_available_dates():
    """Check what dates are available in the campaign_reporting table"""
    logger.info("Checking available dates in campaign_reporting table...")
    
    try:
        conn = psycopg2.connect(REPORTING_DB_URL)
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT campaign_date_key, COUNT(*) as campaign_count
        FROM public.campaign_reporting
        GROUP BY campaign_date_key
        ORDER BY campaign_date_key DESC
        LIMIT 10
        """
        
        cursor.execute(query)
        dates = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info("Available dates in campaign_reporting (last 10):")
        for date_key, count in dates:
            logger.info(f"  - {date_key}: {count} campaigns")
        
        return dates
        
    except Exception as e:
        logger.error(f"Error checking available dates: {e}")
        raise

def fetch_campaigns(report_date):
    """Fetch campaign metrics for campaigns that started on the specified date"""
    # Format date as string (YYYY-MM-DD)
    date_str = report_date.strftime('%Y-%m-%d')
    logger.info(f"Fetching campaigns with start_date: {date_str}")
    
    try:
        conn = psycopg2.connect(REPORTING_DB_URL)
        cursor = conn.cursor()
        
        # Use start_date field for matching instead of campaign_date_key
        query = """
        SELECT 
            campaign_date_key,
            campaign_id,
            parent_campaign_id,
            campaign_name,
            client_name,
            status,
            start_date,
            end_date,
            total_sent,
            new_leads_reached,
            replies_count,
            positive_reply,
            bounce_count
        FROM public.campaign_reporting
        WHERE start_date = %s
        """
        
        cursor.execute(query, (date_str,))
        campaigns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Fetched {len(campaigns)} campaigns with start_date = {date_str}")
        return campaigns
        
    except Exception as e:
        logger.error(f"Error fetching campaigns: {e}")
        raise

def build_client_mapping(clients):
    """Build mapping: AM Name -> Client Name -> client_id"""
    logger.info("Building client mapping...")
    
    mapping = defaultdict(dict)
    
    for client_id, client_code, am_name in clients:
        if am_name and am_name.strip():
            # Use client_code as the client name for mapping
            mapping[am_name.strip()][client_code] = client_id
            logger.debug(f"Mapped client '{client_code}' (ID: {client_id}) to AM '{am_name.strip()}'")
    
    logger.info(f"Built mapping for {len(mapping)} Account Managers:")
    for am_name, clients_dict in mapping.items():
        logger.info(f"  - {am_name}: {len(clients_dict)} clients")
    
    return mapping

def group_campaigns_by_am(campaigns, client_mapping):
    """Group campaigns by Account Manager"""
    logger.info("Grouping campaigns by Account Manager...")
    
    am_campaigns = defaultdict(list)
    
    # Create reverse mapping: client_name -> AM
    client_to_am = {}
    for am_name, clients in client_mapping.items():
        for client_name, client_id in clients.items():
            client_to_am[client_name] = am_name
    
    logger.info("Available client mappings:")
    for client_name, am_name in client_to_am.items():
        logger.info(f"  - '{client_name}' -> '{am_name}'")
    
    unique_client_names_in_campaigns = set()
    
    for campaign in campaigns:
        (campaign_date_key, campaign_id, parent_campaign_id, campaign_name, 
         client_name, status, start_date, end_date, total_sent, 
         new_leads_reached, replies_count, positive_reply, bounce_count) = campaign
        
        unique_client_names_in_campaigns.add(client_name)
        
        # Find AM for this client
        am_name = client_to_am.get(client_name)
        if am_name:
            am_campaigns[am_name].append(campaign)
            logger.debug(f"Campaign '{campaign_name}' for client '{client_name}' assigned to AM '{am_name}'")
        else:
            logger.warning(f"No AM found for client: '{client_name}'")
    
    logger.info("Unique client names found in campaigns:")
    for client_name in sorted(unique_client_names_in_campaigns):
        logger.info(f"  - '{client_name}'")
    
    logger.info(f"Grouped campaigns for {len(am_campaigns)} Account Managers")
    for am_name, campaigns_list in am_campaigns.items():
        logger.info(f"  - {am_name}: {len(campaigns_list)} campaigns")
    
    return am_campaigns

def format_campaign_data(campaigns, client_mapping, am_name):
    """Format campaign data into required output format"""
    logger.info(f"Formatting data for AM: {am_name}")
    
    formatted_rows = []
    
    for campaign in campaigns:
        (campaign_date_key, campaign_id, parent_campaign_id, campaign_name, 
         client_name, status, start_date, end_date, total_sent, 
         new_leads_reached, replies_count, positive_reply, bounce_count) = campaign
        
        # Get client_id from mapping
        client_id = client_mapping.get(am_name, {}).get(client_name, 0)
        
        # Map available data to output format
        row_data = {
            "client_name": client_name or "",
            "id": campaign_id or 0,
            "user_id": 0,  # Not available
            "created_at": "",  # Not available
            "status": status or "",
            "name": campaign_name or "",
            "start_date": start_date.strftime('%Y-%m-%d') if start_date else "",
            "end_date": end_date.strftime('%Y-%m-%d') if end_date else "",
            "sent_count": total_sent or 0,
            "unique_sent_count": new_leads_reached or 0,
            "open_count": 0,  # Not available
            "unique_open_count": 0,  # Not available
            "click_count": 0,  # Not available
            "unique_click_count": 0,  # Not available
            "reply_count": replies_count or 0,
            "block_count": 0,  # Not available
            "total_count": 0,  # Not available
            "drafted_count": 0,  # Not available
            "bounce_count": bounce_count or 0,
            "unsubscribed_count": 0,  # Not available
            "client_email": "",  # Not available
            "ln_connection_req_pending_count": 0,  # Not available
            "ln_connection_req_accepted_count": 0,  # Not available
            "ln_connection_req_skipped_sent_msg_count": 0,  # Not available
            "positive_reply_count": positive_reply or 0
        }
        
        # Convert to ordered list matching OUTPUT_COLUMNS
        row = [row_data[col] for col in OUTPUT_COLUMNS]
        formatted_rows.append(row)
    
    logger.info(f"Formatted {len(formatted_rows)} rows for {am_name}")
    return formatted_rows

def calculate_summary(formatted_rows):
    """Calculate summary totals"""
    if not formatted_rows:
        return [0] * len(OUTPUT_COLUMNS)
    
    # Initialize summary with zeros
    summary = [0] * len(OUTPUT_COLUMNS)
    
    # Set summary row identifiers
    summary[0] = "TOTAL"  # client_name
    summary[4] = "SUMMARY"  # status
    summary[5] = "Summary Row"  # name
    
    # Sum numerical columns
    numerical_indices = [1, 2, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23, 24]
    
    for row in formatted_rows:
        for idx in numerical_indices:
            try:
                summary[idx] += float(row[idx]) if row[idx] else 0
            except (ValueError, TypeError):
                continue
    
    return summary

# ========== GOOGLE SHEETS FUNCTIONS ==========

def setup_google_sheets():
    """Setup Google Sheets client"""
    logger.info("Setting up Google Sheets client...")
    
    try:
        credentials = Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://spreadsheets.google.com/feeds',
                   'https://www.googleapis.com/auth/drive']
        )
        
        client = gspread.authorize(credentials)
        logger.info("Google Sheets client setup successful")
        return client
        
    except Exception as e:
        logger.error(f"Error setting up Google Sheets: {e}")
        raise

def write_to_sheet(gc, am_name, sheet_id, formatted_rows, report_date):
    """Write data to Google Sheet"""
    logger.info(f"Writing data to sheet for {am_name}")
    
    try:
        # Open the spreadsheet
        spreadsheet = gc.open_by_key(sheet_id)
        
        # Create worksheet name (DD-MMM format)
        worksheet_name = report_date.strftime('%d-%b')
        
        # Check if worksheet exists, create or clear it
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
            worksheet.clear()
            logger.info(f"Cleared existing worksheet: {worksheet_name}")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=30)
            logger.info(f"Created new worksheet: {worksheet_name}")
        
        # Move worksheet to first position
        try:
            worksheet_list = spreadsheet.worksheets()
            if len(worksheet_list) > 1 and worksheet_list[0].title != worksheet_name:
                worksheet.update_index(0)  # Move to first position
                logger.info(f"Moved worksheet {worksheet_name} to first position")
        except Exception as e:
            logger.warning(f"Could not move worksheet to first position: {e}")
        
        # Prepare data with headers
        all_data = [OUTPUT_COLUMNS]  # Headers
        
        if formatted_rows:
            all_data.extend(formatted_rows)  # Campaign data with client summaries
        
        # Write all data at once
        if all_data:
            worksheet.update(values=all_data, range_name='A1')
            campaign_count = len([row for row in formatted_rows if row[0] and not row[0].endswith(" - Summary")])
            summary_count = len([row for row in formatted_rows if row[0] and row[0].endswith(" - Summary")])
            logger.info(f"Wrote {campaign_count} campaign rows + {summary_count} client summary rows to {worksheet_name}")
        else:
            worksheet.update(values=[OUTPUT_COLUMNS], range_name='A1')
            logger.info(f"Wrote headers only to {worksheet_name} (no data)")
            
    except Exception as e:
        logger.error(f"Error writing to sheet for {am_name}: {e}")
        raise

# ========== MAIN EXECUTION ==========

def main():
    """Main execution function"""
    logger.info("Starting daily campaign reporting...")
    
    try:
        # Calculate yesterday's date (just date, no time)
        yesterday = datetime.utcnow().date() - timedelta(days=1)
        yesterday = datetime.combine(yesterday, datetime.min.time())  # Convert back to datetime
        logger.info(f"Generating report for: {yesterday.strftime('%Y-%m-%d')}")
        
        # First, discover available columns (for debugging)
        discover_campaign_columns()
        
        # Check what dates are available in the database
        check_available_dates()
        
        # Fetch data from databases
        clients = fetch_clients()
        campaigns = fetch_campaigns(yesterday)
        
        # Build mappings
        client_mapping = build_client_mapping(clients)
        am_campaigns = group_campaigns_by_am(campaigns, client_mapping)
        
        # Setup Google Sheets
        gc = setup_google_sheets()
        
        # Process each Account Manager
        for am_name, am_campaign_list in am_campaigns.items():
            if am_name in account_manager_sheets:
                logger.info(f"Processing {len(am_campaign_list)} campaigns for {am_name}")
                
                # Format data with client-wise summaries
                formatted_rows = format_campaign_data_by_client(am_campaign_list, client_mapping, am_name)
                
                # Write to Google Sheet
                sheet_id = account_manager_sheets[am_name]
                write_to_sheet(gc, am_name, sheet_id, formatted_rows, yesterday)
                
            else:
                logger.warning(f"No Google Sheet configured for AM: {am_name}")
        
        # Handle AMs with no campaigns
        for am_name, sheet_id in account_manager_sheets.items():
            if am_name not in am_campaigns:
                logger.info(f"No campaigns found for {am_name}, creating empty sheet")
                write_to_sheet(gc, am_name, sheet_id, [], yesterday)
        
        logger.info("Daily reporting completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()