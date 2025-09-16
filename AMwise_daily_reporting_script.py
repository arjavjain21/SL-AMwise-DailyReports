#!/usr/bin/env python3
"""
Daily Campaign Reporting Script
Extracts campaign analytics from Supabase databases and writes to Google Sheets

USAGE MODES:
1. Single Day (Yesterday): main()
2. Single Day (Specific): main(datetime(2025, 8, 23))
3. Batch Range: batch_main("2025-08-15", "2025-08-23")

The script will create separate worksheets (DD-MMM format) for each date processed.

ENVIRONMENT VARIABLES REQUIRED:
- GOOGLE_SERVICE_ACCOUNT_JSON: Complete service account JSON as string
- REPORTING_DB_URL: PostgreSQL connection string for reporting database
- CLIENTS_DB_URL: PostgreSQL connection string for clients database
"""

import logging
import psycopg2
from datetime import datetime, timedelta, date
import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ========== ENVIRONMENT VARIABLES AND CONFIGURATION ==========

def load_environment_variables():
    """Load and validate required environment variables"""
    logger.info("Loading environment variables...")
    
    # Load database URLs
    reporting_db_url = os.getenv('REPORTING_DB_URL')
    clients_db_url = os.getenv('CLIENTS_DB_URL')
    
    # Load Google service account JSON
    google_service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    # Validate required environment variables
    missing_vars = []
    if not reporting_db_url:
        missing_vars.append('REPORTING_DB_URL')
    if not clients_db_url:
        missing_vars.append('CLIENTS_DB_URL')
    if not google_service_account_json:
        missing_vars.append('GOOGLE_SERVICE_ACCOUNT_JSON')
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    # Parse Google service account JSON
    try:
        service_account_info = json.loads(google_service_account_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in GOOGLE_SERVICE_ACCOUNT_JSON: {e}")
    
    logger.info("All environment variables loaded successfully")
    
    return {
        'reporting_db_url': reporting_db_url,
        'clients_db_url': clients_db_url,
        'service_account_info': service_account_info
    }

# For local development only
try:
    from dotenv import load_dotenv
    load_dotenv()
    logger.info("Loaded .env file for local development")
except ImportError:
    logger.info("python-dotenv not installed, using system environment variables")

# Load environment variables at startup
try:
    env_config = load_environment_variables()
    REPORTING_DB_URL = env_config['reporting_db_url']
    CLIENTS_DB_URL = env_config['clients_db_url']
    service_account_info = env_config['service_account_info']
except Exception as e:
    logger.error(f"Environment configuration error: {e}")
    logger.error("Please ensure all required environment variables are set:")
    logger.error("- REPORTING_DB_URL: PostgreSQL connection string for reporting database")
    logger.error("- CLIENTS_DB_URL: PostgreSQL connection string for clients database") 
    logger.error("- GOOGLE_SERVICE_ACCOUNT_JSON: Complete service account JSON as string")
    raise

# Account Manager to Google Sheet ID mapping
account_manager_sheets = {
    "Carl": "1Fx82mA0GjvmdwXIsvgKf92p_W133dniP82OGr1LoXIU",
    "Gaius": "1wbz2evFpJwfPhC2uhqqUrg-_ybh2waqV9Y5P3UG-nzs",
    "Ram Prakash": "15MJhaktorXvTwqEVZp8Om77ecEi1MSLMs1uFR0tv6iI",
    "Dhanraj": "1uAYJnh1Y5c8L8sflOcULgI1Hw8eEjchY3GSs9tgAI0U",
    "Benjie": "1ZEiwmSdFnlybhIBpi4lEh0Pe1AOZw087EXjFyU-fk1A"
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

def main(target_date=None):
    """Main execution function"""
    logger.info("Starting daily campaign reporting...")
    
    try:
        # Use provided date or default to yesterday
        if target_date:
            report_date = target_date
        else:
            # Calculate yesterday's date (just date, no time)
            yesterday = datetime.utcnow().date() - timedelta(days=1)
            report_date = datetime.combine(yesterday, datetime.min.time())  # Convert back to datetime
        
        logger.info(f"Generating report for: {report_date.strftime('%Y-%m-%d')}")
        
        # First, discover available columns (for debugging) - only on first run
        if not target_date:
            discover_campaign_columns()
            check_available_dates()
        
        # Fetch data from databases
        clients = fetch_clients()
        campaigns = fetch_campaigns(report_date)
        
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
                write_to_sheet(gc, am_name, sheet_id, formatted_rows, report_date)
                
            else:
                logger.warning(f"No Google Sheet configured for AM: {am_name}")
        
        # Handle AMs with no campaigns
        for am_name, sheet_id in account_manager_sheets.items():
            if am_name not in am_campaigns:
                logger.info(f"No campaigns found for {am_name}, creating empty sheet")
                write_to_sheet(gc, am_name, sheet_id, [], report_date)
        
        logger.info(f"Daily reporting completed successfully for {report_date.strftime('%Y-%m-%d')}!")
        
    except Exception as e:
        logger.error(f"Error in main execution for {report_date.strftime('%Y-%m-%d') if 'report_date' in locals() else 'unknown date'}: {e}")
        raise

def batch_main(batch_start_date, batch_end_date):
    """Run main script for a batch of dates"""
    logger.info(f"Starting batch campaign reporting from {batch_start_date} to {batch_end_date}")
    
    try:
        # Parse start and end dates
        if isinstance(batch_start_date, str):
            start_date = datetime.strptime(batch_start_date, '%Y-%m-%d').date()
        else:
            start_date = batch_start_date
            
        if isinstance(batch_end_date, str):
            end_date = datetime.strptime(batch_end_date, '%Y-%m-%d').date()
        else:
            end_date = batch_end_date
        
        # Validate date range
        if start_date > end_date:
            raise ValueError("batch_start_date must be before or equal to batch_end_date")
        
        # Run discovery functions once at the beginning
        logger.info("Running initial discovery functions...")
        discover_campaign_columns()
        check_available_dates()
        
        # Generate list of dates in the range
        current_date = start_date
        processed_dates = []
        failed_dates = []
        
        while current_date <= end_date:
            try:
                # Convert to datetime for main function
                target_datetime = datetime.combine(current_date, datetime.min.time())
                
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing batch date: {current_date.strftime('%Y-%m-%d')} ({current_date.strftime('%d-%b')})")
                logger.info(f"{'='*60}")
                
                # Run main function for this specific date
                main(target_datetime)
                
                processed_dates.append(current_date.strftime('%Y-%m-%d'))
                logger.info(f"Successfully processed {current_date.strftime('%Y-%m-%d')}")
                
            except Exception as e:
                failed_dates.append(current_date.strftime('%Y-%m-%d'))
                logger.error(f"Failed to process {current_date.strftime('%Y-%m-%d')}: {e}")
                # Continue with next date instead of stopping
            
            # Move to next date
            current_date += timedelta(days=1)
        
        # Summary report
        logger.info(f"\n{'='*60}")
        logger.info("BATCH PROCESSING SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Date range: {batch_start_date} to {batch_end_date}")
        logger.info(f"Successfully processed: {len(processed_dates)} dates")
        logger.info(f"Failed: {len(failed_dates)} dates")
        
        if processed_dates:
            logger.info(f"Successful dates: {', '.join(processed_dates)}")
        
        if failed_dates:
            logger.info(f"Failed dates: {', '.join(failed_dates)}")
        
        logger.info("Batch campaign reporting completed!")
        
        return {
            'processed_dates': processed_dates,
            'failed_dates': failed_dates,
            'total_processed': len(processed_dates),
            'total_failed': len(failed_dates)
        }
        
    except Exception as e:
        logger.error(f"Error in batch execution: {e}")
        raise

if __name__ == "__main__":
    # Choose your execution mode:
    
    # MODE 1: Run for yesterday (default single day execution)
    main()
    
    # MODE 2: Run for a specific single date
    # from datetime import datetime
    # specific_date = datetime(2025, 8, 23)  # August 23, 2025
    # main(specific_date)
    
    # MODE 3: Run batch processing for a date range
    # Uncomment the lines below and comment out main() above to use batch mode
    # batch_result = batch_main(
    #     batch_start_date="2025-08-15",  # Start date (YYYY-MM-DD)
    #     batch_end_date="2025-08-23"     # End date (YYYY-MM-DD)
    # )
    # print(f"Batch processing completed: {batch_result}")
    
    # MODE 4: Run batch processing with datetime objects
    # from datetime import date
    # batch_result = batch_main(
    #     batch_start_date=date(2025, 8, 15),
    #     batch_end_date=date(2025, 8, 23)
    # )
    # print(f"Batch processing completed: {batch_result}")
