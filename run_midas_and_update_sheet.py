"""Run Midas scraper and update Google Sheet (remove old, add new)."""
import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.scrapers.midas_scraper import scrape_midas
from app.utils.sheets_writer import remove_rows_by_business_name, write_to_sheets
from app.utils.promo_builder import build_standard_promo, load_existing_promos, get_google_reviews_for_competitor
from app.utils.extraction_flow import format_for_google_sheets
from app.config.constants import GOOGLE_SHEETS_ID
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Midas
    midas = next((c for c in competitors if "midas" in c.get("name", "").lower()), None)

    if not midas:
        print("❌ Midas not found in competitor list")
        sys.exit(1)

    print(f"🚀 Starting Midas scraper and sheet update...")
    print(f"   URLs: {', '.join(midas.get('promo_links', []))}")
    print()

    # Step 1: Remove existing Midas rows from sheet
    print("📝 Step 1: Removing existing Midas rows from Google Sheet...")
    if GOOGLE_SHEETS_ID:
        success = remove_rows_by_business_name(GOOGLE_SHEETS_ID, "Midas", "Promotions")
        if success:
            print("   ✅ Removed existing Midas rows")
        else:
            print("   ⚠️  Could not remove rows (may not exist)")
    else:
        print("   ⚠️  GOOGLE_SHEETS_ID not set, skipping removal")

    print()

    # Step 2: Run scraper
    print("🔍 Step 2: Running Midas scraper...")
    result = scrape_midas(midas)

    print(f"   ✅ Found {result.get('count', 0)} promotions")
    print()

    # Step 3: Add new promotions to sheet
    if result.get("promotions") and GOOGLE_SHEETS_ID:
        print("📊 Step 3: Adding new Midas promotions to Google Sheet...")

        # Get Google Reviews
        google_reviews = get_google_reviews_for_competitor(midas)

        # Format promotions for sheets
        formatted_promos = []
        for promo in result.get("promotions", []):
            # Ensure promo has all required fields
            formatted_promo = format_for_google_sheets(promo)
            formatted_promos.append(formatted_promo)

        # Read existing data from sheet
        from app.utils.sheets_writer import get_sheets_service, ensure_sheet_exists
        service = get_sheets_service()
        if service:
            ensure_sheet_exists(service, GOOGLE_SHEETS_ID, "Promotions")

            # Read existing rows
            try:
                existing_result = service.spreadsheets().values().get(
                    spreadsheetId=GOOGLE_SHEETS_ID,
                    range="Promotions!A1:Z1000"
                ).execute()

                existing_rows = existing_result.get('values', [])

                # Prepare new rows (skip header if it exists)
                if existing_rows:
                    headers = existing_rows[0]
                    new_rows = existing_rows[1:]  # Keep existing non-Midas rows
                else:
                    headers = ["website", "page_url", "business_name", "google_reviews", "service_name",
                               "promo_description", "category", "contact", "location", "offer_details",
                               "ad_title", "ad_text", "new_or_updated", "date_scraped"]
                    new_rows = []

                # Add new Midas promotions
                for promo in formatted_promos:
                    row = [promo.get(col, "") for col in headers]
                    new_rows.append(row)

                # Combine headers and data
                all_rows = [headers] + new_rows

                # Write to sheet
                body = {'values': all_rows}
                service.spreadsheets().values().update(
                    spreadsheetId=GOOGLE_SHEETS_ID,
                    range="Promotions!A1",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()

                # Apply formatting
                from app.utils.sheets_writer import apply_sheet_formatting
                apply_sheet_formatting(service, GOOGLE_SHEETS_ID, "Promotions", len(all_rows))

                print(f"   ✅ Added {len(formatted_promos)} Midas promotions to sheet")
                print(f"   📊 Total rows in sheet: {len(new_rows)}")

            except Exception as e:
                logger.error(f"Error updating sheet: {e}", exc_info=True)
                print(f"   ❌ Error updating sheet: {e}")
        else:
            print("   ❌ Could not connect to Google Sheets")
    else:
        if not GOOGLE_SHEETS_ID:
            print("   ⚠️  GOOGLE_SHEETS_ID not set, skipping sheet update")
        else:
            print("   ⚠️  No promotions found to add")

    print()
    print("✅ Complete!")
    print(f"   Promotions saved to: data/promotions/midas.json")
    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", []):
            service_name = promo.get('service_name', 'N/A')
            ad_title = promo.get('ad_title', 'N/A')
            print(f"   • {service_name}: {ad_title[:60]}")

