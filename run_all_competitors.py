"""Run all competitor scrapers and merge results into Google Sheets."""
import json
import sys
import io
from pathlib import Path
from app.utils.logging_utils import setup_logger
from app.utils.sheets_merger import merge_and_write_to_sheets
from app.extractors.serpapi.ads_extractor import extract_ads_for_all_competitors
from app.utils.sheets_writer import write_ads_to_sheets
from app.config.constants import GOOGLE_SHEETS_ID

# Import all scrapers
from app.scrapers.goodnews_scraper import scrape_goodnews
from app.scrapers.speedy_scraper import scrape_speedy
from app.scrapers.midas_scraper import scrape_midas
from app.scrapers.jiffy_scraper import scrape_jiffy
from app.scrapers.kal_scraper import scrape_kal
from app.scrapers.fountain_scraper import scrape_fountain
from app.scrapers.integra_scraper import scrape_integra
from app.scrapers.trail_scraper import scrape_trail
from app.scrapers.valvoline_scraper import scrape_valvoline
from app.scrapers.mrlube_scraper import scrape_mrlube

# Fix encoding for Windows console
# Only wrap if not already wrapped to avoid I/O errors
if sys.platform == 'win32':
    try:
        if not isinstance(sys.stdout, io.TextIOWrapper) or sys.stdout.encoding != 'utf-8':
            if hasattr(sys.stdout, 'buffer'):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        if not isinstance(sys.stderr, io.TextIOWrapper) or sys.stderr.encoding != 'utf-8':
            if hasattr(sys.stderr, 'buffer'):
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError, OSError):
        # If wrapping fails, continue without it
        pass

logger = setup_logger(__name__)

# Competitor list with their scrapers
COMPETITORS_AND_SCRAPERS = [
    ("Good News Auto", scrape_goodnews),
    ("Midas", scrape_midas),
    ("Kal Tire", scrape_kal),
    ("Jiffy Lube", scrape_jiffy),
    ("Fountain Tire", scrape_fountain),
    ("Speedy Auto Service", scrape_speedy),
    ("Trail Tire Auto Centres", scrape_trail),
    ("Integra Tire Auto Centre", scrape_integra),
    ("Valvoline Express Care", scrape_valvoline),
    ("Mr. Lube", scrape_mrlube),  # MR LUBE uses AI Overview as primary
]


def load_competitor(name: str) -> dict:
    """Load competitor data from JSON."""
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # More flexible matching
    name_lower = name.lower().strip()
    for comp in competitors:
        comp_name = comp.get("name", "").lower().strip()
        # Check if either name contains the other (with spaces removed for flexibility)
        name_no_spaces = name_lower.replace(" ", "")
        comp_no_spaces = comp_name.replace(" ", "")

        if (name_lower in comp_name or comp_name in name_lower or
            name_no_spaces in comp_no_spaces or comp_no_spaces in name_no_spaces):
            return comp

    return None


def run_all_scrapers():
    """Run all competitor scrapers sequentially."""
    print("\n" + "=" * 60)
    print("🚀 Starting All Competitor Scrapers")
    print("=" * 60 + "\n")

    results = []

    for competitor_name, scraper_func in COMPETITORS_AND_SCRAPERS:
        print(f"\n📋 Processing: {competitor_name}")
        print("-" * 60)

        # Load competitor data
        competitor = load_competitor(competitor_name)
        if not competitor:
            print(f"❌ {competitor_name} not found in competitor list")
            continue

        try:
            # Run scraper
            result = scraper_func(competitor)

            count = result.get("count", 0)
            if count > 0:
                print(f"✅ {competitor_name}: Found {count} promotion(s)")
                results.append(result)
            else:
                print(f"⚠️  {competitor_name}: No promotions found")
                results.append(result)

        except Exception as e:
            print(f"❌ {competitor_name}: Error - {e}")
            logger.error(f"Error scraping {competitor_name}: {e}", exc_info=True)
            results.append({
                "competitor": competitor_name,
                "error": str(e),
                "promotions": [],
                "count": 0
            })

    return results


def main():
    """Main function: Run all scrapers and merge to Google Sheets."""
    print("\n" + "=" * 60)
    print("🎯 Competitor Intelligence Dashboard - Full Pipeline")
    print("=" * 60)

    # Step 1: Run all scrapers
    print("\n📊 STEP 1: Running All Competitor Scrapers")
    print("=" * 60)
    results = run_all_scrapers()

    # Summary
    total_promos = sum(r.get("count", 0) for r in results)
    successful = sum(1 for r in results if r.get("count", 0) > 0)

    print("\n" + "=" * 60)
    print("📈 SCRAPING SUMMARY")
    print("=" * 60)
    print(f"   Competitors processed: {len(results)}")
    print(f"   Successful: {successful}")
    print(f"   Total promotions: {total_promos}")
    print("=" * 60)

    # Step 2: Merge and write to Google Sheets
    print("\n📝 STEP 2: Merging and Writing to Google Sheets")
    print("=" * 60)

    success = merge_and_write_to_sheets()

    if success:
        print("\n✅ SUCCESS! All promotions merged and written to Google Sheets")
        print("   Check your Google Sheet for the updated dashboard.")
    else:
        print("\n❌ FAILED to write to Google Sheets")
        print("   Check logs for details.")

    # Step 3: Extract and write Google Ads
    print("\n📢 STEP 3: Extracting Google Ads")
    print("=" * 60)

    # Load all competitors
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    all_competitors = json.loads(competitor_file.read_text())

    print(f"   Extracting ads for {len(all_competitors)} competitors...")
    all_ads = extract_ads_for_all_competitors(all_competitors, limit=2)

    if all_ads:
        print(f"   Found {len(all_ads)} promotional ads")

        # Write ads to Google Sheets
        ads_success = write_ads_to_sheets(GOOGLE_SHEETS_ID, all_ads, sheet_name="Advertisements")

        if ads_success:
            print(f"\n✅ SUCCESS! {len(all_ads)} ads written to 'Advertisements' tab")
        else:
            print("\n❌ FAILED to write ads to Google Sheets")
            print("   Check logs for details.")
    else:
        print("\n⚠️  No promotional ads found for any competitor")

    print("\n" + "=" * 60)
    print("✨ Pipeline Complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()

