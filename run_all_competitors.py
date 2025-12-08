"""Run all competitor scrapers and merge results into Google Sheets."""
import json
import sys
import io
from pathlib import Path
from app.utils.logging_utils import setup_logger
from app.utils.sheets_merger import merge_and_write_to_sheets

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
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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

    # Find competitor by name (case-insensitive partial match)
    for comp in competitors:
        comp_name = comp.get("name", "").lower()
        if name.lower() in comp_name or comp_name in name.lower():
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

    print("\n" + "=" * 60)
    print("✨ Pipeline Complete!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()

