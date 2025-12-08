"""Run Good News Auto scraper."""
import json
import sys
import io
from pathlib import Path
from app.scrapers.goodnews_scraper import scrape_goodnews

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Good News Auto
    goodnews = next((c for c in competitors if "good news" in c.get("name", "").lower()), None)

    if not goodnews:
        print("❌ Good News Auto not found in competitor list")
        exit(1)

    print(f"🚀 Starting Good News Auto scraper...")
    print(f"   URL: {goodnews.get('promo_links', [None])[0]}")
    print()

    result = scrape_goodnews(goodnews)

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", []):
            title = promo.get('service_name', promo.get('ad_title', 'N/A'))
            status = promo.get('new_or_updated', 'NEW')
            print(f"   • {title}: {status}")

