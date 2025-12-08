"""Run Trail Tire scraper."""
import json
import sys
import io
from pathlib import Path
from app.scrapers.trail_scraper import scrape_trail

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Trail Tire
    trail = next((c for c in competitors if "trail" in c.get("name", "").lower()), None)

    if not trail:
        print("❌ Trail Tire Auto Centres not found in competitor list")
        exit(1)

    print(f"🚀 Starting Trail Tire scraper...")
    print(f"   URL: {trail.get('promo_links', [None])[0]}")
    print()

    result = scrape_trail(trail)

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", [])[:15]:  # Show first 15
            title = promo.get('promotion_title', promo.get('ad_title', 'N/A'))
            discount = promo.get('discount_value', 'N/A')
            print(f"   • {title}: {discount}")
        if len(result.get("promotions", [])) > 15:
            print(f"   ... and {len(result.get('promotions', [])) - 15} more")

