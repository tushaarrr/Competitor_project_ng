"""Run Jiffy Lube scraper."""
import json
import sys
import io
from pathlib import Path
from app.scrapers.jiffy_scraper import scrape_jiffy

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Jiffy Lube
    jiffy = next((c for c in competitors if "jiffy" in c.get("name", "").lower()), None)

    if not jiffy:
        print("❌ Jiffy Lube not found in competitor list")
        exit(1)

    print(f"🚀 Starting Jiffy Lube scraper...")
    print(f"   URL: {jiffy.get('promo_links', [None])[0]}")
    print()

    result = scrape_jiffy(jiffy)

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", []):
            title = promo.get('promotion_title', promo.get('ad_title', 'N/A'))
            discount = promo.get('discount_value', 'N/A')
            code = promo.get('coupon_code', 'N/A')
            print(f"   • {title}: {discount} (Code: {code})")

