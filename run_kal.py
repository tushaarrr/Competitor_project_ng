"""Run Kal Tire scraper."""
import json
import sys
import io
from pathlib import Path
from app.scrapers.kal_scraper import scrape_kal

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Kal Tire
    kal = next((c for c in competitors if "kal" in c.get("name", "").lower()), None)

    if not kal:
        print("❌ Kal Tire not found in competitor list")
        exit(1)

    print(f"🚀 Starting Kal Tire scraper...")
    print(f"   URL: {kal.get('promo_links', [None])[0]}")
    print()

    result = scrape_kal(kal)

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", [])[:20]:  # Show first 20
            title = promo.get('promotion_title', promo.get('ad_title', 'N/A'))
            discount = promo.get('discount_value', 'N/A')
            tab = promo.get('tab_source', 'N/A')
            print(f"   • [{tab}] {title}: {discount}")
        if len(result.get("promotions", [])) > 20:
            print(f"   ... and {len(result.get('promotions', [])) - 20} more")

