"""Run Integra Tire scraper."""
import json
import sys
import io
from pathlib import Path
from app.scrapers.integra_scraper import scrape_integra

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if __name__ == "__main__":
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Integra Tire
    integra = next((c for c in competitors if "integra" in c.get("name", "").lower()), None)

    if not integra:
        print("❌ Integra Tire Auto Centre not found in competitor list")
        exit(1)

    print(f"🚀 Starting Integra Tire scraper...")
    print(f"   URL: {integra.get('promo_links', [None])[0]}")
    print()

    result = scrape_integra(integra)

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result.get("promotions", [])[:10]:  # Show first 10
            title = promo.get('promotion_title', promo.get('ad_title', 'N/A'))
            discount = promo.get('discount_value', 'N/A')
            print(f"   • {title}: {discount}")
        if len(result.get("promotions", [])) > 10:
            print(f"   ... and {len(result.get('promotions', [])) - 10} more")

