#!/usr/bin/env python3
"""Quick runner script for Speedy scraper."""
import sys
import json
import io
from pathlib import Path

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add app to path
sys.path.insert(0, str(Path(__file__).parent))

from app.scrapers.speedy_scraper import scrape_speedy

def main():
    # Load competitor data
    competitor_file = Path(__file__).parent / "app" / "config" / "competitor_list.json"

    if not competitor_file.exists():
        print(f"Error: Competitor list not found at {competitor_file}")
        return 1

    competitors = json.loads(competitor_file.read_text())

    # Find Speedy
    speedy = next((c for c in competitors if "speedy" in c.get("name", "").lower()), None)

    if not speedy:
        print("Error: Speedy Auto Service not found in competitor list")
        return 1

    print(f"🚀 Starting Speedy Auto Service scraper...")
    print(f"   URL: {speedy.get('promo_links', [None])[0]}")
    print()

    result = scrape_speedy(speedy)

    if result.get("error"):
        print(f"\n❌ Error: {result['error']}")
        return 1

    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")

    # Show summary
    if result.get("promotions"):
        print(f"\n📊 Summary:")
        for promo in result["promotions"][:5]:  # Show first 5
            print(f"   • {promo.get('service_name', 'Unknown')}: {promo.get('discount_value', 'N/A')}")
        if len(result["promotions"]) > 5:
            print(f"   ... and {len(result['promotions']) - 5} more")

    return 0

if __name__ == "__main__":
    sys.exit(main())

