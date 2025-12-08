"""Mr. Lube scraper - Uses AI Overview as primary method."""
import json
from pathlib import Path
from typing import Dict
from datetime import datetime
from app.utils.extraction_flow import unified_extraction_flow, format_for_google_sheets
from app.config.constants import DATA_DIR
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMOTIONS_DIR = DATA_DIR / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)


def process_mrlube_promotions(competitor: Dict) -> list:
    """
    Process Mr. Lube promotions.

    Note: This function is passed to unified_extraction_flow but will be skipped
    for MR LUBE since unified_extraction_flow detects MR LUBE and uses AI Overview directly.
    """
    # This should never be called for MR LUBE, but included for consistency
    return []


def scrape_mrlube(competitor: Dict) -> Dict:
    """Main entry point for Mr. Lube scraper."""
    try:
        # Use unified extraction flow (will automatically use AI Overview for MR LUBE)
        promos = unified_extraction_flow(competitor, process_mrlube_promotions)

        # Format for Google Sheets
        formatted_promos = [format_for_google_sheets(promo) for promo in promos]

        # Save results
        output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'mrlube').lower().replace(' ', '_').replace('.', '')}.json"
        result = {
            "competitor": competitor.get("name"),
            "scraped_at": datetime.now().isoformat(),
            "promotions": formatted_promos,
            "count": len(formatted_promos)
        }

        output_file.write_text(json.dumps(result, indent=2, default=str))
        logger.info(f"Saved {len(formatted_promos)} promotions to {output_file}")

        return result

    except Exception as e:
        logger.error(f"Error scraping Mr. Lube: {e}", exc_info=True)
        return {
            "competitor": competitor.get("name"),
            "error": str(e),
            "promotions": [],
            "count": 0
        }


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Load competitor data
    competitor_file = Path(__file__).parent.parent / "config" / "competitor_list.json"
    competitors = json.loads(competitor_file.read_text())

    # Find Mr. Lube
    mrlube = next((c for c in competitors if "lube" in c.get("name", "").lower() and "mr" in c.get("name", "").lower()), None)

    if not mrlube:
        logger.error("Mr. Lube not found in competitor list")
        sys.exit(1)

    result = scrape_mrlube(mrlube)
    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: {PROMOTIONS_DIR}")

