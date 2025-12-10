"""Merge all competitor promotions and write to Google Sheets."""
import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
from app.utils.logging_utils import setup_logger
from app.utils.sheets_writer import write_to_sheets, clean_promo_for_sheets
from app.config.constants import DATA_DIR, ROOT, GOOGLE_SHEETS_ID

logger = setup_logger(__name__)

PROMOTIONS_DIR = DATA_DIR / "promotions"


def load_all_promotions() -> List[Dict]:
    """
    Load all promotions from JSON files.

    Returns:
        List of all promotion dicts from all competitors
    """
    all_promos = []

    if not PROMOTIONS_DIR.exists():
        logger.warning(f"Promotions directory not found: {PROMOTIONS_DIR}")
        return all_promos

    # Find all JSON files in promotions directory (EXCLUDE merged_promotions.json)
    json_files = [f for f in PROMOTIONS_DIR.glob("*.json") if f.name != "merged_promotions.json"]

    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                promos = data.get("promotions", [])
                all_promos.extend(promos)
                logger.info(f"Loaded {len(promos)} promotions from {json_file.name}")
        except Exception as e:
            logger.warning(f"Error loading {json_file.name}: {e}")

    logger.info(f"Total promotions loaded: {len(all_promos)}")
    return all_promos


def deduplicate_promotions(promos: List[Dict]) -> List[Dict]:
    """
    Deduplicate promotions across all competitors.

    For Fountain Tire, uses specialized deduplication rules.
    For other competitors, uses title, discount value, and text similarity.

    Args:
        promos: List of all promotions

    Returns:
        Deduplicated list
    """
    from fuzzywuzzy import fuzz

    # Separate Fountain Tire from other competitors
    fountain_promos = []
    other_promos = []

    for promo in promos:
        business_name = promo.get("business_name", "").lower()
        if "fountain" in business_name and "tire" in business_name:
            fountain_promos.append(promo)
        else:
            other_promos.append(promo)

    # For Fountain Tire, use the scraper's deduplication (already done in scraper)
    # But we still need to deduplicate across different runs, so use Fountain Tire rules here too
    deduplicated = []
    seen = []

    # Process Fountain Tire with specialized rules
    if fountain_promos:
        from app.scrapers.fountain_scraper import are_promos_duplicate, normalize_text_for_dedup

        seen_keys = set()
        for promo in fountain_promos:
            is_duplicate = False

            # Build composite key
            service_clean = normalize_text_for_dedup(promo.get("service_name", ""))
            desc_clean = normalize_text_for_dedup(promo.get("promo_description", ""))
            offer_clean = normalize_text_for_dedup(promo.get("offer_details", ""))
            composite_key = service_clean + desc_clean + offer_clean

            # Check against seen Fountain Tire promos
            for seen_promo in seen:
                if are_promos_duplicate(promo, seen_promo):
                    is_duplicate = True
                    break

            # Check composite key
            if composite_key and composite_key in seen_keys:
                is_duplicate = True

            if not is_duplicate:
                deduplicated.append(promo)
                seen.append(promo)
                if composite_key:
                    seen_keys.add(composite_key)

    # Process other competitors with standard deduplication
    for promo in other_promos:
        is_duplicate = False

        # Create a signature for this promo
        business_name = promo.get("business_name", "").lower()
        service_name = promo.get("service_name", "").lower()
        offer_details = promo.get("offer_details", "").lower()
        ad_title = promo.get("ad_title", "").lower()

        # Check against seen promos
        for seen_promo in seen:
            seen_business = seen_promo.get("business_name", "").lower()
            seen_service = seen_promo.get("service_name", "").lower()
            seen_offer = seen_promo.get("offer_details", "").lower()
            seen_title = seen_promo.get("ad_title", "").lower()

            # Same business and service
            if business_name == seen_business and service_name == seen_service:
                # Check title similarity
                title_similarity = fuzz.ratio(ad_title, seen_title)

                # Check offer similarity
                offer_similarity = fuzz.ratio(offer_details, seen_offer)

                # If very similar (90%+), consider duplicate
                if title_similarity >= 90 or offer_similarity >= 90:
                    is_duplicate = True
                    break

        if not is_duplicate:
            deduplicated.append(promo)
            seen.append(promo)

    logger.info(f"Deduplicated: {len(promos)} -> {len(deduplicated)} promotions")
    return deduplicated


def determine_new_or_updated(promo: Dict, existing_promos: List[Dict]) -> str:
    """
    Determine if promo is NEW, UPDATED, or SAME.

    Args:
        promo: Current promo
        existing_promos: List of existing promos from previous scrape

    Returns:
        "NEW", "UPDATED", or "SAME"
    """
    # Create key for this promo
    promo_key = f"{promo.get('business_name', '')}::{promo.get('service_name', '')}::{promo.get('page_url', '')}"

    # Find matching existing promo
    for existing in existing_promos:
        existing_key = f"{existing.get('business_name', '')}::{existing.get('service_name', '')}::{existing.get('page_url', '')}"

        if promo_key == existing_key:
            # Compare key fields
            fields_to_compare = [
                "promo_description",
                "offer_details",
                "ad_title",
                "ad_text"
            ]

            all_same = all(
                promo.get(field, "") == existing.get(field, "")
                for field in fields_to_compare
            )

            if all_same:
                return "SAME"
            else:
                return "UPDATED"

    return "NEW"


def merge_and_write_to_sheets(
    spreadsheet_id: str = None,
    sheet_name: str = "Promotions"
) -> bool:
    """
    Merge all competitor promotions and write to Google Sheets.

    Args:
        spreadsheet_id: Google Sheets ID (from env if not provided)
        sheet_name: Name of the sheet tab

    Returns:
        True if successful
    """
    # Get spreadsheet ID
    if not spreadsheet_id:
        spreadsheet_id = GOOGLE_SHEETS_ID
        if not spreadsheet_id:
            logger.error("GOOGLE_SHEETS_ID not set in environment")
            return False

    # Load all promotions
    all_promos = load_all_promotions()

    if not all_promos:
        logger.warning("No promotions found to write to sheets")
        return False

    # Deduplicate
    deduplicated = deduplicate_promotions(all_promos)

    # Load existing promos for comparison (if available)
    existing_file = PROMOTIONS_DIR / "merged_promotions.json"
    existing_promos = []
    if existing_file.exists():
        try:
            with open(existing_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                existing_promos = existing_data.get("promotions", [])
        except Exception:
            pass

    # Determine new_or_updated status
    for promo in deduplicated:
        if promo.get("new_or_updated") not in ["NEW", "UPDATED", "SAME"]:
            promo["new_or_updated"] = determine_new_or_updated(promo, existing_promos)

    # Write to Google Sheets
    success = write_to_sheets(spreadsheet_id, deduplicated, sheet_name)

    if success:
        # Save merged data locally
        merged_file = PROMOTIONS_DIR / "merged_promotions.json"
        merged_data = {
            "merged_at": datetime.now().isoformat(),
            "total_promotions": len(deduplicated),
            "promotions": deduplicated
        }
        merged_file.write_text(json.dumps(merged_data, indent=2, default=str))
        logger.info(f"Saved merged data to {merged_file}")

    return success

