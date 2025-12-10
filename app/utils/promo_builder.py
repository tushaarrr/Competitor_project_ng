"""Utility functions for building standardized promotion objects."""
from typing import Dict, Optional, List
from datetime import datetime
from pathlib import Path
import json


def build_standard_promo(
    competitor: Dict,
    promo_url: str,
    service_name: str,
    promo_description: str,
    category: str,
    offer_details: str,
    ad_title: str,
    ad_text: str,
    google_reviews: Optional[float] = None,
    existing_promo: Optional[Dict] = None
) -> Dict:
    """
    Build a standardized promotion object with only the required fields.

    Args:
        competitor: Competitor data dict
        promo_url: URL of the promotion page
        service_name: Short readable service name
        promo_description: Clean summary of the promotion
        category: Service category
        offer_details: Discount amount, code, expiry if present
        ad_title: Headline-like title
        ad_text: Clear marketing text
        google_reviews: Numeric review rating (e.g., 4.3) or None
        existing_promo: Existing promotion for comparison (for new_or_updated)

    Returns:
        Dict with exactly these fields:
        - website
        - page_url
        - business_name
        - google_reviews (numeric or None)
        - service_name
        - promo_description
        - category
        - contact
        - location
        - offer_details
        - ad_title
        - ad_text
        - new_or_updated (NEW/UPDATED/SAME)
        - date_scraped (YYYY-MM-DD)
    """
    # Determine new_or_updated status
    new_or_updated = "NEW"
    if existing_promo:
        # Compare key fields to determine if updated
        if (existing_promo.get("service_name") == service_name and
            existing_promo.get("promo_description") == promo_description and
            existing_promo.get("offer_details") == offer_details):
            new_or_updated = "SAME"
        else:
            new_or_updated = "UPDATED"

    # Format date as YYYY-MM-DD
    date_scraped = datetime.now().strftime("%Y-%m-%d")

    # Clean text - remove markdown and extra whitespace
    def clean_text(text: str) -> str:
        if not text:
            return ""
        # Remove markdown formatting
        text = text.replace("**", "").replace("*", "").replace("__", "").replace("_", "")
        text = text.replace("#", "").replace("##", "").replace("###", "")
        # Clean up whitespace
        text = " ".join(text.split())
        return text.strip()

    promo = {
        "website": competitor.get("domain", ""),
        "page_url": promo_url,
        "business_name": competitor.get("name", ""),
        "google_reviews": google_reviews if google_reviews is not None else None,
        "service_name": clean_text(service_name),
        "promo_description": clean_text(promo_description),
        "category": clean_text(category),
        "contact": competitor.get("address", ""),
        "location": competitor.get("address", ""),
        "offer_details": clean_text(offer_details),
        "ad_title": clean_text(ad_title),
        "ad_text": clean_text(ad_text),
        "new_or_updated": new_or_updated,
        "date_scraped": date_scraped
    }

    return promo


def load_existing_promos(promotions_file: Path) -> Dict[str, Dict]:
    """
    Load existing promotions from JSON file for comparison.

    Returns:
        Dict mapping promo_key -> promo_dict for quick lookup
    """
    if not promotions_file.exists():
        return {}

    try:
        with open(promotions_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            promos = data.get("promotions", [])

            # Create lookup by key (page_url + service_name)
            lookup = {}
            for promo in promos:
                key = f"{promo.get('page_url', '')}::{promo.get('service_name', '')}"
                lookup[key] = promo

            return lookup
    except Exception as e:
        return {}


def apply_ai_overview_fallback(promos: List[Dict], competitor: Dict) -> List[Dict]:
    """
    Apply AI Overview fallback if no promotions were found.

    Args:
        promos: List of promotions (may be empty)
        competitor: Competitor data dict

    Returns:
        List of promotions (original or with AI Overview fallback added)
    """
    if promos and len(promos) > 0:
        return promos

    # Try AI Overview fallback
    try:
        from app.extractors.serpapi.business_overview_extractor import extract_promo_from_ai_overview
        ai_overview_promo = extract_promo_from_ai_overview(competitor)
        if ai_overview_promo:
            return [ai_overview_promo]
    except Exception as e:
        # Silently fail - don't break the scraper if AI Overview fails
        pass

    return promos


def get_google_reviews_for_competitor(competitor: Dict) -> Optional[float]:
    """
    Get Google Reviews rating for a competitor using SerpAPI.

    Args:
        competitor: Competitor data dict

    Returns:
        Google Reviews rating (float) or None if not found
    """
    try:
        from app.extractors.serpapi.serpapi_client import get_ai_overview, extract_business_info_from_serpapi
        from app.utils.logging_utils import setup_logger

        logger = setup_logger(__name__)

        business_name = competitor.get("name", "")
        if not business_name:
            return None

        # Build search query
        query = f"{business_name} {competitor.get('address', 'Edmonton')}"
        location = competitor.get("address", "Edmonton, AB, Canada")

        # Fetch AI Overview (this also gets business info)
        overview_data = get_ai_overview(query, location)
        if not overview_data:
            return None

        full_data = overview_data.get("full_data", {})
        business_info = extract_business_info_from_serpapi(full_data)

        google_reviews = business_info.get("google_reviews")
        if google_reviews:
            logger.info(f"Found Google Reviews for {business_name}: {google_reviews}")

        return google_reviews
    except Exception as e:
        # Silently fail - don't break the scraper if Google Reviews fetch fails
        return None

