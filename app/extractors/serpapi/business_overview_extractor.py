"""Extract business overview and convert to promotion format using SerpAPI AI Overview."""
import json
from typing import Dict, Optional
from app.extractors.serpapi.serpapi_client import get_ai_overview, extract_business_info_from_serpapi
from app.utils.promo_builder import build_standard_promo
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)


def extract_promo_from_ai_overview(competitor: Dict) -> Optional[Dict]:
    """
    Extract promotion-style data from SerpAPI AI Overview as fallback.

    This is used when no promotions are found on the website.
    Converts business overview into structured promo format.

    Args:
        competitor: Competitor dict with name, address, domain, etc.

    Returns:
        Dict with standardized promo fields or None if failed
    """
    business_name = competitor.get("name", "")
    if not business_name:
        logger.warning("No business name provided for AI Overview extraction")
        return None

    # Build search query
    query = f"{business_name} auto service promotions discounts"
    location = competitor.get("address", "Edmonton, AB, Canada")

    # Fetch AI Overview
    overview_data = get_ai_overview(query, location)
    if not overview_data:
        logger.warning(f"Could not fetch AI Overview for {business_name}")
        return None

    ai_overview_text = overview_data.get("ai_overview", "")
    full_data = overview_data.get("full_data", {})

    if not ai_overview_text:
        logger.warning(f"AI Overview text is empty for {business_name}")
        return None

    # Extract business info
    business_info = extract_business_info_from_serpapi(full_data)

    # Determine if discounts are mentioned
    overview_lower = ai_overview_text.lower()
    has_discounts = any(keyword in overview_lower for keyword in [
        "discount", "promotion", "special", "offer", "deal", "sale",
        "coupon", "rebate", "save", "free", "% off", "$ off"
    ])

    # Build promo_description
    if has_discounts:
        # Extract discount-related sentences
        sentences = ai_overview_text.split(". ")
        discount_sentences = [
            s for s in sentences
            if any(kw in s.lower() for kw in ["discount", "promotion", "special", "offer", "deal", "sale", "coupon", "rebate", "save", "free"])
        ]
        if discount_sentences:
            promo_description = ". ".join(discount_sentences[:3])  # First 3 discount-related sentences
        else:
            promo_description = ai_overview_text[:500]
    else:
        promo_description = "No active discounts found; highlighting key service benefits."

    # Extract service_name from overview
    service_keywords = ["oil change", "brake", "tire", "battery", "inspection", "service"]
    service_name = "auto service"
    for keyword in service_keywords:
        if keyword in overview_lower:
            service_name = keyword
            break

    # Build offer_details - ensure it's clean text, not JSON
    if has_discounts:
        offer_details = ai_overview_text[:1000]
    else:
        # Extract service benefits
        sentences = ai_overview_text.split(". ")
        service_sentences = [
            s for s in sentences
            if any(kw in s.lower() for kw in ["service", "repair", "maintenance", "inspection", "quality", "professional"])
        ]
        if service_sentences:
            offer_details = ". ".join(service_sentences[:5])  # First 5 service-related sentences
        else:
            offer_details = ai_overview_text[:1000]

    # Ensure offer_details is a string, not dict or other type
    if not isinstance(offer_details, str):
        offer_details = str(offer_details)[:1000]

    # Build ad_title and ad_text
    ad_title = f"{business_name} Services"
    ad_text = ai_overview_text[:500]

    # Use business info from SerpAPI or fallback to competitor data
    final_business_name = business_info.get("business_name") or business_name
    google_reviews = business_info.get("google_reviews")
    contact = business_info.get("contact") or competitor.get("address", "")
    location_addr = business_info.get("location") or competitor.get("address", "")
    website = business_info.get("website") or competitor.get("domain", "")

    # Build standardized promo object
    # Use competitor data but override with SerpAPI data if available
    promo_competitor = {
        "name": final_business_name,
        "domain": website or competitor.get("domain", ""),
        "address": contact or competitor.get("address", "")
    }

    # Get promo_url from competitor, or use website URL
    promo_url = competitor.get("url", "") or competitor.get("promo_links", [""])[0] if competitor.get("promo_links") else ""
    if not promo_url:
        promo_url = website or competitor.get("domain", "")

    promo = build_standard_promo(
        competitor=promo_competitor,
        promo_url=promo_url,
        service_name=service_name,
        promo_description=promo_description,
        category="auto service",
        offer_details=offer_details,
        ad_title=ad_title,
        ad_text=ad_text,
        google_reviews=google_reviews,  # Can be None, which is valid
        existing_promo=None  # Always NEW for AI Overview fallback
    )

    logger.info(f"Generated fallback promo from AI Overview for {business_name}")
    return promo

