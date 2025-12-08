"""Unified extraction flow: Firecrawl first, then AI Overview fallback."""
from typing import Dict, List, Optional, Callable
from app.extractors.firecrawl.firecrawl_client import fetch_with_firecrawl
from app.extractors.serpapi.business_overview_extractor import extract_promo_from_ai_overview
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)


def is_mr_lube(competitor: Dict) -> bool:
    """Check if competitor is Mr. Lube."""
    name = competitor.get("name", "").lower()
    return "mr" in name and "lube" in name


def has_valid_promotions(promos: List[Dict]) -> bool:
    """
    Check if promotions list has valid promotions with required fields.

    Args:
        promos: List of promotion dictionaries

    Returns:
        True if valid promotions found, False otherwise
    """
    if not promos or len(promos) == 0:
        return False

    # Check if at least one promo has required fields
    required_fields = [
        "service_name", "promo_description", "offer_details",
        "ad_title", "ad_text"
    ]

    for promo in promos:
        # Check if all required fields are present and not empty
        has_all_fields = all(
            promo.get(field) and str(promo.get(field)).strip()
            for field in required_fields
        )

        if has_all_fields:
            return True

    return False


def check_firecrawl_for_promo_section(competitor: Dict) -> bool:
    """
    Check if website has a promo section by fetching with Firecrawl.

    Args:
        competitor: Competitor data dict

    Returns:
        True if promo section found, False otherwise
    """
    promo_links = competitor.get("promo_links", [])
    if not promo_links:
        # Try main URL
        promo_links = [competitor.get("url", "")]

    for url in promo_links[:1]:  # Check first URL only
        if not url:
            continue

        try:
            logger.info(f"Checking {competitor.get('name')} for promo section: {url}")
            result = fetch_with_firecrawl(url, timeout=30)

            if result.get("error"):
                logger.warning(f"Firecrawl error checking promo section: {result['error']}")
                return False

            html = result.get("html", "")
            if not html:
                logger.warning(f"No HTML content from Firecrawl for {url}")
                return False

            # Check for promo-related keywords in HTML
            html_lower = html.lower()
            promo_keywords = [
                "promo", "promotion", "coupon", "discount", "offer",
                "special", "deal", "rebate", "sale"
            ]

            has_promo_keywords = any(keyword in html_lower for keyword in promo_keywords)

            if has_promo_keywords:
                logger.info(f"Found promo section indicators in {url}")
                return True
            else:
                logger.info(f"No promo section found in {url}")
                return False

        except Exception as e:
            logger.warning(f"Error checking promo section: {e}")
            return False

    return False


def unified_extraction_flow(
    competitor: Dict,
    firecrawl_extractor: Callable[[Dict], List[Dict]]
) -> List[Dict]:
    """
    Unified extraction flow:
    1. For MR LUBE: Always use AI Overview (skip Firecrawl)
    2. For others: Try Firecrawl first, then AI Overview fallback if needed

    Args:
        competitor: Competitor data dict
        firecrawl_extractor: Function that takes competitor dict and returns list of promos

    Returns:
        List of promotion dictionaries
    """
    competitor_name = competitor.get("name", "")

    # Special handling for MR LUBE: Always use AI Overview
    if is_mr_lube(competitor):
        logger.info(f"MR LUBE detected: Using AI Overview as primary method (skipping Firecrawl)")
        ai_overview_promo = extract_promo_from_ai_overview(competitor)
        if ai_overview_promo:
            return [ai_overview_promo]
        else:
            logger.warning(f"AI Overview failed for {competitor_name}, returning empty list")
            return []

    # For other competitors: Try Firecrawl first
    logger.info(f"Attempting Firecrawl extraction for {competitor_name}")

    try:
        promos = firecrawl_extractor(competitor)

        # Check if Firecrawl found valid promotions
        if has_valid_promotions(promos):
            logger.info(f"Firecrawl extraction successful: Found {len(promos)} valid promotions")
            return promos

        # Firecrawl didn't find valid promotions - check why
        logger.warning(f"Firecrawl extraction found {len(promos)} promotions, but none are valid")

        # Check if website has promo section
        has_promo_section = check_firecrawl_for_promo_section(competitor)

        if not has_promo_section:
            logger.info(f"No promo section found on website, using AI Overview fallback")
        else:
            logger.info(f"Promo section exists but no valid promotions extracted, using AI Overview fallback")

        # Use AI Overview fallback
        ai_overview_promo = extract_promo_from_ai_overview(competitor)
        if ai_overview_promo:
            logger.info(f"AI Overview fallback successful for {competitor_name}")
            return [ai_overview_promo]
        else:
            logger.warning(f"AI Overview fallback also failed for {competitor_name}")
            return promos  # Return whatever Firecrawl found, even if invalid

    except Exception as e:
        logger.error(f"Error in Firecrawl extraction for {competitor_name}: {e}")
        logger.info(f"Using AI Overview fallback due to error")

        # Try AI Overview fallback
        ai_overview_promo = extract_promo_from_ai_overview(competitor)
        if ai_overview_promo:
            logger.info(f"AI Overview fallback successful after error")
            return [ai_overview_promo]
        else:
            logger.error(f"AI Overview fallback also failed after error")
            return []


def format_for_google_sheets(promo: Dict) -> Dict:
    """
    Format promotion dict to match Google Sheets column order.

    The exact column order from the sheet:
    1. website
    2. page_url
    3. business_name
    4. google_reviews
    5. service_name
    6. promo_description
    7. category
    8. contact
    9. location
    10. offer_details
    11. ad_title
    12. ad_text
    13. new_or_updated
    14. date_scraped

    Returns:
        Dict with fields in exact Google Sheets order
    """
    return {
        "website": promo.get("website", ""),
        "page_url": promo.get("page_url", ""),
        "business_name": promo.get("business_name", ""),
        "google_reviews": promo.get("google_reviews"),
        "service_name": promo.get("service_name", ""),
        "promo_description": promo.get("promo_description", ""),
        "category": promo.get("category", ""),
        "contact": promo.get("contact", ""),
        "location": promo.get("location", ""),
        "offer_details": promo.get("offer_details", ""),
        "ad_title": promo.get("ad_title", ""),
        "ad_text": promo.get("ad_text", ""),
        "new_or_updated": promo.get("new_or_updated", "NEW"),
        "date_scraped": promo.get("date_scraped", "")
    }

