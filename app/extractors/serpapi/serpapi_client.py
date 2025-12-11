"""SerpAPI client for fetching AI Overview and business information."""
import os
import requests
from typing import Dict, Optional
from app.config.constants import SERPAPI_KEY
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)

SERPAPI_URL = "https://serpapi.com/search"


def get_ai_overview(query: str, location: str = "Edmonton, AB, Canada") -> Optional[Dict]:
    """
    Fetch AI Overview from SerpAPI for a business query.

    Args:
        query: Business name or search query
        location: Location string (default: Edmonton, AB, Canada)

    Returns:
        Dict with AI Overview data or None if failed
    """
    if not SERPAPI_KEY:
        logger.warning("SERPAPI_KEY not set, cannot fetch AI Overview")
        return None

    try:
        # Build query with location if provided
        search_query = query
        if location and "edmonton" in location.lower():
            search_query = f"{query} Edmonton Canada"

        params = {
            "q": search_query,
            "api_key": SERPAPI_KEY,
            "engine": "google",
            "hl": "en",
            "gl": "ca"
        }

        logger.info(f"Fetching AI Overview for: {search_query}")
        response = requests.get(SERPAPI_URL, params=params, timeout=30)

        # Better error handling
        if response.status_code != 200:
            try:
                error_data = response.json()
                error_msg = error_data.get("error", str(error_data))
                logger.error(f"SerpAPI error details: {error_msg}")
            except:
                error_msg = response.text[:500]
                logger.error(f"SerpAPI error response: {error_msg}")
            response.raise_for_status()

        data = response.json()

        # Extract AI Overview - SerpAPI returns it in different formats
        ai_overview_text = ""

        # Try ai_overview field (new format)
        ai_overview = data.get("ai_overview")
        if ai_overview:
            if isinstance(ai_overview, dict):
                ai_overview_text = ai_overview.get("text", "") or ai_overview.get("answer", "") or str(ai_overview)
            elif isinstance(ai_overview, str):
                ai_overview_text = ai_overview

        # Try answer_box (alternative format)
        if not ai_overview_text:
            answer_box = data.get("answer_box", {})
            if answer_box:
                ai_overview_text = answer_box.get("text", "") or answer_box.get("answer", "") or str(answer_box)

        # Try knowledge_graph description
        if not ai_overview_text:
            knowledge_graph = data.get("knowledge_graph", {})
            if knowledge_graph:
                ai_overview_text = knowledge_graph.get("description", "") or knowledge_graph.get("about", {}).get("text", "") if isinstance(knowledge_graph.get("about"), dict) else ""

        # Try organic results snippet as fallback
        if not ai_overview_text:
            organic_results = data.get("organic_results", [])
            if organic_results:
                snippets = [r.get("snippet", "") for r in organic_results[:3] if r.get("snippet")]
                if snippets:
                    ai_overview_text = " ".join(snippets)

        if ai_overview_text:
            logger.info(f"Successfully fetched AI Overview for {query}")
            return {
                "ai_overview": ai_overview_text,
                "full_data": data  # Store full response for additional extraction
            }
        else:
            logger.warning(f"No AI Overview found in SerpAPI response for {query}")
            return None

    except Exception as e:
        logger.error(f"Error fetching AI Overview from SerpAPI: {e}")
        return None


def extract_business_info_from_serpapi(data: Dict) -> Dict:
    """
    Extract business information from SerpAPI response.

    Returns:
        Dict with business_name, google_reviews, contact, location, etc.
    """
    result = {
        "business_name": None,
        "google_reviews": None,
        "contact": None,
        "location": None,
        "website": None
    }

    try:
        # Try knowledge_graph first
        knowledge_graph = data.get("knowledge_graph", {})
        if knowledge_graph:
            result["business_name"] = knowledge_graph.get("title", "")
            result["website"] = knowledge_graph.get("website", "")

            # Extract rating
            rating = knowledge_graph.get("rating")
            if rating:
                try:
                    result["google_reviews"] = float(rating)
                except (ValueError, TypeError):
                    pass

            # Extract address
            address = knowledge_graph.get("address")
            if address:
                result["location"] = address
                result["contact"] = address

        # Try organic results for additional info
        organic_results = data.get("organic_results", [])
        if organic_results and not result["business_name"]:
            first_result = organic_results[0]
            result["business_name"] = first_result.get("title", "")
            result["website"] = first_result.get("link", "")

        # Try local_results
        local_results = data.get("local_results", {})
        if local_results:
            if not result["business_name"]:
                result["business_name"] = local_results.get("title", "")
            if not result["location"]:
                result["location"] = local_results.get("address", "")
            if not result["contact"]:
                result["contact"] = local_results.get("address", "")
            rating = local_results.get("rating")
            if rating and not result["google_reviews"]:
                try:
                    result["google_reviews"] = float(rating)
                except (ValueError, TypeError):
                    pass

    except Exception as e:
        logger.warning(f"Error extracting business info from SerpAPI: {e}")

    return result

