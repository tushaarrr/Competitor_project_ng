"""SerpAPI client for fetching AI Overview and business information."""
import os
import requests
from typing import Dict, Optional, List
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


def get_google_ads(query: str, location: str = "Edmonton, Canada") -> Optional[List[Dict]]:
    """
    Fetch Google Ads from SerpAPI for a business query.

    Args:
        query: Business name or search query
        location: Location string (default: Edmonton, Canada)

    Returns:
        List of ad dictionaries or None if failed
    """
    if not SERPAPI_KEY:
        logger.warning("SERPAPI_KEY not set, cannot fetch Google Ads")
        return None

    try:
        # Build query with location
        search_query = f"{query} {location}"

        params = {
            "q": search_query,
            "api_key": SERPAPI_KEY,
            "engine": "google",
            "hl": "en",
            "gl": "ca",
            "no_cache": "true"  # Force fresh results
        }

        logger.info(f"Fetching Google Ads for: {search_query}")
        response = requests.get(SERPAPI_URL, params=params, timeout=30)

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

        # Extract ads from multiple possible fields
        ads = []

        # Try response.ads (primary field)
        if "ads" in data:
            ads_list = data.get("ads", [])
            if isinstance(ads_list, list):
                ads.extend(ads_list)

        # Try ad_results (alternative field name)
        if "ad_results" in data:
            ad_results = data.get("ad_results", [])
            if isinstance(ad_results, list):
                ads.extend(ad_results)

        # Try sponsored_results (another possible field)
        if "sponsored_results" in data:
            sponsored = data.get("sponsored_results", [])
            if isinstance(sponsored, list):
                ads.extend(sponsored)

        # Check organic_results for sponsored items (ads often appear here)
        # Also check top 3 results as they might be ads even if not marked
        if "organic_results" in data:
            organic = data.get("organic_results", [])
            if isinstance(organic, list):
                for i, item in enumerate(organic):
                    # Check if item is marked as sponsored/ad
                    is_sponsored = (
                        item.get("sponsored") or
                        item.get("ad") or
                        item.get("type") == "ad" or
                        "Ad" in str(item.get("position", "")) or
                        item.get("link_type") == "sponsored"
                    )

                    # Also check top 3 positions - sometimes ads appear there without explicit marking
                    # Look for indicators like "Ad" in snippet, or specific ad-like structure
                    is_top_position = i < 3
                    has_ad_indicators = False
                    if is_top_position:
                        snippet = item.get("snippet", "").lower()
                        title = item.get("title", "").lower()
                        # Check for ad-like patterns
                        ad_patterns = ["ad", "sponsored", "promoted", "advertisement"]
                        has_ad_indicators = any(pattern in snippet or pattern in title for pattern in ad_patterns)

                    if is_sponsored or (is_top_position and has_ad_indicators):
                        ads.append(item)

        # Try shopping_results (sometimes contains ads)
        if "shopping_results" in data:
            shopping = data.get("shopping_results", [])
            if isinstance(shopping, list):
                # Filter for sponsored/promoted items
                for item in shopping:
                    if item.get("sponsored") or item.get("promoted"):
                        ads.append(item)

        # Check top_stories or answer_box for ads
        if "top_stories" in data:
            top_stories = data.get("top_stories", [])
            if isinstance(top_stories, list):
                for item in top_stories:
                    if item.get("sponsored") or item.get("ad"):
                        ads.append(item)

        # Check local_results for sponsored listings
        if "local_results" in data:
            local_results = data.get("local_results", [])
            if isinstance(local_results, list):
                for item in local_results:
                    if item.get("sponsored") or item.get("ad"):
                        ads.append(item)
            elif isinstance(local_results, dict):
                # Sometimes local_results is a single dict
                if local_results.get("sponsored") or local_results.get("ad"):
                    ads.append(local_results)

        # Check knowledge_graph for sponsored info
        if "knowledge_graph" in data:
            kg = data.get("knowledge_graph", {})
            if isinstance(kg, dict) and (kg.get("sponsored") or kg.get("ad")):
                ads.append(kg)

        if ads:
            logger.info(f"Found {len(ads)} ads for {query}")
            return ads
        else:
            logger.info(f"No ads found for {query}")
            # Log available fields for debugging (first run only)
            if logger.level <= 10:  # DEBUG level
                available_fields = [k for k in data.keys() if k not in ["search_metadata", "search_parameters"]]
                logger.debug(f"Available fields in response: {available_fields[:10]}")
            return []

    except Exception as e:
        logger.error(f"Error fetching Google Ads from SerpAPI: {e}")
        return None

