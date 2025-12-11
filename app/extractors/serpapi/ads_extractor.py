"""Google Ads extractor for competitors."""
import re
from typing import List, Dict, Optional
from app.extractors.serpapi.serpapi_client import get_google_ads
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)

# Discount symbols and keywords
DISCOUNT_SYMBOLS = ["$", "%", "off", "save", "discount", "deal"]

# Promo words
PROMO_WORDS = ["special", "promo", "promotion", "coupon", "rebate", "bonus"]

# Free offers
FREE_WORDS = ["free", "complimentary"]

# Limited-time words
LIMITED_TIME_WORDS = ["sale", "limited time", "special offer"]

# All promo keywords combined
ALL_PROMO_KEYWORDS = DISCOUNT_SYMBOLS + PROMO_WORDS + FREE_WORDS + LIMITED_TIME_WORDS


def extract_coupon_code(text: str) -> Optional[str]:
    """
    Extract coupon code (alphanumeric 3+ characters) from text.

    Args:
        text: Text to search

    Returns:
        Coupon code if found, None otherwise
    """
    # Common words to exclude (not coupon codes)
    excluded_words = {
        "code", "coupon", "promo", "use", "enter", "apply", "mention", "mentioned",
        "available", "checkout", "discount", "save", "off", "special", "offer",
        "here", "for", "with", "the", "and", "or", "at", "on", "in"
    }

    # Pattern 1: "use code SAVE20" or "enter promo code WINTER25" (most specific first)
    # Handle "promo code" as a phrase - must come before other patterns
    use_code_pattern = r'(?:use|enter|apply)[:\s]+(?:promo[:\s]+)?(?:code|coupon)[:\s]+([A-Z0-9]{3,})(?:\s|$|[.,;!?])'
    matches = re.findall(use_code_pattern, text, re.IGNORECASE)
    if matches:
        for match in matches:
            match_upper = match.upper()
            if len(match) >= 3 and match_upper not in excluded_words:
                return match_upper

    # Pattern 2: "code: SAVE20" or "coupon: PROMO2024" (with colon - exclude "promo" to avoid false matches)
    # Only match if followed by colon (more specific)
    code_pattern = r'(?:code|coupon):\s+([A-Z0-9]{3,})(?:\s|$|[.,;!?])'
    matches = re.findall(code_pattern, text, re.IGNORECASE)
    if matches:
        for match in matches:
            match_upper = match.upper()
            if len(match) >= 3 and match_upper not in excluded_words:
                return match_upper

    # Pattern 3: Standalone alphanumeric codes (2+ letters followed by 2+ digits)
    # This should be more specific - letters then numbers, not just any alphanumeric
    standalone_pattern = r'\b([A-Z]{2,}\d{2,})\b'
    matches = re.findall(standalone_pattern, text, re.IGNORECASE)
    if matches:
        for match in matches:
            match_upper = match.upper()
            if len(match) >= 3 and match_upper not in excluded_words:
                return match_upper

    return None


def extract_discount_value(text: str) -> Optional[str]:
    """
    Extract discount value from text (e.g., "$10", "15%", "free").

    Args:
        text: Text to search

    Returns:
        Discount value if found, None otherwise
    """
    # Pattern for dollar amounts: $10, $20 off, save $50
    dollar_pattern = r'\$(\d+(?:\.\d+)?)'
    dollar_matches = re.findall(dollar_pattern, text, re.IGNORECASE)
    if dollar_matches:
        # Return the largest dollar amount found
        amounts = [float(m) for m in dollar_matches]
        max_amount = max(amounts)
        if max_amount == int(max_amount):
            return f"${int(max_amount)}"
        return f"${max_amount}"

    # Pattern for percentages: 15%, 20% off, save 25%
    percent_pattern = r'(\d+)%'
    percent_matches = re.findall(percent_pattern, text, re.IGNORECASE)
    if percent_matches:
        # Return the largest percentage found
        percents = [int(p) for p in percent_matches]
        return f"{max(percents)}%"

    # Check for "free" or "complimentary"
    if re.search(r'\bfree\b', text, re.IGNORECASE):
        return "Free"

    if re.search(r'\bcomplimentary\b', text, re.IGNORECASE):
        return "Complimentary"

    return None


def has_promo_content(ad: Dict) -> bool:
    """
    Check if an ad contains promotional language (discounts, coupons, deals).
    STRICT FILTERING: Only includes ads with specific discount/promo keywords.

    Args:
        ad: Ad dictionary from SerpAPI

    Returns:
        True if ad contains required promo keywords, False otherwise
    """
    # Extract title and description (primary fields to check)
    title = ad.get("title", "").lower()
    snippet = ad.get("snippet", "").lower()
    description = ad.get("description", "").lower()

    # Combine title and description for checking
    combined_text = f"{title} {snippet} {description}".strip()

    # If no text at all, skip
    if not combined_text:
        return False

    # EXCLUSION: Check if ad is informational only
    # Exclude if it's just business name + location with no offer
    business_name = ad.get("business_name", "").lower()
    if business_name:
        # Check if text is mostly just business name and location words
        location_words = ["edmonton", "canada", "alberta", "location", "address", "contact", "phone"]
        text_words = set(combined_text.split())
        business_words = set(business_name.split())
        location_words_set = set(location_words)

        # If text only contains business name and location words, exclude
        if text_words.issubset(business_words.union(location_words_set)):
            return False

    # INCLUSION: Check for required promo keywords in title OR description
    has_discount_symbol = any(symbol in combined_text for symbol in DISCOUNT_SYMBOLS)
    has_promo_word = any(word in combined_text for word in PROMO_WORDS)
    has_free_word = any(word in combined_text for word in FREE_WORDS)
    has_limited_time = any(phrase in combined_text for phrase in LIMITED_TIME_WORDS)

    # Check for coupon codes (alphanumeric 3+ characters)
    has_coupon_code = extract_coupon_code(combined_text) is not None

    # Include ad if it has at least one of the required indicators
    if has_discount_symbol or has_promo_word or has_free_word or has_limited_time or has_coupon_code:
        return True

    return False


def normalize_text_for_dedup(text: str) -> str:
    """
    Normalize text for duplicate comparison (lowercase, remove extra spaces).

    Args:
        text: Text to normalize

    Returns:
        Normalized text
    """
    if not text:
        return ""
    return " ".join(text.lower().split())


def extract_discount_phrase(text: str) -> Optional[str]:
    """
    Extract discount phrase pattern from text (e.g., "$20 off", "15% discount", "save $50").

    Args:
        text: Text to search

    Returns:
        Discount phrase pattern if found, None otherwise
    """
    # Look for common discount phrase patterns
    patterns = [
        r'\$\d+(?:\.\d+)?\s+off',
        r'\d+%\s+off',
        r'save\s+\$\d+(?:\.\d+)?',
        r'save\s+\d+%',
        r'\$\d+(?:\.\d+)?\s+discount',
        r'\d+%\s+discount',
        r'free\s+\w+',
        r'complimentary\s+\w+',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).lower()

    return None


def calculate_promo_score(ad: Dict) -> int:
    """
    Calculate promotional score for an ad (higher = more promotional).

    Criteria:
    1. Highest discount value (numerical)
    2. Presence of coupon code
    3. Strong promo keywords ("sale", "offer", "save", "free")

    Args:
        ad: Ad dictionary

    Returns:
        Promotional score (higher is better)
    """
    score = 0

    # Extract text
    title = ad.get("title", "").lower()
    snippet = ad.get("snippet", "").lower()
    description = ad.get("description", "").lower()
    combined_text = f"{title} {snippet} {description}"

    # 1. Discount value (higher discount = higher score)
    discount_value = extract_discount_value(combined_text)
    if discount_value:
        if discount_value.startswith("$"):
            try:
                amount = float(discount_value.replace("$", ""))
                score += int(amount)  # $50 = 50 points
            except:
                score += 10
        elif discount_value.endswith("%"):
            try:
                percent = int(discount_value.replace("%", ""))
                score += percent  # 25% = 25 points
            except:
                score += 10
        elif discount_value.lower() in ["free", "complimentary"]:
            score += 50  # Free is highly promotional

    # 2. Coupon code presence
    coupon_code = extract_coupon_code(combined_text)
    if coupon_code:
        score += 30  # Having a code is very promotional

    # 3. Strong promo keywords
    strong_keywords = ["sale", "offer", "save", "free", "limited", "special offer"]
    for keyword in strong_keywords:
        if keyword in combined_text:
            score += 10

    return score


def deduplicate_ads(ads: List[Dict]) -> List[Dict]:
    """
    Remove duplicate ads based on:
    - Exact match of (title + description)
    - Same discount phrase pattern

    If more than 2 ads match, keep the MOST promotional ones.

    Args:
        ads: List of ad dictionaries

    Returns:
        Deduplicated list of ads
    """
    if not ads:
        return []

    # Create a mapping of ad to its groups
    ad_to_groups = {}

    # Group ads by (title + description) exact match
    title_desc_groups = {}
    for ad in ads:
        title = ad.get("title", "")
        snippet = ad.get("snippet", "")
        description = ad.get("description", "")
        combined = f"{title} {snippet} {description}"
        key = normalize_text_for_dedup(combined)

        if key not in title_desc_groups:
            title_desc_groups[key] = []
        title_desc_groups[key].append(ad)

        # Track which group this ad belongs to
        ad_id = id(ad)
        if ad_id not in ad_to_groups:
            ad_to_groups[ad_id] = []
        ad_to_groups[ad_id].append(("title_desc", key))

    # Also group by discount phrase pattern
    discount_phrase_groups = {}
    for ad in ads:
        title = ad.get("title", "")
        snippet = ad.get("snippet", "")
        description = ad.get("description", "")
        combined = f"{title} {snippet} {description}"
        discount_phrase = extract_discount_phrase(combined)

        if discount_phrase:
            if discount_phrase not in discount_phrase_groups:
                discount_phrase_groups[discount_phrase] = []
            discount_phrase_groups[discount_phrase].append(ad)

            # Track which group this ad belongs to
            ad_id = id(ad)
            if ad_id not in ad_to_groups:
                ad_to_groups[ad_id] = []
            ad_to_groups[ad_id].append(("discount_phrase", discount_phrase))

    # Find all duplicate groups (groups with more than 1 ad)
    duplicate_groups = []

    # Check title+description groups
    for key, group in title_desc_groups.items():
        if len(group) > 1:
            duplicate_groups.append(group)

    # Check discount phrase groups
    for phrase, group in discount_phrase_groups.items():
        if len(group) > 1:
            # Only add if not already covered by title+description group
            # Check if all ads in this group are in the same title+description group
            title_desc_keys = set()
            for ad in group:
                ad_id = id(ad)
                for group_type, group_key in ad_to_groups.get(ad_id, []):
                    if group_type == "title_desc":
                        title_desc_keys.add(group_key)

            # If ads span multiple title+description groups, this is a separate duplicate group
            if len(title_desc_keys) > 1 or not title_desc_keys:
                duplicate_groups.append(group)

    # Process duplicate groups - keep most promotional ads (up to 2 per group)
    seen_ads = set()
    deduplicated = []

    # First, add all non-duplicate ads
    for ad in ads:
        ad_id = id(ad)
        is_duplicate = False
        for group in duplicate_groups:
            if ad in group:
                is_duplicate = True
                break

        if not is_duplicate:
            if ad_id not in seen_ads:
                seen_ads.add(ad_id)
                deduplicated.append(ad)

    # Then, process duplicate groups
    for group in duplicate_groups:
        # Score all ads in the group
        scored_ads = [(calculate_promo_score(ad), ad) for ad in group]
        scored_ads.sort(reverse=True, key=lambda x: x[0])  # Sort by score descending

        # Keep top 2 most promotional
        for score, ad in scored_ads[:2]:
            ad_id = id(ad)
            if ad_id not in seen_ads:
                seen_ads.add(ad_id)
                deduplicated.append(ad)

    return deduplicated


def extract_ads_for_competitor(competitor: Dict, limit: int = 2) -> List[Dict]:
    """
    Extract Google Ads for a competitor following the specified flow:
    1. Build query string "{business_name} Edmonton, Canada"
    2. Fetch SerpAPI ads
    3. Filter only promotional ads
    4. Extract required data fields
    5. Deduplicate
    6. Limit to 2
    7. Return list (will be written to sheet separately)

    Args:
        competitor: Competitor dictionary with name, address, etc.
        limit: Maximum number of ads to return (default: 2)

    Returns:
        List of ad dictionaries with promotional content (max 2)
    """
    business_name = competitor.get("name", "")
    if not business_name:
        logger.warning("No business name provided for ads extraction")
        return []

    # Step 1: Build query string "{business_name} Edmonton, Canada"
    query = f"{business_name} Edmonton, Canada"
    location = "Edmonton, Canada"

    logger.info(f"Extracting Google Ads for {business_name}")

    # Step 2: Fetch SerpAPI ads
    ads = get_google_ads(business_name, location)

    if not ads:
        logger.info(f"No ads found for {business_name}")
        return []

    # Step 3: Filter only promotional ads
    promo_ads = []
    for ad in ads:
        if has_promo_content(ad):
            # Add business_name to ad dict for sheet writing
            ad_with_metadata = ad.copy()
            ad_with_metadata["business_name"] = business_name
            promo_ads.append(ad_with_metadata)

    if not promo_ads:
        logger.info(f"No promotional ads found for {business_name}")
        return []

    # Step 4: Extract required data fields (done in format_ad_for_sheets)
    # Step 5: Deduplicate
    deduplicated_ads = deduplicate_ads(promo_ads)

    # Step 6: Limit to 2
    final_ads = deduplicated_ads[:limit]

    logger.info(f"Found {len(final_ads)} promotional ads for {business_name} (after deduplication)")
    return final_ads


def extract_ads_for_all_competitors(competitors: List[Dict], limit: int = 2) -> List[Dict]:
    """
    Extract Google Ads for all competitors.
    Skips competitors with 0 valid ads.

    Args:
        competitors: List of competitor dictionaries
        limit: Maximum number of ads per competitor (default: 2)

    Returns:
        List of all ad dictionaries from all competitors
    """
    all_ads = []

    for competitor in competitors:
        try:
            ads = extract_ads_for_competitor(competitor, limit=limit)
            if ads:  # Only add if we have valid ads (skip if 0)
                all_ads.extend(ads)
            else:
                logger.info(f"Skipping {competitor.get('name', 'Unknown')} - 0 valid ads")
        except Exception as e:
            logger.error(f"Error extracting ads for {competitor.get('name', 'Unknown')}: {e}")
            continue

    logger.info(f"Total promotional ads extracted: {len(all_ads)}")
    return all_ads

