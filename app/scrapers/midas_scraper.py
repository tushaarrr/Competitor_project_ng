"""Midas scraper - Text-based HTML extraction only."""
import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import re
from fuzzywuzzy import fuzz

from app.extractors.firecrawl.firecrawl_client import fetch_with_firecrawl
from app.extractors.ocr.llm_cleaner import clean_promo_text_with_llm
from app.config.constants import DATA_DIR, PROMO_KEYWORDS
from app.utils.logging_utils import setup_logger
from app.utils.promo_builder import build_standard_promo, load_existing_promos, get_google_reviews_for_competitor

logger = setup_logger(__name__, "midas_scraper.log")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMOTIONS_DIR = DATA_DIR / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)


def fetch_with_fallback(url: str) -> str:
    """Fetch HTML using Firecrawl (Markdown + HTML mode), fallback to ZenRows/ScraperAPI."""
    # For rebates page, prefer ZenRows with JS rendering as it may have dynamic content
    if "rebates" in url.lower():
        try:
            from app.config.constants import ZENROWS_API_KEY
            if ZENROWS_API_KEY:
                import requests
                zenrows_url = f"https://api.zenrows.com/v1/?apikey={ZENROWS_API_KEY}&url={url}&js_render=true&wait=3000&premium_proxy=true"
                response = requests.get(zenrows_url, timeout=45)
                response.raise_for_status()
                logger.info("Successfully fetched rebates page with ZenRows (JS rendering)")
                return response.text
        except Exception as e:
            logger.warning(f"ZenRows with JS failed, trying Firecrawl: {e}")

    # Try Firecrawl first - request both HTML and Markdown
    firecrawl_result = fetch_with_firecrawl(url, timeout=90)

    if firecrawl_result.get("html") and not firecrawl_result.get("error"):
        logger.info("Successfully fetched with Firecrawl")
        return firecrawl_result.get("html", "")

    logger.warning("Firecrawl failed, trying fallback methods...")

    # Fallback to ZenRows
    try:
        from app.config.constants import ZENROWS_API_KEY
        if ZENROWS_API_KEY:
            import requests
            zenrows_url = f"https://api.zenrows.com/v1/?apikey={ZENROWS_API_KEY}&url={url}&js_render=true&wait=2000"
            response = requests.get(zenrows_url, timeout=30)
            response.raise_for_status()
            logger.info("Successfully fetched with ZenRows")
            return response.text
    except Exception as e:
        logger.warning(f"ZenRows fallback failed: {e}")

    # Fallback to ScraperAPI
    try:
        from app.config.constants import SCRAPERAPI_KEY
        if SCRAPERAPI_KEY:
            import requests
            scraperapi_url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={url}"
            response = requests.get(scraperapi_url, timeout=30)
            response.raise_for_status()
            logger.info("Successfully fetched with ScraperAPI")
            return response.text
    except Exception as e:
        logger.warning(f"ScraperAPI fallback failed: {e}")

    logger.error("All fetch methods failed")
    return ""


def extract_promo_blocks(html: str, url: str = "") -> List[Dict]:
    """Extract promotional text blocks from HTML - Canada rebates only."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Remove script and style elements
    for script in soup(["script", "style", "noscript"]):
        script.decompose()

    promo_blocks = []
    seen_texts = set()

    # For rebates page, focus on actual rebate offer cards/sections only
    if "rebates" in url.lower() and "country=ca" in url.lower():
        logger.info("Extracting Canada rebates from rebates page...")

        # Method 0: Look for all divs/sections with rebate-related classes or data attributes
        rebate_containers = soup.find_all(['div', 'section', 'article'],
                                         class_=re.compile(r'rebate|offer|promo|deal|special', re.IGNORECASE))
        for container in rebate_containers:
            text = container.get_text(separator=" ", strip=True)
            if text and len(text) > 40:
                # Check for rebate indicators
                has_amount = bool(re.search(r'\$\d+', text))
                has_rebate_keyword = bool(re.search(r'rebate|back|save|off|discount', text, re.IGNORECASE))
                mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)|\bUnited States\b(?!.*Canada)', text, re.IGNORECASE))
                generic_keywords = ["partners with suppliers", "best products at the best value", "filter by country"]
                is_generic = any(keyword in text.lower() for keyword in generic_keywords)

                if (has_amount or has_rebate_keyword) and not mentions_usa and not is_generic:
                    if 40 < len(text) < 2000:
                        text_hash = hash(text[:400])
                        if text_hash not in seen_texts:
                            seen_texts.add(text_hash)
                            promo_blocks.append({
                                "text": text,
                                "html": str(container)[:2000],
                                "selector": "rebate-container-class"
                            })
                            logger.info(f"Found rebate via container class: {text[:80]}... ({len(text)} chars)")

        # Method 1: Search full page text for rebate patterns (more reliable for JS-rendered content)
        full_text = soup.get_text(separator="\n")
        lines = [line.strip() for line in full_text.split("\n") if line.strip()]

        rebate_blocks = []
        current_block = []

        for line in lines:
            # More flexible patterns - any $ amount with rebate keywords or brand names
            if re.search(r'\$\d+.*?(?:Back|Rebate|Off|Save)|Get.*?\$\d+|Save.*?\$\d+|Rebate.*?\$\d+|Bridgestone|Firestone|Michelin|Goodyear|Continental|Pirelli|BFGoodrich|Toyo|Nitto|Hankook|Falken|Kumho|Yokohama|Dunlop|General|Cooper|Uniroyal', line, re.IGNORECASE):
                current_block.append(line)
            elif current_block:
                # Check if we have a valid rebate block
                block_text = " ".join(current_block)
                if re.search(r'\$\d+', block_text) and len(block_text) > 30:  # Lowered threshold
                    rebate_blocks.append(block_text)
                current_block = []

        # Also check for standalone rebate lines (more flexible patterns)
        for line in lines:
            # Any line with $ amount and rebate keywords
            if re.search(r'\$\d+.*?(?:Back|Rebate|Off|Save|Purchase)', line, re.IGNORECASE):
                if 30 < len(line) < 500:  # Lowered minimum length
                    rebate_blocks.append(line)

        for block_text in rebate_blocks:
            # Verify it's Canada-relevant (exclude USA-only)
            mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)|\bUnited States\b(?!.*Canada)', block_text, re.IGNORECASE))
            # Filter out generic marketing text
            generic_keywords = [
                "partners with suppliers", "best products at the best value",
                "bring the best products", "best value to you", "filter by country"
            ]
            is_generic = any(keyword in block_text.lower() for keyword in generic_keywords)

            if not mentions_usa and not is_generic:
                text_hash = hash(block_text[:400])
                if text_hash not in seen_texts:
                    seen_texts.add(text_hash)
                    promo_blocks.append({
                        "text": block_text,
                        "html": "",
                        "selector": "rebate-text-search"
                    })
                    logger.info(f"Found rebate via text search: {block_text[:80]}... ({len(block_text)} chars)")

        # Method 2: Look for rebate cards/containers by class/id attributes
        rebate_selectors = [
            '[class*="rebate"]',
            '[class*="offer"]',
            '[class*="promo"]',
            '[id*="rebate"]',
            '[id*="offer"]',
            'div[data-rebate]',
            'section[class*="rebate"]',
            'article[class*="rebate"]'
        ]

        for selector in rebate_selectors:
            try:
                elements = soup.select(selector)
                for elem in elements:
                    text = elem.get_text(separator=" ", strip=True)
                    if text and len(text) > 50:
                        # Check if it contains rebate indicators
                        has_rebate_amount = bool(re.search(r'\$\d+|Get.*?\$\d+.*?Back|up.*?to.*?\$\d+', text, re.IGNORECASE))
                        has_brand = bool(re.search(r'Bridgestone|Firestone|Michelin|Goodyear|Continental|Pirelli|BFGoodrich|Toyo|Nitto|Hankook|Falken|Kumho|Yokohama|Dunlop|General|Cooper|Uniroyal', text, re.IGNORECASE))
                        mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)|\bUnited States\b(?!.*Canada)', text, re.IGNORECASE))

                        # Filter out generic marketing text
                        generic_keywords = [
                            "partners with suppliers", "best products at the best value",
                            "bring the best products", "best value to you"
                        ]
                        is_generic = any(keyword in text.lower() for keyword in generic_keywords)

                        if (has_rebate_amount or has_brand) and not mentions_usa and not is_generic:
                            if 50 < len(text) < 2000:
                                text_hash = hash(text[:400])
                                if text_hash not in seen_texts:
                                    seen_texts.add(text_hash)
                                    promo_blocks.append({
                                        "text": text,
                                        "html": str(elem)[:2000],
                                        "selector": f"rebate-container-{selector}"
                                    })
                                    logger.info(f"Found rebate via container: {text[:80]}... ({len(text)} chars)")
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {e}")
                continue

        # Method 2b: Look for text nodes containing rebate patterns (less restrictive)
        all_text_nodes = soup.find_all(string=True)
        rebate_text_nodes = []

        for text_node in all_text_nodes:
            text = text_node.strip()
            # More flexible patterns - any mention of $ amount with rebate keywords
            if re.search(r'\$\d+.*?(?:Back|Rebate|Off|Save)|Get.*?\$\d+|Save.*?\$\d+|Rebate.*?\$\d+', text, re.IGNORECASE):
                if len(text) > 15 and len(text) < 500:  # Reasonable length for rebate text
                    rebate_text_nodes.append(text_node)

        # For each rebate text found, get its parent container
        for text_node in rebate_text_nodes:
            parent = text_node.find_parent()
            if parent:
                # Traverse up to find the rebate card/section container
                container = parent
                for _ in range(5):  # Check up to 5 levels up
                    if container:
                        classes = ' '.join(container.get('class', [])) if container.get('class') else ''
                        # Check if this looks like a rebate card
                        if any(word in classes.lower() for word in ['rebate', 'offer', 'card', 'promo', 'tile', 'deal', 'special']):
                            break
                        container = container.find_parent()
                    else:
                        break

                if not container:
                    container = parent

                rebate_text = container.get_text(separator=" ", strip=True)

                # Less restrictive: just needs rebate amount, no date/form required
                has_rebate_amount = bool(re.search(r'\$\d+.*?Back|Get.*?\$\d+.*?Back|up.*?to.*?\$\d+.*?Back|\$\d+.*?Rebate|\$\d+.*?Off|\$\d+.*?Save', rebate_text, re.IGNORECASE))
                mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)|\bUnited States\b(?!.*Canada)', rebate_text, re.IGNORECASE))

                if has_rebate_amount and not mentions_usa:
                    if 50 < len(rebate_text) < 2500:
                        text_hash = hash(rebate_text[:400])
                        if text_hash not in seen_texts:
                            seen_texts.add(text_hash)
                            promo_blocks.append({
                                "text": rebate_text,
                                "html": str(container)[:2000],
                                "selector": "rebate-text-pattern"
                            })
                            logger.info(f"Found rebate offer: {rebate_text[:80]}... ({len(rebate_text)} chars)")

        # Method 3: Look for brand names in headings and text (expanded brand list)
        brand_names = [
            'Bridgestone', 'Firestone', 'Michelin', 'Goodyear', 'Continental',
            'Pirelli', 'BFGoodrich', 'Toyo', 'Nitto', 'Hankook', 'Falken',
            'Kumho', 'Yokohama', 'Dunlop', 'General', 'Cooper', 'Uniroyal'
        ]
        for brand in brand_names:
            # Search in headings
            headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5'], string=re.compile(brand, re.IGNORECASE))
            for heading in headings:
                container = heading.find_parent(['section', 'div', 'article']) or heading.find_parent()
                if container:
                    text = container.get_text(separator=" ", strip=True)
                    # More flexible: any $ amount or rebate keyword
                    if re.search(r'\$\d+|Rebate|Back|Save|Off', text, re.IGNORECASE):
                        mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)|\bUnited States\b(?!.*Canada)', text, re.IGNORECASE))
                        if not mentions_usa and 50 < len(text) < 2000:
                            text_hash = hash(text[:400])
                            if text_hash not in seen_texts:
                                seen_texts.add(text_hash)
                                promo_blocks.append({
                                    "text": text,
                                    "html": str(container)[:2000],
                                    "selector": f"brand-heading-{brand}"
                                })
                                logger.info(f"Found rebate via brand heading {brand}: {text[:80]}... ({len(text)} chars)")

            # Also search for brand mentions in any text with rebate indicators
            brand_elements = soup.find_all(string=re.compile(brand, re.IGNORECASE))
            for brand_text_node in brand_elements:
                parent = brand_text_node.find_parent()
                if parent:
                    container = parent
                    for _ in range(3):
                        if container:
                            container_text = container.get_text(separator=" ", strip=True)
                            if re.search(r'\$\d+|Rebate|Back|Save', container_text, re.IGNORECASE):
                                mentions_usa = bool(re.search(r'\bUSA\b(?!.*Canada)', container_text, re.IGNORECASE))
                                if not mentions_usa and 50 < len(container_text) < 2000:
                                    text_hash = hash(container_text[:400])
                                    if text_hash not in seen_texts:
                                        seen_texts.add(text_hash)
                                        promo_blocks.append({
                                            "text": container_text,
                                            "html": str(container)[:2000],
                                            "selector": f"brand-text-{brand}"
                                        })
                                        logger.info(f"Found rebate via brand text {brand}: {container_text[:80]}... ({len(container_text)} chars)")
                                break
                            container = container.find_parent()
                        else:
                            break

        if promo_blocks:
            logger.info(f"Extracted {len(promo_blocks)} Canada rebate offers")
            return promo_blocks
        else:
            logger.warning("No rebate offers found on rebates page - may need JavaScript rendering")

    # For archive page, use more targeted extraction
    elif "archive" in url.lower():
        logger.info("Extracting promotions from archive page...")

        # Method 1: Look for featured monthly offers - prioritize large prices ($79, $89, $499, $599)
        # These typically appear in headings or large text, not fine print
        large_price_pattern = re.compile(r'\$\s*([7-9]\d{2}|\d{2,3})\b')  # $79, $89, $499, $599, etc.

        # Look for headings or large text elements containing large prices
        for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'div', 'p']:
            elements = soup.find_all(tag, string=large_price_pattern)
            for elem in elements:
                # Get parent container
                container = elem.find_parent(['section', 'div', 'article'])
                if not container:
                    container = elem.find_parent()

                if container:
                    text = container.get_text(separator=" ", strip=True)

                    # Must have large price AND promo keywords
                    has_large_price = bool(large_price_pattern.search(text))
                    has_promo_keywords = bool(re.search(
                        r'oil\s+change|tire|brake|buy.*get|free|special|offer|rotation|synthetic|euro|limited|lifetime|rovelo|full\s+stop',
                        text, re.IGNORECASE
                    ))

                    # Filter out fine print (mentions shop fee, disposal fee, tax, etc. without main promo)
                    is_fine_print = bool(re.search(
                        r'shop\s+fee|disposal\s+fee|plus\s+tax|void\s+where|not\s+valid|limited\s+time\s+offer',
                        text, re.IGNORECASE
                    )) and not bool(re.search(r'\$\s*([7-9]\d{2}|\d{2,3})\b', text))  # Has fine print but no large price

                    is_too_long = len(text) > 3000
                    is_too_short = len(text) < 50
                    is_generic = any(keyword in text.lower() for keyword in [
                        "we're in the business", "worry-free", "book online", "rating from reviews",
                        "why us", "experienced technicians", "unbeatable service", "convenient locations"
                    ])

                    if has_large_price and has_promo_keywords and not is_fine_print and not is_too_long and not is_too_short and not is_generic:
                        text_hash = hash(text[:400])
                        if text_hash not in seen_texts:
                            seen_texts.add(text_hash)
                            promo_blocks.append({
                                "text": text,
                                "html": str(container)[:2000],
                                "selector": "archive-large-price"
                            })
                            logger.info(f"Found archive promo (large price): {text[:80]}... ({len(text)} chars)")

        # Method 1b: Look for "Buy 3 Get 1 Free" and "Free" offers (may not have large prices)
        free_offers = soup.find_all(string=re.compile(r'buy\s+\d+\s+get\s+\d+\s+free|buy\s+\d+\s+tires?\s+get\s+\d+\s+free|free\s+tire|free\s+flat', re.IGNORECASE))
        for free_text in free_offers:
            container = free_text.find_parent(['section', 'div', 'article'])
            if not container:
                container = free_text.find_parent()

            if container:
                text = container.get_text(separator=" ", strip=True)
                has_free_keyword = bool(re.search(r'buy.*get.*free|free\s+tire|free\s+flat|free\s+repair', text, re.IGNORECASE))
                is_too_long = len(text) > 3000
                is_too_short = len(text) < 50
                is_generic = any(keyword in text.lower() for keyword in [
                    "we're in the business", "worry-free", "book online", "rating from reviews"
                ])

                if has_free_keyword and not is_too_long and not is_too_short and not is_generic:
                    text_hash = hash(text[:400])
                    if text_hash not in seen_texts:
                        seen_texts.add(text_hash)
                        promo_blocks.append({
                            "text": text,
                            "html": str(container)[:2000],
                            "selector": "archive-free-offer"
                        })
                        logger.info(f"Found archive promo (free offer): {text[:80]}... ({len(text)} chars)")

        # Method 2: Look for specific promo headings and their containers
        promo_headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'], string=re.compile(
            r'oil\s+change|tire|brake|buy.*get|free|special|offer|full\s+stop|rovelo|full\s+synthetic|euro',
            re.IGNORECASE
        ))

        for heading in promo_headings:
            heading_text = heading.get_text(strip=True)

            # Get parent container
            container = heading.find_parent(['section', 'div', 'article'])
            if not container:
                container = heading.find_parent()

            if container:
                text = container.get_text(separator=" ", strip=True)

                # Must have price indicator or promo keyword
                has_price = bool(re.search(r'\$\d+', text))
                has_promo = bool(re.search(
                    r'oil\s+change|tire|brake|free|special|offer|buy.*get|rovelo|synthetic|euro|limited|lifetime',
                    text, re.IGNORECASE
                ))

                # Filter out generic text
                is_generic = any(keyword in text.lower() for keyword in [
                    "we're in the business", "worry-free", "book online", "rating from reviews",
                    "why us", "experienced technicians", "unbeatable service"
                ])

                if (has_price or has_promo) and 50 < len(text) < 3000 and not is_generic:
                    text_hash = hash(text[:400])
                    if text_hash not in seen_texts:
                        seen_texts.add(text_hash)
                        promo_blocks.append({
                            "text": text,
                            "html": str(container)[:2000],
                            "selector": f"archive-{heading.name}"
                        })
                        logger.info(f"Found archive promo: {heading_text[:50]}... ({len(text)} chars)")

    # Remove very similar duplicates using fuzzy matching
    if len(promo_blocks) > 1:
        unique_blocks = []
        for block in promo_blocks:
            is_duplicate = False
            for existing in unique_blocks:
                similarity = fuzz.ratio(block["text"][:300], existing["text"][:300])
                if similarity > 90:  # Very high threshold for duplicates
                    is_duplicate = True
                    logger.debug(f"Skipping duplicate block ({similarity}% similar)")
                    break
            if not is_duplicate:
                unique_blocks.append(block)

        logger.info(f"Extracted {len(unique_blocks)} unique promo blocks (removed {len(promo_blocks) - len(unique_blocks)} duplicates)")
        return unique_blocks

    logger.info(f"Extracted {len(promo_blocks)} promo blocks")
    return promo_blocks


def extract_discount_value(text: str) -> Optional[str]:
    """Extract discount value from text."""
    text_lower = text.lower()

    # Try dollar amount first (rebate amounts)
    dollar_match = re.search(r'\$(\d+(?:\.\d+)?)\s+Back|\$(\d+(?:\.\d+)?)\s+back|Get\s+(?:up\s+to\s+)?\$(\d+(?:\.\d+)?)\s+Back', text, re.IGNORECASE)
    if dollar_match:
        amount = dollar_match.group(1) or dollar_match.group(2) or dollar_match.group(3)
        return f"${amount} back"

    # Try regular dollar amount
    dollar_match = re.search(r'\$(\d+(?:\.\d+)?)', text)
    if dollar_match:
        return f"${dollar_match.group(1)}"

    # Try percentage
    percent_match = re.search(r'(\d+)\s*%', text)
    if percent_match:
        return f"{percent_match.group(1)}%"

    # Try "free"
    if "free" in text_lower:
        return "free"

    return None


def extract_coupon_code(text: str) -> Optional[str]:
    """Extract coupon code from text."""
    code_patterns = [
        r'(?:code|coupon|promo)[:\s]+([A-Z0-9]{3,20})',
        r'use[:\s]+([A-Z0-9]{3,20})',
        r'code[:\s]*([A-Z0-9]{4,15})',
    ]

    for pattern in code_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    return None


def extract_expiry_date(text: str) -> Optional[str]:
    """Extract expiry date from text."""
    date_patterns = [
        r'Postmark.*?Date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Submission.*?Date[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'Offer\s+Valid[:\s]+([^–-]+?)(?:\s+–\s+|\s+-\s+)(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?:expires?|valid until|until|ends?)[:\s]+([A-Za-z]+\s+\d{1,2}[,\s]+\d{4})',
        r'(?:expires?|valid until|until|ends?)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?:expires?|valid)[:\s]*(\d{1,2}\s+[A-Za-z]+\s+\d{4})',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1) if match.lastindex == 1 else f"{match.group(1)} - {match.group(2)}"

    return None


def map_service_category(text: str) -> str:
    """Map text to service category."""
    text_lower = text.lower()

    service_keywords = {
        "tires": ["tire", "tires", "wheel", "wheels", "alignment", "bridgestone", "firestone", "michelin", "goodyear"],
        "oil change": ["oil", "lube", "oil change"],
        "brakes": ["brake", "brakes", "brake pad", "brake service"],
        "battery": ["battery", "batteries"],
        "exhaust": ["exhaust", "muffler"],
        "transmission": ["transmission", "trans"],
        "cooling": ["coolant", "radiator", "cooling system"],
        "filters": ["filter", "filters", "air filter"],
    }

    for category, keywords in service_keywords.items():
        if any(keyword in text_lower for keyword in keywords):
            return category

    return "other"


def extract_brand_name(text: str) -> Optional[str]:
    """Extract tire brand name from text."""
    text_lower = text.lower()
    brands = [
        "michelin", "bridgestone", "firestone", "goodyear", "continental",
        "pirelli", "bfgoodrich", "toyo", "nitto", "hankook", "falken",
        "kumho", "yokohama", "dunlop", "general", "cooper", "uniroyal"
    ]

    for brand in brands:
        if brand in text_lower:
            return brand.title()
    return None


def are_promos_duplicate(promo1: Dict, promo2: Dict) -> bool:
    """Check if two promotions are duplicates - considers discount amount and brand."""
    # Extract discount from offer_details if discount_value not available
    discount1 = promo1.get("discount_value") or ""
    discount2 = promo2.get("discount_value") or ""

    # Try to extract discount from offer_details if not in discount_value
    if not discount1:
        offer1 = promo1.get("offer_details", "")
        discount_match = re.search(r'\$(\d+(?:\.\d+)?)', offer1)
        if discount_match:
            discount1 = f"${discount_match.group(1)}"

    if not discount2:
        offer2 = promo2.get("offer_details", "")
        discount_match = re.search(r'\$(\d+(?:\.\d+)?)', offer2)
        if discount_match:
            discount2 = f"${discount_match.group(1)}"

    # Quick check: same page and same/similar title => treat as duplicate, keep first
    page1 = promo1.get("page_url", "")
    page2 = promo2.get("page_url", "")
    title1_quick = (promo1.get("ad_title") or promo1.get("service_name", "")).lower()
    title2_quick = (promo2.get("ad_title") or promo2.get("service_name", "")).lower()
    if page1 == page2 and title1_quick and title2_quick:
        if fuzz.ratio(title1_quick[:120], title2_quick[:120]) >= 90:
            return True

    # Extract brand names from promo text/description
    promo1_text = (promo1.get("promo_description", "") + " " +
                   promo1.get("ad_text", "") + " " +
                   promo1.get("offer_details", "")).lower()
    promo2_text = (promo2.get("promo_description", "") + " " +
                   promo2.get("ad_text", "") + " " +
                   promo2.get("offer_details", "")).lower()

    brand1 = extract_brand_name(promo1_text)
    brand2 = extract_brand_name(promo2_text)

    # If different discount amounts, they are NOT duplicates
    if discount1 and discount2 and discount1 != discount2:
        return False

    # If different brands, they are NOT duplicates
    if brand1 and brand2 and brand1 != brand2:
        return False

    # If one has a brand and the other doesn't, they might be different
    if (brand1 and not brand2) or (brand2 and not brand1):
        # Check if the text is very similar (might be same rebate with/without brand mention)
        title1 = (promo1.get("ad_title") or promo1.get("service_name", "")).lower()
        title2 = (promo2.get("ad_title") or promo2.get("service_name", "")).lower()
        similarity = fuzz.ratio(title1[:200], title2[:200])
        if similarity < 95:  # Not very similar, likely different
            return False

    # Same discount AND same brand (or both no brand) - check if text is very similar
    title1 = (promo1.get("ad_title") or promo1.get("service_name", "")).lower()
    title2 = (promo2.get("ad_title") or promo2.get("service_name", "")).lower()
    desc1 = promo1.get("promo_description", "").lower()
    desc2 = promo2.get("promo_description", "").lower()

    # Very high title similarity (95%+) AND same discount = likely duplicate
    title_similarity = fuzz.ratio(title1[:200], title2[:200])
    desc_similarity = fuzz.ratio(desc1[:300], desc2[:300])

    if title_similarity >= 95 and desc_similarity >= 90:
        return True

    # If same discount and very high description similarity
    if discount1 and discount2 and discount1 == discount2:
        if desc_similarity >= 90:
            return True

    return False


def process_midas_promotions(competitor: Dict) -> List[Dict]:
    """Process Midas promotions using text-based HTML extraction."""
    logger.info(f"Processing promotions for {competitor.get('name')}")

    # Get Google Reviews
    google_reviews = get_google_reviews_for_competitor(competitor)

    promo_links = competitor.get("promo_links", [])
    if not promo_links:
        logger.warning(f"No promo_links found for {competitor.get('name')}")
        return []

    # Load existing promos for comparison
    output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'midas').lower().replace(' ', '_')}.json"
    existing_promos = load_existing_promos(output_file)

    all_promos = []

    for promo_url in promo_links:
        logger.info(f"Fetching {promo_url}")

        # Step 1: Fetch HTML with fallback
        html = fetch_with_fallback(promo_url)

        if not html:
            logger.error(f"Failed to fetch HTML from {promo_url}")
            continue

        # Step 2: Extract promo blocks (pass URL for context)
        promo_blocks = extract_promo_blocks(html, promo_url)

        if not promo_blocks:
            logger.warning(f"No promo blocks found on {promo_url}")
            continue

        # Step 3: Process each promo block with LLM
        for block in promo_blocks:
            text = block["text"]

            # Skip if too short
            if len(text) < 50:
                continue

            logger.info(f"Processing promo block: {len(text)} chars")

            try:
                # Send to LLM for cleaning and structuring
                context = f"Midas promotion from {promo_url}. Block selector: {block.get('selector', 'unknown')}"
                cleaned_data = clean_promo_text_with_llm(text, context)

                # Handle case where LLM returns a list instead of dict
                if isinstance(cleaned_data, list):
                    if len(cleaned_data) > 0 and isinstance(cleaned_data[0], dict):
                        cleaned_data = cleaned_data[0]
                    else:
                        cleaned_data = None

                # Extract basic details from text
                discount_value = extract_discount_value(text)
                coupon_code = extract_coupon_code(text)
                expiry_date = extract_expiry_date(text)
                service_category = map_service_category(text)

                # Extract brand name from text for better service_name
                brand_name = extract_brand_name(text)

                # Build promotion using LLM cleaned data if available
                if cleaned_data and isinstance(cleaned_data, dict):
                    # Build service name with brand if available
                    base_service_name = cleaned_data.get("service_name") or service_category
                    if brand_name and brand_name.lower() not in base_service_name.lower():
                        final_service_name = f"{brand_name} {base_service_name}"
                    else:
                        final_service_name = base_service_name

                    promotion_title = final_service_name or (cleaned_data.get("promo_description") or "").split("\n")[0].strip()[:100] if cleaned_data.get("promo_description") else None
                    if not promotion_title:
                        lines = [l.strip() for l in text.split("\n") if l.strip() and len(l.strip()) > 5]
                        promotion_title = lines[0][:100] if lines else "Midas Promotion"

                    promo_description = cleaned_data.get("promo_description") or text[:500]
                    offer_details = cleaned_data.get("promo_description") or text[:1000]
                    discount_value = cleaned_data.get("discount_value") or discount_value
                    coupon_code = cleaned_data.get("coupon_code") or coupon_code
                    expiry_date = cleaned_data.get("expiry_date") or expiry_date

                    if cleaned_data.get("service_name"):
                        service_category = map_service_category(cleaned_data.get("service_name"))
                else:
                    # Fallback to direct text extraction
                    # Build service name with brand if available
                    base_service_name = service_category
                    if brand_name and brand_name.lower() not in base_service_name.lower():
                        final_service_name = f"{brand_name} {base_service_name}"
                    else:
                        final_service_name = base_service_name

                    lines = [l.strip() for l in text.split("\n") if l.strip() and len(l.strip()) > 5]
                    promotion_title = final_service_name or (lines[0][:100] if lines else "Midas Promotion")
                    promo_description = text[:500]
                    offer_details = text[:1000]

                # Build offer_details with discount, code, expiry
                offer_parts = []
                if discount_value:
                    offer_parts.append(f"Discount: {discount_value}")
                if coupon_code:
                    offer_parts.append(f"Code: {coupon_code}")
                if expiry_date:
                    offer_parts.append(f"Expires: {expiry_date}")

                if offer_parts:
                    final_offer_details = ". ".join(offer_parts) + ". " + (offer_details or text[:500])
                else:
                    final_offer_details = offer_details or text[:1000]

                # Ensure ad_text is never empty
                ad_text_final = text[:500] if text else f"{final_service_name} promotion"

                # Find existing promo for comparison
                promo_key = f"{promo_url}::{final_service_name}"
                existing_promo = existing_promos.get(promo_key)

                # Build standardized promo object
                promo = build_standard_promo(
                    competitor=competitor,
                    promo_url=promo_url,
                    service_name=final_service_name,
                    promo_description=promo_description,
                    category=service_category,
                    offer_details=final_offer_details,
                    ad_title=promotion_title,
                    ad_text=ad_text_final,
                    google_reviews=google_reviews,
                    existing_promo=existing_promo
                )

                all_promos.append(promo)
                logger.info(f"[OK] Added promo: {promotion_title[:50]} - {discount_value or 'N/A'}")

            except Exception as e:
                logger.error(f"Error processing promo block: {e}", exc_info=True)
                continue

    # Final deduplication pass
    logger.info(f"Found {len(all_promos)} promotions before deduplication")
    deduplicated = []
    for promo in all_promos:
        is_duplicate = False
        for existing in deduplicated:
            if are_promos_duplicate(promo, existing):
                promo_title = promo.get('ad_title') or promo.get('service_name', 'N/A')
                existing_title = existing.get('ad_title') or existing.get('service_name', 'N/A')
                logger.info(f"Removed duplicate: {promo_title[:50]} (similar to {existing_title[:50]})")
                is_duplicate = True
                break
        if not is_duplicate:
            deduplicated.append(promo)

    logger.info(f"Total unique promotions found: {len(deduplicated)}")
    return deduplicated


def scrape_midas(competitor: Dict) -> Dict:
    """Main entry point for Midas scraper."""
    try:
        promos = process_midas_promotions(competitor)

        # Save results
        output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'midas').lower().replace(' ', '_')}.json"
        result = {
            "competitor": competitor.get("name"),
            "scraped_at": datetime.now().isoformat(),
            "promotions": promos,
            "count": len(promos)
        }

        output_file.write_text(json.dumps(result, indent=2, default=str))
        logger.info(f"Saved {len(promos)} promotions to {output_file}")

        return result

    except Exception as e:
        logger.error(f"Error scraping Midas: {e}", exc_info=True)
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

    # Find Midas
    midas = next((c for c in competitors if "midas" in c.get("name", "").lower()), None)

    if not midas:
        logger.error("Midas not found in competitor list")
        sys.exit(1)

    result = scrape_midas(midas)
    print(f"\n✅ Scraping complete!")
    print(f" Found {result.get('count', 0)} promotions")
    print(f" Saved to: data/promotions/")
    print(f"\n📊 Summary:")
    for promo in result.get("promotions", []):
        print(f" • {promo.get('promotion_title', 'N/A')}: {promo.get('discount_value', 'N/A')}")
