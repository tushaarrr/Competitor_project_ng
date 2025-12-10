"""Fountain Tire scraper - Text-based extraction with OCR for main promotions page."""
import json
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import re
from fuzzywuzzy import fuzz

from app.extractors.firecrawl.firecrawl_client import fetch_with_firecrawl
from app.extractors.ocr.llm_cleaner import clean_promo_text_with_llm
from app.extractors.images.image_downloader import download_image, normalize_url
from app.extractors.ocr.ocr_processor import ocr_image
from app.extractors.html_parser import find_images_by_css_selector
from app.config.constants import DATA_DIR
from app.utils.logging_utils import setup_logger
from app.utils.promo_builder import build_standard_promo, load_existing_promos, apply_ai_overview_fallback, get_google_reviews_for_competitor

logger = setup_logger(__name__, "fountain_scraper.log")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMOTIONS_DIR = DATA_DIR / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)


def fetch_with_fallback(url: str) -> Dict:
    """Fetch HTML using Firecrawl, fallback to ZenRows/ScraperAPI."""
    # Try Firecrawl first
    firecrawl_result = fetch_with_firecrawl(url, timeout=90)

    if firecrawl_result.get("html") and not firecrawl_result.get("error"):
        logger.info("Successfully fetched with Firecrawl")
        return {
            "html": firecrawl_result.get("html", ""),
            "images": firecrawl_result.get("images", [])
        }

    logger.warning("Firecrawl failed, trying fallback methods...")

    # Fallback to ZenRows
    try:
        from app.config.constants import ZENROWS_API_KEY
        if ZENROWS_API_KEY:
            import requests
            zenrows_url = f"https://api.zenrows.com/v1/?apikey={ZENROWS_API_KEY}&url={url}"
            response = requests.get(zenrows_url, timeout=30)
            response.raise_for_status()
            logger.info("Successfully fetched with ZenRows")
            # Extract images from HTML
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin
            soup = BeautifulSoup(response.text, "html.parser")
            images = []
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
                if src:
                    images.append(urljoin(url, src))
            return {"html": response.text, "images": images}
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
            # Extract images from HTML
            from bs4 import BeautifulSoup
            from urllib.parse import urljoin
            soup = BeautifulSoup(response.text, "html.parser")
            images = []
            for img in soup.find_all("img"):
                src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
                if src:
                    images.append(urljoin(url, src))
            return {"html": response.text, "images": images}
    except Exception as e:
        logger.warning(f"ScraperAPI fallback failed: {e}")

    logger.error("All fetch methods failed")
    return {"html": "", "images": []}


def extract_promo_sections_from_html(html: str) -> List[Dict]:
    """Extract promotional sections from HTML."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    promo_sections = []

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Common selectors for promo content
    selectors = [
        "div.promo",
        "div.promotion",
        "div[class*='promo']",
        "div[class*='offer']",
        "div[class*='special']",
        "div[class*='rebate']",
        "div[class*='coupon']",
        "article",
        "section",
    ]

    seen_texts = set()

    for selector in selectors:
        elements = soup.select(selector)
        for elem in elements:
            text = elem.get_text(separator=" ", strip=True)
            if text and len(text) > 50:  # Minimum length for valid promo
                # Skip JavaScript template strings
                if "{{" in text or "}}" in text or "${{" in text:
                    continue

                # Skip if looks like template/code
                if re.search(r'\{\{.*?\}\}', text):
                    continue

                # Normalize and deduplicate
                text_normalized = " ".join(text.lower().split()[:50])  # Use first 50 words for dedup
                if text_normalized not in seen_texts:
                    seen_texts.add(text_normalized)
                    # Extract images from this section
                    images = []
                    for img in elem.find_all("img"):
                        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original")
                        if src:
                            images.append(normalize_url("", src))

                    promo_sections.append({
                        "html": str(elem),
                        "text": text,
                        "images": images,
                        "selector": selector
                    })

    # If no specific promo sections found, extract from main content
    # But also look for specific promo blocks even if generic selectors didn't find them
    if not promo_sections or len(promo_sections) < 2:
        main_content = soup.find("main") or soup.find("article") or soup.find("div", class_=lambda x: x and ("content" in str(x).lower() or "main" in str(x).lower()))
        if main_content:
            # Look for headings or paragraphs with promo keywords
            promo_elements = []
            for elem in main_content.find_all(["h1", "h2", "h3", "p", "div"]):
                text = elem.get_text(strip=True)
                if text and len(text) > 20:
                    text_lower = text.lower()
                    # Check for promo keywords
                    if re.search(r'(save|discount|off|rebate|financing|promo|offer|deal|special)', text_lower):
                        promo_elements.append(elem)

            if promo_elements:
                # Combine promo elements into sections
                for elem in promo_elements[:3]:  # Limit to top 3
                    text = elem.get_text(separator=" ", strip=True)
                    if text and len(text) > 20:
                        images = []
                        for img in elem.find_all("img"):
                            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
                            if src:
                                images.append(normalize_url("", src))
                        promo_sections.append({
                            "html": str(elem),
                            "text": text,
                            "images": images,
                            "selector": "promo_element"
                        })

            # Fallback: use full main content if still nothing
            if not promo_sections:
                text = main_content.get_text(separator=" ", strip=True)
                if text and len(text) > 100:
                    # Extract images
                    images = []
                    for img in main_content.find_all("img"):
                        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
                        if src:
                            images.append(normalize_url("", src))
                    promo_sections.append({
                        "html": str(main_content),
                        "text": text,
                        "images": images,
                        "selector": "main_content"
                    })

    logger.info(f"Extracted {len(promo_sections)} promo sections from HTML")
    return promo_sections


def extract_images_for_ocr(html: str, base_url: str) -> List[str]:
    """Extract image URLs from HTML for OCR processing."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    images = []

    # Look for promo-related images
    img_selectors = [
        "img[class*='promo']",
        "img[class*='offer']",
        "img[class*='rebate']",
        "img[class*='coupon']",
        "img[class*='special']",
    ]

    seen_urls = set()

    for selector in img_selectors:
        for img in soup.select(selector):
            src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or img.get("data-original")
            if src:
                img_url = normalize_url(base_url, src)
                if img_url not in seen_urls and not img_url.startswith("data:"):
                    seen_urls.add(img_url)
                    images.append(img_url)

    # Also get all images from main content area if no promo-specific images found
    if not images:
        main_content = soup.find("main") or soup.find("article") or soup.find("div", class_=lambda x: x and "content" in str(x).lower())
        if main_content:
            for img in main_content.find_all("img"):
                src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
                if src:
                    img_url = normalize_url(base_url, src)
                    if img_url not in seen_urls and not img_url.startswith("data:"):
                        seen_urls.add(img_url)
                        images.append(img_url)

    logger.info(f"Found {len(images)} images for OCR processing")
    return images


def process_page_text_only(url: str) -> List[Dict]:
    """Process a page using text extraction only (no OCR)."""
    logger.info(f"Processing {url} (text-only mode)")

    result = fetch_with_fallback(url)
    html = result.get("html", "")

    if not html:
        logger.error(f"Failed to fetch HTML from {url}")
        return []

    promo_sections = extract_promo_sections_from_html(html)

    if not promo_sections:
        # Fallback: extract from entire page
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text(separator=" ", strip=True)
        if body_text and len(body_text) > 100:
            promo_sections = [{
                "html": html,
                "text": body_text,
                "images": [],
                "selector": "full_page"
            }]

    return promo_sections


def process_page_with_ocr(url: str) -> List[Dict]:
    """Process a page using text extraction + OCR on images."""
    logger.info(f"Processing {url} (text + OCR mode)")

    result = fetch_with_fallback(url)
    html = result.get("html", "")
    image_urls = result.get("images", [])

    if not html:
        logger.error(f"Failed to fetch HTML from {url}")
        return []

    promo_sections = extract_promo_sections_from_html(html)

    # Also extract images for OCR
    ocr_images = extract_images_for_ocr(html, url)
    # Add images from Firecrawl result if any
    for img_url in image_urls:
        if img_url not in ocr_images:
            ocr_images.append(img_url)

    # Run OCR on images
    ocr_text_parts = []
    processed_image_urls = []

    for img_url in ocr_images[:5]:  # Limit to 5 images to avoid too many OCR calls
        try:
            logger.info(f"Running OCR on image: {img_url}")
            img_path = download_image(img_url)
            if img_path:
                ocr_text = ocr_image(img_path)
                if ocr_text and len(ocr_text.strip()) > 10:
                    ocr_text_parts.append(ocr_text)
                    processed_image_urls.append(img_url)
                # Clean up
                try:
                    img_path.unlink()
                except:
                    pass
        except Exception as e:
            logger.warning(f"OCR error for {img_url}: {e}")

    # If OCR found text, add it as a separate section or merge with existing
    if ocr_text_parts:
        ocr_text = "\n".join(ocr_text_parts)
        # Check if we should merge with existing sections or create new
        if promo_sections:
            # Merge OCR text with first promo section
            promo_sections[0]["text"] += "\n\n" + ocr_text
            promo_sections[0]["ocr_images"] = processed_image_urls
        else:
            # Create new section from OCR
            promo_sections.append({
                "html": "",
                "text": ocr_text,
                "images": processed_image_urls,
                "ocr_images": processed_image_urls,
                "selector": "ocr"
            })

    # Fallback: if no sections found, use full page text
    if not promo_sections:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text(separator=" ", strip=True)
        if body_text and len(body_text) > 100:
            promo_sections = [{
                "html": html,
                "text": body_text + ("\n\n" + ocr_text if ocr_text_parts else ""),
                "images": processed_image_urls,
                "selector": "full_page"
            }]

    return promo_sections


def normalize_title(title: str) -> str:
    """Normalize title for comparison."""
    if not title:
        return ""
    normalized = re.sub(r'[^\w\s]', ' ', title.lower())
    normalized = " ".join(normalized.split())
    return normalized


def normalize_text_for_dedup(text: str) -> str:
    """
    Normalize text for Fountain Tire deduplication.

    Rules:
    - lowercase everything
    - remove extra spaces and line breaks
    - remove duplicate phrases like "learn more", "see details", etc.
    - collapse multiple spaces into one
    """
    if not text:
        return ""

    # Lowercase
    normalized = text.lower()

    # Remove common duplicate phrases
    duplicate_phrases = [
        "learn more", "see details", "view details", "click here", "read more",
        "find out more", "get started", "shop now", "buy now", "apply now",
        "view offer", "see offer", "view promotion", "see promotion"
    ]
    for phrase in duplicate_phrases:
        # Remove phrase and any surrounding punctuation/spaces
        normalized = re.sub(rf'\b{re.escape(phrase)}\b[.,;:!?\s]*', ' ', normalized, flags=re.IGNORECASE)

    # Remove line breaks and normalize whitespace
    normalized = normalized.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')

    # Collapse multiple spaces into one
    normalized = re.sub(r'\s+', ' ', normalized)

    # Remove leading/trailing spaces
    normalized = normalized.strip()

    return normalized


def extract_brand_name_from_text(text: str) -> Optional[str]:
    """Extract tire brand name from text for Fountain Tire deduplication."""
    if not text:
        return None

    text_lower = text.lower()
    brands = [
        "michelin", "bridgestone", "goodyear", "continental", "pirelli",
        "bfgoodrich", "toyo", "nitto", "hankook", "falken", "kumho",
        "yokohama", "dunlop", "firestone", "general", "cooper", "uniroyal",
        "mastercraft", "hercules", "nexen", "laufenn"
    ]

    for brand in brands:
        if brand in text_lower:
            return brand.title()
    return None


def are_fountain_promos_duplicate(promo1: Dict, promo2: Dict) -> bool:
    """
    Check if two Fountain Tire promotions are duplicates.

    Deduplication rules:
    1. Normalize text (lowercase, remove extra spaces, remove duplicate phrases)
    2. Use composite key: service_name_clean + promo_description_clean + offer_details_clean
    3. If match on key, they are duplicates
    4. Also treat as duplicate if:
       - Same discount value AND same brand
       - Same ad_title + ad_text combination
       - Same promo from multiple Fountain Tire URLs
    """
    # Normalize all text fields
    service1_clean = normalize_text_for_dedup(promo1.get("service_name", ""))
    service2_clean = normalize_text_for_dedup(promo2.get("service_name", ""))

    desc1_clean = normalize_text_for_dedup(promo1.get("promo_description", ""))
    desc2_clean = normalize_text_for_dedup(promo2.get("promo_description", ""))

    offer1_clean = normalize_text_for_dedup(promo1.get("offer_details", ""))
    offer2_clean = normalize_text_for_dedup(promo2.get("offer_details", ""))

    # Rule 2: Composite key match
    key1 = service1_clean + desc1_clean + offer1_clean
    key2 = service2_clean + desc2_clean + offer2_clean

    if key1 and key2 and key1 == key2:
        return True

    # Rule 4a: Same discount value AND same brand
    discount1 = promo1.get("discount_value", "")
    discount2 = promo2.get("discount_value", "")

    if discount1 and discount2 and discount1 == discount2:
        # Extract brand names
        promo1_text = (promo1.get("service_name", "") + " " +
                      promo1.get("promo_description", "") + " " +
                      promo1.get("offer_details", "")).lower()
        promo2_text = (promo2.get("service_name", "") + " " +
                      promo2.get("promo_description", "") + " " +
                      promo2.get("offer_details", "")).lower()

        brand1 = extract_brand_name_from_text(promo1_text)
        brand2 = extract_brand_name_from_text(promo2_text)

        if brand1 and brand2 and brand1 == brand2:
            return True

    # Rule 4b: Same ad_title + ad_text combination
    ad_title1_clean = normalize_text_for_dedup(promo1.get("ad_title", ""))
    ad_title2_clean = normalize_text_for_dedup(promo2.get("ad_title", ""))

    ad_text1_clean = normalize_text_for_dedup(promo1.get("ad_text", ""))
    ad_text2_clean = normalize_text_for_dedup(promo2.get("ad_text", ""))

    if ad_title1_clean and ad_title2_clean and ad_text1_clean and ad_text2_clean:
        if ad_title1_clean == ad_title2_clean and ad_text1_clean == ad_text2_clean:
            return True

    # Rule 4c: Same promo from multiple Fountain Tire URLs (check if content is very similar)
    # This is already covered by the composite key, but we can add extra check for URL variations
    page_url1 = promo1.get("page_url", "")
    page_url2 = promo2.get("page_url", "")

    # If from different Fountain Tire URLs but content is very similar
    if page_url1 != page_url2 and "fountaintire.com" in page_url1 and "fountaintire.com" in page_url2:
        # Check if service, description, and offer are very similar (95%+)
        service_sim = fuzz.ratio(service1_clean, service2_clean) if service1_clean and service2_clean else 0
        desc_sim = fuzz.ratio(desc1_clean, desc2_clean) if desc1_clean and desc2_clean else 0
        offer_sim = fuzz.ratio(offer1_clean, offer2_clean) if offer1_clean and offer2_clean else 0

        if service_sim >= 95 and desc_sim >= 95 and offer_sim >= 95:
            return True

    return False


def are_promos_duplicate(promo1: Dict, promo2: Dict) -> bool:
    """Check if two promotions are duplicates - uses Fountain Tire-specific logic for Fountain Tire only."""
    # Only apply Fountain Tire rules to Fountain Tire promotions
    business1 = promo1.get("business_name", "").lower()
    business2 = promo2.get("business_name", "").lower()

    is_fountain1 = "fountain" in business1 and "tire" in business1
    is_fountain2 = "fountain" in business2 and "tire" in business2

    # If both are Fountain Tire, use specialized deduplication
    if is_fountain1 and is_fountain2:
        return are_fountain_promos_duplicate(promo1, promo2)

    # For non-Fountain Tire or mixed, use original logic (title + image URL)
    title1 = normalize_title(promo1.get("promotion_title", ""))
    title2 = normalize_title(promo2.get("promotion_title", ""))

    # Skip if titles are too short or generic
    if len(title1.split()) < 3 or len(title2.split()) < 3:
        return False

    # Get image URLs
    img1 = promo1.get("image_url") or promo1.get("primary_image_url")
    img2 = promo2.get("image_url") or promo2.get("primary_image_url")

    # Same image URL
    if img1 and img2 and img1 == img2:
        return True

    # Same title (high similarity) - 90% threshold
    if title1 and title2:
        title_similarity = fuzz.token_set_ratio(title1, title2)
        if title_similarity >= 90:
            return True

    return False


def process_fountain_promotions(competitor: Dict) -> List[Dict]:
    """Process Fountain Tire promotions."""
    logger.info(f"Processing promotions for {competitor.get('name')}")

    promo_links = competitor.get("promo_links", [])
    if not promo_links:
        logger.warning(f"No promo_links found for {competitor.get('name')}")
        return []

    # Get Google Reviews once for this competitor
    google_reviews = get_google_reviews_for_competitor(competitor)

    all_promos = []

    for promo_url in promo_links:
        logger.info(f"Processing URL: {promo_url}")

        # Determine processing mode based on URL
        if "/promotions/tire-rebates/" in promo_url or "/promotions/financing/" in promo_url:
            # Text-only mode
            promo_sections = process_page_text_only(promo_url)
        else:
            # Text + OCR mode for main promotions page
            promo_sections = process_page_with_ocr(promo_url)

        # Process each promo section
        for section in promo_sections:
            section_text = section["text"]
            section_html = section["html"]
            section_images = section.get("images", [])
            ocr_images = section.get("ocr_images", [])

            # Extract basic details
            discount_value = extract_discount_value(section_text)
            coupon_code = extract_coupon_code(section_text)
            expiry_date = extract_expiry_date(section_text)

            # Clean with LLM
            context = f"Fountain Tire promotion from {promo_url}. HTML: {section_html[:1000]}"
            cleaned_data = clean_promo_text_with_llm(section_text, context)

            # Skip if section text contains template strings
            if "{{" in section_text or "}}" in section_text or "${{" in section_text:
                logger.info(f"Skipping template string: {section_text[:100]}")
                continue

            # Skip header/intro text that's not an actual promotion
            section_lower = section_text.lower()

            # Skip if it starts with intro phrase and doesn't have actual promo content
            if section_lower.startswith("put some money back") or section_lower.startswith("claiming your rebate from the following"):
                if not re.search(r'\$(\d+)|(\d+)\s*%|save|off|discount|financing|offer', section_lower):
                    logger.info(f"Skipping intro text: {section_text[:100]}")
                    continue

            # Skip very short text that's likely not a real promotion (but allow rebate links)
            if len(section_text.split()) < 5:
                logger.info(f"Skipping very short text: {section_text[:100]}")
                continue

            # Build promotion title
            if cleaned_data and cleaned_data.get("service_name"):
                promotion_title = cleaned_data.get("service_name")
            elif cleaned_data and cleaned_data.get("promo_description"):
                first_line = cleaned_data.get("promo_description", "").split("\n")[0].strip()[:100]
                promotion_title = first_line if first_line else section_text.split("\n")[0][:100]
            else:
                # Extract first meaningful line
                lines = [l.strip() for l in section_text.split("\n") if l.strip() and len(l.strip()) > 10]
                promotion_title = lines[0][:100] if lines else "Fountain Tire Promotion"

            # Use LLM cleaned data if available
            if cleaned_data:
                service_name = cleaned_data.get("service_name", "tires")
                promo_description = cleaned_data.get("promo_description", section_text[:500])
                category = cleaned_data.get("category", "tires")
                offer_details = cleaned_data.get("offer_details")
                if not offer_details:
                    offer_parts = []
                    discount_val = cleaned_data.get("discount_value") or discount_value
                    coupon_code_val = cleaned_data.get("coupon_code") or coupon_code
                    expiry_date_val = cleaned_data.get("expiry_date") or expiry_date
                    if discount_val:
                        offer_parts.append(f"Discount: {discount_val}")
                    if coupon_code_val:
                        offer_parts.append(f"Code: {coupon_code_val}")
                    if expiry_date_val:
                        offer_parts.append(f"Expires: {expiry_date_val}")
                    if offer_parts:
                        offer_details = ". ".join(offer_parts) + ". " + section_text[:500]
                    else:
                        offer_details = section_text[:1000]
            else:
                service_name = "tires"
                promo_description = section_text[:500]
                category = "tires"
                offer_parts = []
                if discount_value:
                    offer_parts.append(f"Discount: {discount_value}")
                if coupon_code:
                    offer_parts.append(f"Code: {coupon_code}")
                if expiry_date:
                    offer_parts.append(f"Expires: {expiry_date}")
                if offer_parts:
                    offer_details = ". ".join(offer_parts) + ". " + section_text[:500]
                else:
                    offer_details = section_text[:1000]

            # Load existing promos for comparison
            output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'fountain').lower().replace(' ', '_')}.json"
            existing_promos = load_existing_promos(output_file)
            promo_key = f"{promo_url}::{service_name}"
            existing_promo = existing_promos.get(promo_key)

            promo = build_standard_promo(
                competitor=competitor,
                promo_url=promo_url,
                service_name=service_name,
                promo_description=promo_description,
                category=category,
                offer_details=offer_details,
                ad_title=promotion_title,
                ad_text=section_text[:500],
                google_reviews=google_reviews,
                existing_promo=existing_promo
            )

            all_promos.append(promo)
            logger.info(f"[OK] Added promo: {promo.get('service_name', 'N/A')} - {promo.get('new_or_updated', 'NEW')}")

    # Group rebate manufacturer links together if from same page (tire-rebates only)
    rebate_promos = {}
    other_promos = []

    for promo in all_promos:
        page_url = promo.get("page_url", "")
        title = promo.get("promotion_title", "").lower()

        # Check if it's a rebate manufacturer link from tire-rebates page
        if "/promotions/tire-rebates/" in page_url and any(brand in title for brand in ["goodyear", "cooper", "toyo", "kumho", "visit the", "rebate site", "rebate center"]):
            # Group by page
            if page_url not in rebate_promos:
                rebate_promos[page_url] = []
            rebate_promos[page_url].append(promo)
        else:
            # Keep other promos separate (from main promotions page and financing page)
            other_promos.append(promo)

    # Merge rebate promos from tire-rebates page into one
    merged_rebate_promos = []
    for page_url, rebates in rebate_promos.items():
        if rebates:
            # Combine all rebate texts
            combined_text = "\n".join([p.get("offer_details", "") for p in rebates])
            # Use first promo as base
            base_promo = rebates[0].copy()
            base_promo["promotion_title"] = "Tire Manufacturer Rebates"
            base_promo["offer_details"] = combined_text[:1000]
            merged_rebate_promos.append(base_promo)
            logger.info(f"Merged {len(rebates)} rebate manufacturer links into one promotion")

    # Combine with other promos (keep main promotions and financing separate)
    all_promos_merged = other_promos + merged_rebate_promos

    # Deduplicate using Fountain Tire-specific rules
    logger.info(f"Found {len(all_promos)} promotions before grouping, {len(all_promos_merged)} after grouping rebates")

    deduplicated = []
    seen_keys = set()  # Track composite keys for Fountain Tire deduplication
    seen_promos = []  # Track seen promotions for comparison

    for promo in all_promos_merged:
        is_duplicate = False

        # Build composite key for Fountain Tire deduplication
        service_clean = normalize_text_for_dedup(promo.get("service_name", ""))
        desc_clean = normalize_text_for_dedup(promo.get("promo_description", ""))
        offer_clean = normalize_text_for_dedup(promo.get("offer_details", ""))
        composite_key = service_clean + desc_clean + offer_clean

        # Check against seen promotions using Fountain Tire-specific rules
        for seen_promo in seen_promos:
            if are_promos_duplicate(promo, seen_promo):
                logger.info(f"Removed duplicate Fountain Tire promo: {promo.get('service_name', 'N/A')[:50]} (matches {seen_promo.get('service_name', 'N/A')[:50]})")
                is_duplicate = True
                break

        # Also check composite key (for exact matches)
        if composite_key and composite_key in seen_keys:
            logger.info(f"Removed duplicate Fountain Tire promo (composite key match): {promo.get('service_name', 'N/A')[:50]}")
            is_duplicate = True

        if not is_duplicate:
            deduplicated.append(promo)
            seen_promos.append(promo)
            if composite_key:
                seen_keys.add(composite_key)

    logger.info(f"Total unique Fountain Tire promotions found: {len(deduplicated)}")
    return deduplicated


def extract_discount_value(text: str) -> Optional[str]:
    """Extract discount value from text."""
    text_lower = text.lower()

    # Try dollar amount first
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
    ]

    for pattern in code_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    return None


def extract_expiry_date(text: str) -> Optional[str]:
    """Extract expiry date from text."""
    date_patterns = [
        r'(?:expires?|valid until|until)[:\s]+([A-Za-z]+\s+\d{1,2}[,\s]+\d{4})',
        r'(?:expires?|valid until|until)[:\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)

    return None


def scrape_fountain(competitor: Dict) -> Dict:
    """Main entry point for Fountain Tire scraper."""
    from app.utils.extraction_flow import unified_extraction_flow, format_for_google_sheets

    try:
        # Use unified extraction flow
        promos = unified_extraction_flow(competitor, process_fountain_promotions)

        # Format for Google Sheets
        formatted_promos = [format_for_google_sheets(promo) for promo in promos]

        # Save results
        output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'fountain').lower().replace(' ', '_')}.json"
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
        logger.error(f"Error scraping Fountain Tire: {e}", exc_info=True)
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

    # Find Fountain Tire
    fountain = next((c for c in competitors if "fountain" in c.get("name", "").lower()), None)

    if not fountain:
        logger.error("Fountain Tire not found in competitor list")
        sys.exit(1)

    result = scrape_fountain(fountain)
    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")
    print(f"\n📊 Summary:")
    for promo in result.get("promotions", []):
        print(f"   • {promo.get('promotion_title', 'N/A')}: {promo.get('discount_value', 'N/A')}")

