"""Integra Tire Auto Centre scraper - Image OCR for tire rebates."""
import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import hashlib
import re

from app.extractors.firecrawl.firecrawl_client import fetch_with_firecrawl
from app.extractors.html_parser import find_images_by_css_selector
from app.extractors.images.image_downloader import download_image, get_image_hash, normalize_url
from app.extractors.ocr.ocr_processor import ocr_image, detect_promo_keywords
from app.extractors.ocr.llm_cleaner import clean_promo_text_with_llm
from app.config.constants import PROMO_KEYWORDS, DATA_DIR
from app.utils.logging_utils import setup_logger
from app.utils.promo_builder import build_standard_promo, load_existing_promos, apply_ai_overview_fallback, get_google_reviews_for_competitor

logger = setup_logger(__name__, "integra_scraper.log")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMOTIONS_DIR = DATA_DIR / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)


def normalize_title(title: str) -> str:
    """Normalize title for deduplication."""
    return " ".join(title.lower().strip().split())


def extract_rebate_details_from_text(text: str, alt_text: str = "") -> Dict:
    """Extract rebate details from OCR text or alt text."""
    # Try OCR text first, fallback to alt text
    source_text = text if text and len(text.strip()) > 10 else alt_text
    if not source_text:
        return {}

    text_lower = source_text.lower()

    # Extract rebate amount ($)
    rebate_amount = None
    dollar_match = re.search(r'\$(\d+(?:\.\d+)?)', source_text)
    if dollar_match:
        rebate_amount = f"${dollar_match.group(1)}"

    # Extract percentage discount
    percent_match = re.search(r'(\d+)\s*%', source_text)
    if percent_match and not rebate_amount:
        rebate_amount = f"{percent_match.group(1)}%"

    # Extract brand name (look for common tire brands)
    tire_brands = [
        "michelin", "bridgestone", "goodyear", "continental", "pirelli",
        "bfgoodrich", "toyo", "nitto", "hankook", "falken", "kumho",
        "yokohama", "dunlop", "firestone", "general", "cooper", "uniroyal"
    ]
    brand_name = None
    for brand in tire_brands:
        if brand in text_lower:
            brand_name = brand.title()
            break

    # Extract expiry date
    expiry_date = None
    # Try various date patterns
    date_patterns = [
        r'(?:expires?|expiry|valid until|until)[\s:]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
    ]
    for pattern in date_patterns:
        date_match = re.search(pattern, source_text, re.IGNORECASE)
        if date_match:
            expiry_date = date_match.group(1).strip()
            break

    # Extract eligibility (look for common terms)
    eligibility = None
    if "mail" in text_lower or "rebate" in text_lower:
        eligibility = "Mail-in rebate"
    elif "instant" in text_lower:
        eligibility = "Instant rebate"

    return {
        "rebate_amount": rebate_amount,
        "brand_name": brand_name,
        "expiry_date": expiry_date,
        "eligibility": eligibility
    }


def process_integra_promotions(competitor: Dict) -> List[Dict]:
    """Process Integra Tire promotions using image OCR."""
    logger.info(f"Processing promotions for {competitor.get('name')}")

    promo_links = competitor.get("promo_links", [])
    if not promo_links:
        logger.warning(f"No promo_links found for {competitor.get('name')}")
        return []

    # Get Google Reviews once for this competitor
    google_reviews = get_google_reviews_for_competitor(competitor)

    all_promos = []
    seen_image_urls = set()
    seen_titles = set()
    seen_image_hashes = set()

    for promo_url in promo_links:
        logger.info(f"Fetching {promo_url}")

        # Step 1: Fetch with Firecrawl
        firecrawl_result = fetch_with_firecrawl(promo_url, timeout=90)

        if firecrawl_result.get("error"):
            logger.error(f"Firecrawl error: {firecrawl_result['error']}")
            continue

        html = firecrawl_result.get("html", "")
        if not html:
            logger.warning(f"No HTML content from Firecrawl for {promo_url}")
            continue

        # Step 2: Find images using CSS selector
        images = find_images_by_css_selector(html, promo_url, "img.single-rebate")
        logger.info(f"Found {len(images)} rebate images")

        if not images:
            logger.warning(f"No images found with selector 'img.single-rebate'")
            continue

        # Step 3: Process each image
        for img_data in images:
            image_url = img_data["image_url"]
            alt_text = img_data.get("alt_text", "")

            # Normalize image URL for deduplication
            normalized_img_url = normalize_url(promo_url, image_url).lower().strip()

            # Skip if we've seen this image URL before
            if normalized_img_url in seen_image_urls:
                logger.info(f"Skipping duplicate image URL: {image_url[:80]}...")
                continue
            seen_image_urls.add(normalized_img_url)

            # Download image
            logger.info(f"Downloading image: {image_url[:80]}...")
            img_path = download_image(normalize_url(promo_url, image_url))

            if not img_path:
                logger.warning(f"Failed to download image: {image_url}")
                continue

            # Check for duplicate image (same file content)
            img_hash = get_image_hash(img_path)
            if img_hash and img_hash in seen_image_hashes:
                logger.info(f"Skipping duplicate image content: {image_url}")
                img_path.unlink()
                continue
            seen_image_hashes.add(img_hash)

            # Step 4: Run OCR
            logger.info(f"Running OCR on {img_path.name}...")
            ocr_text = ocr_image(img_path)

            # Step 5: If OCR fails, try alt text as fallback
            if not ocr_text or len(ocr_text.strip()) < 10:
                if alt_text and len(alt_text.strip()) > 10:
                    logger.info(f"OCR failed, using alt text as fallback")
                    ocr_text = alt_text
                else:
                    logger.warning(f"No OCR text or alt text extracted from {image_url}")
                    img_path.unlink()
                    continue

            # Step 6: Extract rebate details from text FIRST
            rebate_details = extract_rebate_details_from_text(ocr_text, alt_text)

            # CRITICAL: Extract brand name from image URL/filename BEFORE promo check
            # This ensures we can identify rebates even if OCR doesn't contain brand name
            if not rebate_details.get("brand_name"):
                # Check image URL/filename for brand names
                image_url_lower = image_url.lower()
                tire_brands = [
                    "michelin", "bridgestone", "goodyear", "continental", "pirelli",
                    "bfgoodrich", "toyo", "nitto", "hankook", "falken", "kumho",
                    "yokohama", "dunlop", "firestone", "general", "cooper", "uniroyal",
                    "hercules", "nexen", "laufenn", "yokohoma"  # Note: yokohoma is a typo in some URLs
                ]
                for brand in tire_brands:
                    if brand in image_url_lower:
                        rebate_details["brand_name"] = brand.title()
                        logger.info(f"Extracted brand name '{brand.title()}' from image URL: {image_url[:80]}")
                        break

            # Step 7: Check if it's promo-related
            # For tire rebates, be more lenient - check if we have brand name, rebate amount, or keywords
            is_promo = detect_promo_keywords(ocr_text, PROMO_KEYWORDS)

            # Also consider it a promo if we found rebate amount or brand name (now includes URL-extracted brands)
            if not is_promo and not rebate_details.get("rebate_amount") and not rebate_details.get("brand_name"):
                # Last check: if alt text contains tire brand or rebate keywords
                alt_lower = alt_text.lower()
                has_tire_brand = any(brand in alt_lower for brand in ["tire", "bridgestone", "michelin", "goodyear", "bfgoodrich", "continental", "pirelli", "toyo", "falken", "hankook", "kumho", "yokohama", "dunlop", "firestone", "general", "cooper", "uniroyal", "nexen", "hercules"])
                has_rebate_keyword = any(kw in alt_lower for kw in ["rebate", "off", "discount", "save", "promo"])

                if not has_tire_brand and not has_rebate_keyword:
                    logger.info(f"Image doesn't contain promo keywords or rebate details: {image_url}")
                    img_path.unlink()
                    continue

            # Step 8: Clean with LLM
            context = f"Integra Tire rebate promotion. Alt text: {alt_text}"
            cleaned_data = clean_promo_text_with_llm(ocr_text, context)

            # Build promotion title - ALWAYS include brand name if available to avoid deduplication
            # This ensures each brand rebate (Bridgestone, Michelin, etc.) is treated as unique
            if rebate_details.get("brand_name"):
                # Brand name is available - prioritize it in title
                if cleaned_data and cleaned_data.get("service_name"):
                    # Combine brand with service name
                    promotion_title = f"{rebate_details['brand_name']} {cleaned_data.get('service_name')}"
                else:
                    promotion_title = f"{rebate_details['brand_name']} Rebate"
            elif cleaned_data and cleaned_data.get("service_name"):
                promotion_title = cleaned_data.get("service_name")
            elif alt_text:
                promotion_title = alt_text[:100]
            else:
                # Extract first line or key phrase from OCR
                first_line = ocr_text.split("\n")[0].strip()[:100]
                promotion_title = first_line if first_line else "Tire Rebate"

            # REMOVED: Title-based deduplication - we already check image URLs at line 138-142
            # Each unique image URL will create a unique promotion
            # No need for additional deduplication here since seen_image_urls already handles it

            # Use LLM cleaned data if available, otherwise use extracted details
            if cleaned_data:
                # Always prioritize brand name in service_name to make each rebate unique
                if rebate_details.get("brand_name"):
                    service_name = f"{rebate_details['brand_name']} {cleaned_data.get('service_name', 'Tire Rebate')}"
                else:
                    service_name = cleaned_data.get("service_name", rebate_details.get("brand_name", "tires"))
                promo_description = cleaned_data.get("promo_description", ocr_text[:500])
                category = cleaned_data.get("category", "tires")
                offer_details = cleaned_data.get("offer_details")
                if not offer_details:
                    offer_parts = []
                    discount_val = cleaned_data.get("discount_value") or rebate_details.get("rebate_amount")
                    coupon_code_val = cleaned_data.get("coupon_code")
                    expiry_date_val = cleaned_data.get("expiry_date") or rebate_details.get("expiry_date")
                    if discount_val:
                        offer_parts.append(f"Discount: {discount_val}")
                    if coupon_code_val:
                        offer_parts.append(f"Code: {coupon_code_val}")
                    if expiry_date_val:
                        offer_parts.append(f"Expires: {expiry_date_val}")
                    if offer_parts:
                        offer_details = ". ".join(offer_parts) + ". " + ocr_text[:500]
                    else:
                        offer_details = ocr_text[:1000]
            else:
                # If no LLM data, use brand name if available, otherwise generic
                if rebate_details.get("brand_name"):
                    service_name = f"{rebate_details['brand_name']} Tire Rebate"
                else:
                    service_name = "tires"
                promo_description = ocr_text[:500]
                category = "tires"
                offer_parts = []
                discount_val = rebate_details.get("rebate_amount")
                expiry_date_val = rebate_details.get("expiry_date")
                if discount_val:
                    offer_parts.append(f"Discount: {discount_val}")
                if expiry_date_val:
                    offer_parts.append(f"Expires: {expiry_date_val}")
                if offer_parts:
                    offer_details = ". ".join(offer_parts) + ". " + ocr_text[:500]
                else:
                    offer_details = ocr_text[:1000]

            # Load existing promos for comparison
            output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'integra').lower().replace(' ', '_')}.json"
            existing_promos = load_existing_promos(output_file)
            promo_key = f"{promo_url}::{service_name}"
            existing_promo = existing_promos.get(promo_key)

            # Ensure ad_text is never empty (required for validation)
            # Use alt_text if available, otherwise OCR text, otherwise fallback
            ad_text_value = alt_text[:200] if alt_text and len(alt_text.strip()) > 0 else (ocr_text[:200] if ocr_text and len(ocr_text.strip()) > 0 else f"{service_name} rebate promotion")

            # Ensure promo_description is never empty
            if not promo_description or len(promo_description.strip()) == 0:
                promo_description = f"{service_name} rebate offer" if rebate_details.get("brand_name") else "Tire rebate promotion"

            # Ensure offer_details is never empty
            if not offer_details or len(offer_details.strip()) == 0:
                offer_details = ocr_text[:500] if ocr_text and len(ocr_text.strip()) > 0 else f"{service_name} rebate details available"

            # Build standardized promo object
            promo = build_standard_promo(
                competitor=competitor,
                promo_url=promo_url,
                service_name=service_name,
                promo_description=promo_description,
                category=category,
                offer_details=offer_details,
                ad_title=promotion_title,
                ad_text=ad_text_value,
                google_reviews=google_reviews,
                existing_promo=existing_promo
            )

            all_promos.append(promo)
            logger.info(f"[OK] Added promo: {promo.get('service_name', 'N/A')} - {promo.get('new_or_updated', 'NEW')}")

    logger.info(f"Total unique promotions found: {len(all_promos)}")
    return all_promos


def scrape_integra(competitor: Dict) -> Dict:
    """Main entry point for Integra Tire scraper."""
    from app.utils.extraction_flow import unified_extraction_flow, format_for_google_sheets

    try:
        # Use unified extraction flow
        promos = unified_extraction_flow(competitor, process_integra_promotions)

        # Format for Google Sheets
        formatted_promos = [format_for_google_sheets(promo) for promo in promos]

        # Save results
        output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'integra').lower().replace(' ', '_')}.json"
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
        logger.error(f"Error scraping Integra Tire: {e}", exc_info=True)
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

    # Find Integra Tire
    integra = next((c for c in competitors if "integra" in c.get("name", "").lower()), None)

    if not integra:
        logger.error("Integra Tire Auto Centre not found in competitor list")
        sys.exit(1)

    result = scrape_integra(integra)
    print(f"\n✅ Scraping complete!")
    print(f"   Found {result.get('count', 0)} promotions")
    print(f"   Saved to: data/promotions/")
    print(f"\n📊 Summary:")
    for promo in result.get("promotions", []):
        print(f"   • {promo.get('promotion_title', 'N/A')}: {promo.get('discount_value', 'N/A')}")

