"""Speedy Auto Service scraper - Image OCR from PDF link images."""
import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import hashlib
from fuzzywuzzy import fuzz

from app.extractors.firecrawl.firecrawl_client import fetch_with_firecrawl
from app.extractors.html_parser import find_promo_pdf_links_with_images
from app.extractors.images.image_downloader import download_image, get_image_hash, normalize_url
from app.extractors.ocr.ocr_processor import ocr_image, detect_promo_keywords
from app.extractors.ocr.llm_cleaner import clean_promo_text_with_llm
from app.extractors.pdf.pdf_extractor import download_pdf, extract_text_from_pdf
from app.config.constants import PROMO_KEYWORDS, DATA_DIR
from app.utils.logging_utils import setup_logger
from app.utils.promo_builder import build_standard_promo, load_existing_promos, get_google_reviews_for_competitor
from app.extractors.serpapi.business_overview_extractor import extract_promo_from_ai_overview

logger = setup_logger(__name__, "speedy_scraper.log")

DATA_DIR.mkdir(parents=True, exist_ok=True)
PROMOTIONS_DIR = DATA_DIR / "promotions"
PROMOTIONS_DIR.mkdir(parents=True, exist_ok=True)


def normalize_pdf_url(url: str) -> str:
    """Normalize PDF URL for deduplication."""
    return url.lower().strip().rstrip('/')


def calculate_ocr_hash(ocr_text: str) -> str:
    """Calculate hash of OCR text for similarity checking."""
    normalized = " ".join(ocr_text.lower().strip().split())
    return hashlib.md5(normalized.encode()).hexdigest()


def are_texts_similar(text1: str, text2: str, threshold: int = 85) -> bool:
    """Check if two OCR texts are similar using token set ratio."""
    if not text1 or not text2:
        return False

    similarity = fuzz.token_set_ratio(text1.lower(), text2.lower())
    return similarity >= threshold


def process_speedy_promotions(competitor: Dict) -> List[Dict]:
    """Process Speedy Auto Service promotions using image OCR from PDF links."""
    logger.info(f"Processing promotions for {competitor.get('name')}")

    promo_links = competitor.get("promo_links", [])
    if not promo_links:
        logger.warning(f"No promo_links found for {competitor.get('name')}")
        return []

    # Get Google Reviews once for this competitor
    google_reviews = get_google_reviews_for_competitor(competitor)

    all_promos = []
    seen_pdf_urls = set()
    seen_ocr_hashes = {}  # Dict to store hash -> text mapping
    seen_image_hashes = set()

    for promo_url in promo_links:
        logger.info(f"Fetching {promo_url}")

        # Step 1: Fetch with Firecrawl (longer timeout for heavy pages)
        firecrawl_result = fetch_with_firecrawl(promo_url, timeout=90)

        if firecrawl_result.get("error"):
            logger.error(f"Firecrawl error: {firecrawl_result['error']}")
            continue

        html = firecrawl_result.get("html", "")
        if not html:
            logger.warning(f"No HTML content from Firecrawl for {promo_url}")
            continue

        # Step 2: Find PDF links with images
        pdf_link_data = find_promo_pdf_links_with_images(html, promo_url)
        logger.info(f"Found {len(pdf_link_data)} PDF links with images")

        # Step 3: Process each PDF link's image
        for link_data in pdf_link_data:
            pdf_url = link_data["pdf_url"]
            normalized_pdf = normalize_pdf_url(pdf_url)
            image_url = link_data["image_url"]

            logger.info(f"Processing: {pdf_url}")

            # Deduplicate by normalized PDF URL
            if normalized_pdf in seen_pdf_urls:
                logger.info(f"Skipping duplicate PDF URL: {pdf_url}")
                continue
            seen_pdf_urls.add(normalized_pdf)

            # Download image (or fallback to PDF if image is a placeholder/data URI)
            normalized_img_url = normalize_url(promo_url, image_url)
            ocr_text = ""
            img_path = None
            pdf_path = None

            # Check if image_url is actually the PDF URL (meaning no image was found)
            if (not normalized_img_url or image_url.startswith("data:") or
                image_url == pdf_url or image_url.endswith(".pdf")):
                # Image is a placeholder/data URI or PDF URL → use PDF extraction as fallback
                logger.info(f"Image is placeholder/data URI/PDF URL, extracting text from PDF instead: {pdf_url[:80]}...")
                pdf_path = download_pdf(pdf_url)
                if pdf_path:
                    logger.info(f"PDF downloaded successfully, extracting text from {pdf_path.name}...")
                    ocr_text = extract_text_from_pdf(pdf_path)
                    if ocr_text:
                        logger.info(f"Extracted {len(ocr_text)} characters from PDF")
                    else:
                        logger.warning(f"No text extracted from PDF {pdf_path.name} - file may be corrupted, skipping...")
                        # Skip this PDF and continue to next one
                        continue
                else:
                    logger.warning(f"Failed to download PDF: {pdf_url}")
                    continue
            else:
                # Normal image → download and use OCR
                logger.info(f"Downloading image: {normalized_img_url[:80]}...")
                img_path = download_image(normalized_img_url)

                if not img_path:
                    logger.warning(f"Failed to download image, falling back to PDF extraction: {image_url}")
                    pdf_path = download_pdf(pdf_url)
                    if pdf_path:
                        ocr_text = extract_text_from_pdf(pdf_path)
                    else:
                        logger.warning(f"Failed to download PDF as fallback: {pdf_url}")
                        continue
                else:
                    # Check for duplicate image (same file content)
                    img_hash = get_image_hash(img_path)
                    if img_hash and img_hash in seen_image_hashes:
                        logger.info(f"Skipping duplicate image content: {image_url}")
                        img_path.unlink()
                        continue
                    seen_image_hashes.add(img_hash)

                    # Step 4: Run OCR (Google Vision primary)
                    logger.info(f"Running OCR on {img_path.name}...")
                    ocr_text = ocr_image(img_path)

            # Validate extracted text
            if not ocr_text or len(ocr_text.strip()) < 10:
                logger.warning(f"No text extracted from {image_url or pdf_url}")
                if img_path:
                    img_path.unlink()
                if pdf_path:
                    pdf_path.unlink()
                continue

            # Deduplicate by OCR text similarity AND service category
            # Only consider duplicate if BOTH text is similar AND service category matches
            ocr_hash = calculate_ocr_hash(ocr_text)
            is_duplicate = False

            # Extract service name early (before LLM cleaning) for better deduplication
            # Try PDF filename first (most reliable)
            from pathlib import Path
            pdf_filename = Path(pdf_url).stem.lower()
            detected_service = "other"
            from app.config.constants import SERVICE_MAP
            for keyword, cat in SERVICE_MAP.items():
                if keyword in pdf_filename:
                    detected_service = cat
                    break

            # If not in filename, check OCR text
            if detected_service == "other":
                ocr_lower = ocr_text.lower()
                for keyword, cat in SERVICE_MAP.items():
                    if keyword in ocr_lower:
                        detected_service = cat
                        break

            # Extract discount value from OCR text for comparison
            ocr_lower = ocr_text.lower()
            extracted_discount = None
            import re
            if "%" in ocr_text:
                percent_match = re.search(r'(\d+)\s*%', ocr_text)
                if percent_match:
                    extracted_discount = f"{percent_match.group(1)}%"
            elif "$" in ocr_text:
                dollar_match = re.search(r'\$(\d+)', ocr_text)
                if dollar_match:
                    extracted_discount = f"${dollar_match.group(1)}"
            elif "free" in ocr_lower:
                extracted_discount = "free"

            # Check against existing promos for similarity
            for promo in all_promos:
                existing_text = promo.get("offer_details", "")
                existing_service = promo.get("service_name", "other")
                existing_discount = promo.get("discount_value")

                # Only consider duplicate if ALL conditions met:
                # 1. Text is similar (85%+)
                # 2. Service category is the same
                # 3. Discount value is the same (or both None/empty)
                if existing_text and are_texts_similar(ocr_text, existing_text):
                    if existing_service == detected_service:
                        # Check if discount values match (or both are None/empty)
                        discount_match = (
                            (not extracted_discount and not existing_discount) or
                            (extracted_discount and existing_discount and
                             extracted_discount.lower() == existing_discount.lower())
                        )

                        if discount_match:
                            similarity = fuzz.token_set_ratio(ocr_text.lower(), existing_text.lower())
                            logger.info(f"Skipping duplicate: same service '{detected_service}', same discount '{existing_discount}', {similarity}% text similarity")
                            is_duplicate = True
                            break
                        else:
                            # Same service but different discount - keep both
                            logger.debug(f"Same service '{detected_service}' but different discounts: '{existing_discount}' vs '{extracted_discount}' - keeping both")
                    else:
                        # Different service categories - NOT a duplicate even if text is similar
                        logger.debug(f"Text similar but different services: '{existing_service}' vs '{detected_service}' - keeping both")

            if is_duplicate:
                if img_path:
                    img_path.unlink()
                if pdf_path:
                    pdf_path.unlink()
                continue

            # Store for future comparisons
            seen_ocr_hashes[ocr_hash] = ocr_text

            # Step 5: Check if it's promo-related - be more lenient
            is_promo = detect_promo_keywords(ocr_text, PROMO_KEYWORDS)

            # Also check for discount patterns, service keywords, or common promo phrases
            import re
            has_discount = bool(re.search(r'\$(\d+)|(\d+)%|free|save', ocr_text, re.IGNORECASE))
            has_service_keyword = any(kw in ocr_text.lower() for kw in ["oil", "brake", "tire", "battery", "service", "change", "inspection"])
            has_promo_phrase = any(phrase in ocr_text.lower() for phrase in ["off", "rebate", "coupon", "special", "limited time", "promo"])

            if not is_promo and not (has_discount or has_service_keyword or has_promo_phrase):
                logger.info(f"Content doesn't contain promo keywords or relevant data: {image_url or pdf_url}")
                if img_path:
                    img_path.unlink()
                if pdf_path:
                    pdf_path.unlink()
                continue

            logger.info(f"OCR extracted {len(ocr_text)} characters")

            # Step 5b: Extract service name EARLY from PDF filename or OCR text for deduplication
            ocr_lower = ocr_text.lower()
            service_name = "other"
            category = "other"

            # First, try to extract from PDF filename (more reliable)
            pdf_filename = Path(pdf_url).stem.lower()
            from app.config.constants import SERVICE_MAP
            for keyword, cat in SERVICE_MAP.items():
                if keyword in pdf_filename:
                    service_name = cat
                    category = cat
                    break

            # If not found in filename, check OCR text
            if service_name == "other":
                for keyword, cat in SERVICE_MAP.items():
                    if keyword in ocr_lower:
                        service_name = cat
                        category = cat
                        break

            # Step 6: Clean with LLM
            context = f"Speedy Auto Service promotion coupon. Alt text: {link_data.get('alt_text', '')}"
            cleaned_data = clean_promo_text_with_llm(ocr_text, context)

            if not cleaned_data:
                # Fallback: use raw OCR text with basic extraction
                logger.warning("LLM cleaning failed, using raw OCR text with basic extraction")

                # Extract discount value
                import re
                discount_value = None
                if "%" in ocr_text:
                    percent_match = re.search(r'(\d+)\s*%', ocr_text)
                    if percent_match:
                        discount_value = f"{percent_match.group(1)}%"
                elif "$" in ocr_text:
                    dollar_match = re.search(r'\$(\d+)', ocr_text)
                    if dollar_match:
                        discount_value = f"${dollar_match.group(1)}"
                elif "free" in ocr_lower:
                    discount_value = "free"

                # Extract expiry date
                expiry_date = None
                date_match = re.search(r'(?:expires?|expiry|valid until)[\s:]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', ocr_text, re.IGNORECASE)
                if date_match:
                    expiry_date = date_match.group(1).strip()

                cleaned_data = {
                    "service_name": service_name,
                    "promo_description": ocr_text[:500],
                    "discount_value": discount_value,
                    "coupon_code": None,
                    "expiry_date": expiry_date,
                    "category": category
                }
            else:
                # LLM might have extracted service, but use our detection if LLM says "other"
                if cleaned_data.get("service_name") == "other" and service_name != "other":
                    cleaned_data["service_name"] = service_name
                    cleaned_data["category"] = category

            # Step 7: Build standardized promo object
            service_name = cleaned_data.get("service_name", "other")
            promo_description = cleaned_data.get("promo_description", ocr_text[:500]) or f"{service_name} promotion"  # Ensure never empty
            category = cleaned_data.get("category", cleaned_data.get("service_name", "other"))

            # Build offer_details from LLM or extracted values
            offer_details = cleaned_data.get("offer_details")
            if not offer_details:
                offer_parts = []
                discount_val = cleaned_data.get("discount_value")
                coupon_code_val = cleaned_data.get("coupon_code")
                expiry_date_val = cleaned_data.get("expiry_date")
                if discount_val:
                    offer_parts.append(f"Discount: {discount_val}")
                if coupon_code_val:
                    offer_parts.append(f"Code: {coupon_code_val}")
                if expiry_date_val:
                    offer_parts.append(f"Expires: {expiry_date_val}")
                if offer_parts:
                    offer_details = ". ".join(offer_parts) + ". " + ocr_text[:500]
                else:
                    offer_details = ocr_text[:1000] or f"{service_name} rebate details"  # Ensure never empty
            else:
                # Ensure offer_details is never empty even if it exists but is invalid
                if not offer_details.strip():
                    offer_details = ocr_text[:1000] or f"{service_name} rebate details"

            # Ensure ad_title is never empty (required field)
            alt_text = link_data.get("alt_text", "")
            ad_title = alt_text if alt_text and len(alt_text.strip()) > 0 else (cleaned_data.get("service_name", "") if cleaned_data.get("service_name") else service_name) or "Speedy Auto Service Promotion"

            # Ensure ad_text is never empty (required field)
            context_text = link_data.get("context", "")
            ad_text_final = context_text[:200] if context_text and len(context_text.strip()) > 0 else (ocr_text[:200] if ocr_text and len(ocr_text.strip()) > 0 else f"{service_name} promotion")

            # Load existing promos for comparison
            output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'speedy').lower().replace(' ', '_')}.json"
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
                ad_title=ad_title,
                ad_text=ad_text_final,
                google_reviews=google_reviews,
                existing_promo=existing_promo
            )

            all_promos.append(promo)
            logger.info(f"[OK] Added promo: {promo.get('service_name')} - {promo.get('new_or_updated', 'NEW')}")

    logger.info(f"Total unique promotions found: {len(all_promos)}")
    return all_promos


def scrape_speedy(competitor: Dict) -> Dict:
    """Main entry point for Speedy scraper."""
    from app.utils.extraction_flow import unified_extraction_flow, format_for_google_sheets

    try:
        # Use unified extraction flow
        promos = unified_extraction_flow(competitor, process_speedy_promotions)

        # Format for Google Sheets
        formatted_promos = [format_for_google_sheets(promo) for promo in promos]

        # Save results
        output_file = PROMOTIONS_DIR / f"{competitor.get('name', 'speedy').lower().replace(' ', '_')}.json"
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
        logger.error(f"Error scraping Speedy: {e}", exc_info=True)
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

    # Find Speedy
    speedy = next((c for c in competitors if "speedy" in c.get("name", "").lower()), None)

    if not speedy:
        logger.error("Speedy Auto Service not found in competitor list")
        sys.exit(1)

    result = scrape_speedy(speedy)
    print(f"\n[OK] Scraping complete!")
    print(f"  Found {result.get('count', 0)} promotions")
    print(f"  Saved to: {PROMOTIONS_DIR}")
