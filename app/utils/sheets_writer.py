"""Google Sheets writer for promotion data."""
import os
from typing import List, Dict, Optional
from pathlib import Path
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from app.config.constants import ROOT
from app.utils.logging_utils import setup_logger

logger = setup_logger(__name__)

# Google Sheets API scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Column order (exact as specified)
COLUMN_ORDER = [
    "website",
    "page_url",
    "business_name",
    "google_reviews",
    "service_name",
    "promo_description",
    "category",
    "contact",
    "location",
    "offer_details",
    "ad_title",
    "ad_text",
    "new_or_updated",
    "date_scraped"
]


def get_sheets_service():
    """Initialize Google Sheets API service."""
    try:
        # Check for credentials file
        creds_path = ROOT / "service_account.json"

        if not creds_path.exists():
            # Try environment variable
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if not creds_path:
                logger.error("No Google Sheets credentials found. Need service_account.json or GOOGLE_APPLICATION_CREDENTIALS")
                return None

        creds = service_account.Credentials.from_service_account_file(
            str(creds_path), scopes=SCOPES
        )

        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        logger.error(f"Error initializing Google Sheets service: {e}")
        return None


def clean_text_for_sheets(text: str) -> str:
    """
    Clean text for Google Sheets: remove markdown, bullets, extra spaces.

    Args:
        text: Raw text to clean

    Returns:
        Clean plain text
    """
    if not text:
        return ""

    # Remove markdown
    text = text.replace("**", "").replace("*", "").replace("__", "").replace("_", "")
    text = text.replace("#", "").replace("##", "").replace("###", "")
    text = text.replace("```", "").replace("`", "")

    # Remove bullets and list markers
    text = text.replace("•", "").replace("·", "").replace("- ", "").replace("• ", "")
    text = text.replace("\n- ", " ").replace("\n• ", " ")

    # Clean up whitespace
    text = " ".join(text.split())

    # Remove leading/trailing punctuation that might be left over
    text = text.strip(".,;:!?")

    return text.strip()


def format_service_name(service_name: str) -> str:
    """
    Format service_name to Title Case, short and clean.

    Args:
        service_name: Raw service name

    Returns:
        Formatted service name
    """
    if not service_name:
        return "Auto Service"

    # Clean first
    service_name = clean_text_for_sheets(service_name)

    # Convert to Title Case
    words = service_name.split()
    title_case = " ".join(word.capitalize() for word in words)

    # Keep it short (max 3 words)
    words = title_case.split()[:3]
    return " ".join(words)


def format_category(category: str) -> str:
    """
    Format category to single keyword.

    Args:
        category: Raw category

    Returns:
        Single keyword category
    """
    if not category:
        return "Auto Service"

    category = clean_text_for_sheets(category)

    # Map common variations
    category_lower = category.lower()
    category_map = {
        "oil change": "Oil Change",
        "oil": "Oil Change",
        "tire": "Tires",
        "tires": "Tires",
        "brake": "Brakes",
        "brakes": "Brakes",
        "battery": "Battery",
        "alignment": "Alignment",
        "financing": "Financing",
        "inspection": "Inspection",
        "service": "Auto Service"
    }

    for key, value in category_map.items():
        if key in category_lower:
            return value

    # Default: capitalize first word
    return category.split()[0].capitalize() if category.split() else "Auto Service"


def format_offer_details(offer_details: str) -> str:
    """
    Format offer_details to highlight core offer.
    Example: "$20 off oil change | Code: SAVE20"

    Args:
        offer_details: Raw offer details

    Returns:
        Formatted offer details
    """
    if not offer_details:
        return ""

    offer_details = clean_text_for_sheets(offer_details)

    # Extract key info: discount, code, expiry
    parts = []

    # Look for discount patterns
    import re
    discount_patterns = [
        r'\$(\d+)',
        r'(\d+)%',
        r'(\d+)\s*off',
        r'save\s*\$?(\d+)',
        r'(\d+)\s*dollars?'
    ]

    for pattern in discount_patterns:
        match = re.search(pattern, offer_details, re.IGNORECASE)
        if match:
            value = match.group(1)
            if '$' in pattern or 'dollar' in pattern:
                parts.append(f"${value} off")
            elif '%' in pattern:
                parts.append(f"{value}% off")
            else:
                parts.append(f"${value} off")
            break

    # Look for coupon code
    code_patterns = [
        r'code[:\s]+([A-Z0-9]+)',
        r'coupon[:\s]+([A-Z0-9]+)',
        r'promo[:\s]+([A-Z0-9]+)'
    ]

    for pattern in code_patterns:
        match = re.search(pattern, offer_details, re.IGNORECASE)
        if match:
            code = match.group(1).upper()
            parts.append(f"Code: {code}")
            break

    # If we found parts, join them
    if parts:
        return " | ".join(parts)

    # Otherwise, return first 100 chars cleaned
    return offer_details[:100]


def format_promo_description(description: str) -> str:
    """
    Format promo_description to one clear sentence.

    Args:
        description: Raw description

    Returns:
        One clear sentence
    """
    if not description:
        return ""

    description = clean_text_for_sheets(description)

    # Take first sentence
    sentences = description.split('.')
    if sentences:
        first_sentence = sentences[0].strip()
        # Ensure it ends with period
        if first_sentence and not first_sentence.endswith(('.', '!', '?')):
            first_sentence += '.'
        return first_sentence

    return description[:200]


def format_ad_text(ad_text: str) -> str:
    """
    Format ad_text to short marketing paragraph.

    Args:
        ad_text: Raw ad text

    Returns:
        Short marketing paragraph
    """
    if not ad_text:
        return ""

    ad_text = clean_text_for_sheets(ad_text)

    # Keep it to 2-3 sentences max
    sentences = ad_text.split('.')
    if len(sentences) > 3:
        ad_text = '. '.join(sentences[:3])
        if not ad_text.endswith('.'):
            ad_text += '.'

    return ad_text[:300]  # Max 300 chars


def clean_promo_for_sheets(promo: Dict) -> Dict:
    """
    Clean and format a single promo dict for Google Sheets.

    Args:
        promo: Raw promo dict

    Returns:
        Cleaned promo dict ready for sheets
    """
    cleaned = {}

    # Clean each field according to rules
    cleaned["website"] = clean_text_for_sheets(promo.get("website", ""))
    cleaned["page_url"] = promo.get("page_url", "")
    cleaned["business_name"] = clean_text_for_sheets(promo.get("business_name", ""))

    # Google Reviews - keep as number or None
    google_reviews = promo.get("google_reviews")
    cleaned["google_reviews"] = google_reviews if google_reviews is not None else ""

    # Format service_name to Title Case
    cleaned["service_name"] = format_service_name(promo.get("service_name", ""))

    # Format promo_description to one sentence
    cleaned["promo_description"] = format_promo_description(promo.get("promo_description", ""))

    # Format category to single keyword
    cleaned["category"] = format_category(promo.get("category", ""))

    cleaned["contact"] = clean_text_for_sheets(promo.get("contact", ""))
    cleaned["location"] = clean_text_for_sheets(promo.get("location", ""))

    # Format offer_details
    cleaned["offer_details"] = format_offer_details(promo.get("offer_details", ""))

    # Format ad_title (headline-like)
    cleaned["ad_title"] = clean_text_for_sheets(promo.get("ad_title", ""))

    # Format ad_text (short marketing paragraph)
    cleaned["ad_text"] = format_ad_text(promo.get("ad_text", ""))

    # new_or_updated - ensure it's uppercase
    new_or_updated = promo.get("new_or_updated", "NEW")
    cleaned["new_or_updated"] = new_or_updated.upper() if isinstance(new_or_updated, str) else "NEW"

    # date_scraped - ensure YYYY-MM-DD format
    date_scraped = promo.get("date_scraped", "")
    cleaned["date_scraped"] = date_scraped

    return cleaned


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str) -> bool:
    """Ensure sheet tab exists, create if it doesn't."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing_sheets = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]

        if sheet_name in existing_sheets:
            return True

        # Create the sheet
        requests = [{
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }]

        body = {'requests': requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        logger.info(f"Created sheet tab: {sheet_name}")
        return True
    except Exception as e:
        logger.error(f"Error ensuring sheet exists: {e}")
        return False


def write_to_sheets(
    spreadsheet_id: str,
    all_promos: List[Dict],
    sheet_name: str = "Promotions"
) -> bool:
    """
    Write all promotions to Google Sheets with formatting.

    Args:
        spreadsheet_id: Google Sheets ID
        all_promos: List of cleaned promo dicts
        sheet_name: Name of the sheet tab

    Returns:
        True if successful, False otherwise
    """
    service = get_sheets_service()
    if not service:
        return False

    try:
        # Ensure sheet exists
        if not ensure_sheet_exists(service, spreadsheet_id, sheet_name):
            logger.error(f"Could not create or access sheet: {sheet_name}")
            return False

        # Clean all promos
        cleaned_promos = [clean_promo_for_sheets(promo) for promo in all_promos]

        # Prepare header row
        headers = COLUMN_ORDER

        # Prepare data rows (in column order)
        rows = []
        for promo in cleaned_promos:
            row = [promo.get(col, "") for col in COLUMN_ORDER]
            rows.append(row)

        # Combine headers and data
        all_rows = [headers] + rows

        # Clear existing data and write new data
        range_name = f"{sheet_name}!A1"

        # Clear the sheet first (use A1:Z1000 to avoid range errors)
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1000"
            ).execute()
        except Exception as e:
            logger.warning(f"Could not clear sheet (may be empty): {e}")

        # Write data
        body = {
            'values': all_rows
        }

        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()

        logger.info(f"Wrote {len(rows)} rows to Google Sheets")

        # Apply formatting
        apply_sheet_formatting(service, spreadsheet_id, sheet_name, len(all_rows))

        return True

    except HttpError as e:
        logger.error(f"Google Sheets API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error writing to Google Sheets: {e}", exc_info=True)
        return False


def apply_sheet_formatting(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    num_rows: int
):
    """
    Apply dashboard-style formatting to the sheet.

    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        sheet_name: Sheet name
        num_rows: Number of data rows (including header)
    """
    try:
        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if not sheet_id:
            logger.warning("Could not find sheet ID for formatting")
            return

        requests = []

        # 1. Format header row (bold, darker background)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {'red': 0.2, 'green': 0.2, 'blue': 0.2},
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'fontSize': 11,
                            'bold': True
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
            }
        })

        # 2. Format data rows (alternating colors, left-aligned, vertically centered)
        for row_idx in range(1, num_rows):
            # Alternate row colors (light gray for even rows)
            bg_color = {'red': 0.95, 'green': 0.95, 'blue': 0.95} if row_idx % 2 == 0 else {'red': 1.0, 'green': 1.0, 'blue': 1.0}

            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': row_idx,
                        'endRowIndex': row_idx + 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': bg_color,
                            'textFormat': {
                                'fontSize': 10,
                                'fontFamily': 'Roboto'
                            },
                            'horizontalAlignment': 'LEFT',
                            'verticalAlignment': 'MIDDLE'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
                }
            })

        # 3. Auto-resize columns
        requests.append({
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': len(COLUMN_ORDER)
                }
            }
        })

        # Apply all formatting requests
        body = {'requests': requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        logger.info("Applied dashboard formatting to sheet")

    except Exception as e:
        logger.warning(f"Error applying formatting: {e}")


def get_sheet_id(service, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
    """Get sheet ID by name."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        return None
    except Exception as e:
        logger.error(f"Error getting sheet ID: {e}")
        return None

