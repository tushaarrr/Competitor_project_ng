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
        "service": "General Service",
        "general": "General Service",
        "auto service": "General Service",
        "seasonal": "General Service"
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


def group_and_sort_promos(promos: List[Dict]) -> List[Dict]:
    """
    Group promotions by business_name in specific order, then sort within each group.

    Order:
    1. Fountain Tire
    2. Good News Auto
    3. Midas
    4. Kal Tire
    5. Jiffy Lube
    6. Speedy Auto Service
    7. Trail Tire Auto Centres
    8. Integra Tire Auto Centre
    9. Valvoline Express Care
    10. Mr. Lube

    Within each group, sort by service_name, then offer_details.
    """
    # Define business order (case-insensitive matching)
    business_order = [
        "Fountain Tire",
        "Good News Auto",
        "Midas",
        "Kal Tire",
        "Jiffy Lube",
        "Speedy Auto Service",
        "Trail Tire Auto Centres",
        "Integra Tire Auto Centre",
        "Valvoline Express Care",
        "Mr. Lube"
    ]

    # Create a mapping for order lookup
    def get_business_order(business_name: str) -> int:
        business_lower = business_name.lower()
        for idx, ordered_business in enumerate(business_order):
            if ordered_business.lower() in business_lower or business_lower in ordered_business.lower():
                return idx
        return 999  # Unknown businesses go to end

    # Group by business_name
    grouped = {}
    for promo in promos:
        business_name = promo.get("business_name", "").strip()
        if business_name not in grouped:
            grouped[business_name] = []
        grouped[business_name].append(promo)

    # Sort groups by order, then sort within each group
    sorted_promos = []
    for business_name in sorted(grouped.keys(), key=get_business_order):
        group_promos = grouped[business_name]
        # Sort within group: service_name, then offer_details
        group_promos.sort(key=lambda p: (
            p.get("service_name", "").lower(),
            p.get("offer_details", "").lower()
        ))
        sorted_promos.extend(group_promos)

    return sorted_promos


def write_to_sheets(
    spreadsheet_id: str,
    all_promos: List[Dict],
    sheet_name: str = "Promotions"
) -> bool:
    """
    Write all promotions to Google Sheets with grouping, sorting, and formatting.

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

        # Group and sort promotions
        sorted_promos = group_and_sort_promos(cleaned_promos)

        # Prepare header row
        headers = COLUMN_ORDER

        # Prepare data rows (in column order)
        rows = []
        for promo in sorted_promos:
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

        logger.info(f"Wrote {len(rows)} rows to Google Sheets (grouped and sorted)")

        # Apply formatting
        apply_sheet_formatting(service, spreadsheet_id, sheet_name, len(all_rows), sorted_promos)

        return True

    except HttpError as e:
        logger.error(f"Google Sheets API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error writing to Google Sheets: {e}", exc_info=True)
        return False


def hex_to_rgb(hex_color: str) -> Dict[str, float]:
    """Convert hex color to RGB dict for Google Sheets API."""
    hex_color = hex_color.lstrip('#')
    return {
        'red': int(hex_color[0:2], 16) / 255.0,
        'green': int(hex_color[2:4], 16) / 255.0,
        'blue': int(hex_color[4:6], 16) / 255.0
    }


def get_company_background_color(business_name: str) -> Optional[str]:
    """Get company-specific background color for business_name column."""
    business_lower = business_name.lower()
    company_colors = {
        "fountain tire": "#E1F5FE",
        "good news auto": "#F3E5F5",
        "midas": "#FFF3E0",
        "kal tire": "#E0F2F1",
        "jiffy lube": "#FDE0DC",
        "speedy auto": "#FFFDE7",
        "trail tire": "#EDE7F6",
        "integra tire": "#E8F5E9",
        "valvoline": "#E0F7FA",
        "mr. lube": "#E3F2FD"
    }

    for company, color in company_colors.items():
        if company in business_lower:
            return color
    return None


def get_category_color(category: str) -> Optional[Dict[str, any]]:
    """Get category color formatting."""
    category_lower = category.lower()
    category_colors = {
        "tires": "#2196F3",
        "oil change": "#FF9800",
        "brakes": "#EF5350",
        "alignment": "#AB47BC",
        "general service": "#009688",
        "financing": "#795548"
    }

    for cat_key, color in category_colors.items():
        if cat_key in category_lower:
            return {
                'backgroundColor': hex_to_rgb(color),
                'textFormat': {
                    'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                    'bold': True
                }
            }
    return None


def get_status_color(status: str) -> Optional[Dict[str, any]]:
    """Get new_or_updated badge color."""
    status_upper = status.upper()
    status_colors = {
        "NEW": "#43A047",
        "UPDATED": "#FB8C00",
        "SAME": "#9E9E9E"
    }

    if status_upper in status_colors:
        return {
            'backgroundColor': hex_to_rgb(status_colors[status_upper]),
            'textFormat': {
                'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                'bold': True
            }
        }
    return None


def apply_sheet_formatting(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    num_rows: int,
    sorted_promos: List[Dict]
):
    """
    Apply colorful dashboard-style formatting with Times New Roman font.

    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        sheet_name: Sheet name
        num_rows: Number of data rows (including header)
        sorted_promos: List of sorted promo dicts (for row-specific formatting)
    """
    try:
        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if not sheet_id:
            logger.warning("Could not find sheet ID for formatting")
            return

        requests = []

        # Column indices
        business_name_col = COLUMN_ORDER.index("business_name")
        category_col = COLUMN_ORDER.index("category")
        new_or_updated_col = COLUMN_ORDER.index("new_or_updated")

        # 1. Format header row (bold, dark background, white text, Times New Roman, size 12)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': hex_to_rgb("#1F1F1F"),
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'fontSize': 12,
                            'bold': True,
                            'fontFamily': 'Times New Roman'
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
            }
        })

        # 2. Freeze header row
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })

        # 3. Format data rows with Times New Roman, alternating colors, and specific formatting
        for row_idx in range(1, num_rows):
            promo_idx = row_idx - 1
            if promo_idx < len(sorted_promos):
                promo = sorted_promos[promo_idx]
                business_name = promo.get("business_name", "")
                category = promo.get("category", "")
                new_or_updated = promo.get("new_or_updated", "NEW")
            else:
                business_name = ""
                category = ""
                new_or_updated = "NEW"

            # Base row formatting: Times New Roman, alternating colors, wrap text
            is_even = row_idx % 2 == 0
            base_bg = {'red': 0.956, 'green': 0.956, 'blue': 0.956} if is_even else {'red': 1.0, 'green': 1.0, 'blue': 1.0}

            # Format entire row with base style
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': row_idx,
                        'endRowIndex': row_idx + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': len(COLUMN_ORDER)
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': base_bg,
                            'textFormat': {
                                'foregroundColor': hex_to_rgb("#222222"),
                                'fontSize': 10,
                                'fontFamily': 'Times New Roman'
                            },
                            'horizontalAlignment': 'LEFT',
                            'verticalAlignment': 'MIDDLE',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
                }
            })

            # 4. Apply company background color to business_name column
            company_bg = get_company_background_color(business_name)
            if company_bg:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': business_name_col,
                            'endColumnIndex': business_name_col + 1
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': hex_to_rgb(company_bg)
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })

            # 5. Apply category color tag
            category_format = get_category_color(category)
            if category_format:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': category_col,
                            'endColumnIndex': category_col + 1
                        },
                        'cell': {
                            'userEnteredFormat': category_format
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                })

            # 6. Apply new_or_updated badge color
            status_format = get_status_color(new_or_updated)
            if status_format:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': row_idx,
                            'endRowIndex': row_idx + 1,
                            'startColumnIndex': new_or_updated_col,
                            'endColumnIndex': new_or_updated_col + 1
                        },
                        'cell': {
                            'userEnteredFormat': status_format
                        },
                        'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                    }
                })

        # 7. Auto-resize columns
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

        logger.info("Applied colorful dashboard formatting with Times New Roman to sheet")

    except Exception as e:
        logger.warning(f"Error applying formatting: {e}", exc_info=True)


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


def remove_rows_by_business_name(
    spreadsheet_id: str,
    business_name: str,
    sheet_name: str = "Promotions"
) -> bool:
    """
    Remove all rows for a specific business from Google Sheets.

    Args:
        spreadsheet_id: Google Sheets ID
        business_name: Business name to filter (case-insensitive)
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
            logger.error(f"Could not access sheet: {sheet_name}")
            return False

        # Read all existing data
        range_name = f"{sheet_name}!A1:Z1000"
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()

        rows = result.get('values', [])
        if not rows:
            logger.info(f"No data found in sheet to remove")
            return True

        # First row is header
        headers = rows[0] if rows else []
        if not headers:
            logger.warning("No header row found")
            return False

        # Find business_name column index
        business_name_col_idx = None
        for idx, header in enumerate(headers):
            if header.lower() == "business_name":
                business_name_col_idx = idx
                break

        if business_name_col_idx is None:
            logger.error("business_name column not found in sheet")
            return False

        # Filter out rows matching business_name (case-insensitive)
        filtered_rows = [headers]  # Keep header
        removed_count = 0

        for row in rows[1:]:  # Skip header
            if len(row) > business_name_col_idx:
                row_business_name = str(row[business_name_col_idx]).strip().lower()
                if row_business_name != business_name.lower():
                    # Pad row to match header length
                    while len(row) < len(headers):
                        row.append("")
                    filtered_rows.append(row)
                else:
                    removed_count += 1
            else:
                # Row too short, keep it
                while len(row) < len(headers):
                    row.append("")
                filtered_rows.append(row)

        # Write back filtered data
        body = {'values': filtered_rows}

        # Clear the sheet first
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1000"
            ).execute()
        except Exception as e:
            logger.warning(f"Could not clear sheet: {e}")

        # Write filtered data
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()

        logger.info(f"Removed {removed_count} rows for business '{business_name}'")

        # Re-read filtered data to get promo dicts for formatting
        filtered_promos = []
        if len(filtered_rows) > 1:  # Has header + data
            headers = filtered_rows[0]
            for row in filtered_rows[1:]:
                promo = {}
                for idx, header in enumerate(headers):
                    if idx < len(row):
                        promo[header] = row[idx]
                filtered_promos.append(promo)

        # Apply formatting (pass empty list if no promos to avoid errors)
        apply_sheet_formatting(service, spreadsheet_id, sheet_name, len(filtered_rows), filtered_promos)

        return True

    except HttpError as e:
        logger.error(f"Google Sheets API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error removing rows from Google Sheets: {e}", exc_info=True)
        return False


# Column order for Advertisements tab (exact order as specified)
ADS_COLUMN_ORDER = [
    "business_name",
    "ad_title",
    "ad_description",
    "discount_value",
    "coupon_code",
    "ad_link",
    "displayed_link",
    "date_scraped"
]


def format_ad_for_sheets(ad: Dict) -> Dict:
    """
    Format ad dictionary for Google Sheets.

    Args:
        ad: Ad dictionary from SerpAPI

    Returns:
        Formatted ad dictionary with required columns
    """
    from datetime import datetime
    import re

    # Extract ad fields
    title = ad.get("title", "")
    snippet = ad.get("snippet", "")
    description = ad.get("description", "")
    link = ad.get("link", "") or ad.get("url", "")
    displayed_link = ad.get("displayed_link", "")
    business_name = ad.get("business_name", "")

    # Clean text
    title = clean_text_for_sheets(title)
    snippet = clean_text_for_sheets(snippet)
    description = clean_text_for_sheets(description)

    # Combine title and description for extraction
    combined_text = f"{title} {snippet} {description}".lower()

    # Extract ad_title: use title OR first line of snippet
    ad_title = title
    if not ad_title and snippet:
        # Use first line of snippet
        first_line = snippet.split('\n')[0].split('.')[0]
        ad_title = first_line.strip()[:100]  # Limit length

    # Extract ad_description: full snippet or description
    ad_description = snippet or description

    # Extract discount_value from text
    discount_value = None
    # Pattern for dollar amounts: $10, $20 off, save $50
    dollar_pattern = r'\$(\d+(?:\.\d+)?)'
    dollar_matches = re.findall(dollar_pattern, combined_text, re.IGNORECASE)
    if dollar_matches:
        amounts = [float(m) for m in dollar_matches]
        max_amount = max(amounts)
        if max_amount == int(max_amount):
            discount_value = f"${int(max_amount)}"
        else:
            discount_value = f"${max_amount}"
    else:
        # Pattern for percentages: 15%, 20% off, save 25%
        percent_pattern = r'(\d+)%'
        percent_matches = re.findall(percent_pattern, combined_text, re.IGNORECASE)
        if percent_matches:
            percents = [int(p) for p in percent_matches]
            discount_value = f"{max(percents)}%"
        else:
            # Check for "free" or "complimentary"
            if re.search(r'\bfree\b', combined_text, re.IGNORECASE):
                discount_value = "Free"
            elif re.search(r'\bcomplimentary\b', combined_text, re.IGNORECASE):
                discount_value = "Complimentary"

    # Extract coupon_code (alphanumeric 3+ characters)
    coupon_code = None
    excluded_words = {
        "code", "coupon", "promo", "use", "enter", "apply", "mention", "mentioned",
        "available", "checkout", "discount", "save", "off", "special", "offer",
        "here", "for", "with", "the", "and", "or", "at", "on", "in"
    }

    # Pattern 1: "use code SAVE20" or "enter promo code WINTER25" (most specific first)
    # Handle "promo code" as a phrase - must come before other patterns
    use_code_pattern = r'(?:use|enter|apply)[:\s]+(?:promo[:\s]+)?(?:code|coupon)[:\s]+([A-Z0-9]{3,})(?:\s|$|[.,;!?])'
    matches = re.findall(use_code_pattern, combined_text, re.IGNORECASE)
    if matches:
        for match in matches:
            match_upper = match.upper()
            if len(match) >= 3 and match_upper not in excluded_words:
                coupon_code = match_upper
                break

    # Pattern 2: "code: SAVE20" or "coupon: PROMO2024" (with colon - more specific)
    # Only match if followed by colon (more specific)
    if not coupon_code:
        code_pattern = r'(?:code|coupon):\s+([A-Z0-9]{3,})(?:\s|$|[.,;!?])'
        matches = re.findall(code_pattern, combined_text, re.IGNORECASE)
        if matches:
            for match in matches:
                match_upper = match.upper()
                if len(match) >= 3 and match_upper not in excluded_words:
                    coupon_code = match_upper
                    break

    # Pattern 3: Standalone alphanumeric codes (2+ letters followed by 2+ digits)
    if not coupon_code:
        standalone_pattern = r'\b([A-Z]{2,}\d{2,})\b'
        matches = re.findall(standalone_pattern, combined_text, re.IGNORECASE)
        if matches:
            for match in matches:
                match_upper = match.upper()
                if len(match) >= 3 and match_upper not in excluded_words:
                    coupon_code = match_upper
                    break

    return {
        "business_name": business_name,
        "ad_title": ad_title or "",
        "ad_description": ad_description or "",
        "discount_value": discount_value or "",
        "coupon_code": coupon_code or "",
        "ad_link": link or "",
        "displayed_link": displayed_link or "",
        "date_scraped": datetime.now().strftime("%Y-%m-%d")
    }


def write_ads_to_sheets(
    spreadsheet_id: str,
    all_ads: List[Dict],
    sheet_name: str = "Advertisements"
) -> bool:
    """
    Write all ads to Google Sheets in the Advertisements tab.

    Args:
        spreadsheet_id: Google Sheets spreadsheet ID
        all_ads: List of ad dictionaries
        sheet_name: Name of the sheet tab (default: "Advertisements")

    Returns:
        True if successful, False otherwise
    """
    service = get_sheets_service()
    if not service:
        return False

    try:
        if not ensure_sheet_exists(service, spreadsheet_id, sheet_name):
            logger.error(f"Could not create or access sheet: {sheet_name}")
            return False

        # Format all ads
        formatted_ads = [format_ad_for_sheets(ad) for ad in all_ads]

        # Group by business_name for better organization
        from collections import defaultdict
        grouped_ads = defaultdict(list)
        for ad in formatted_ads:
            business_name = ad.get("business_name", "Unknown")
            grouped_ads[business_name].append(ad)

        # Sort by business name (ads are already limited to 2 per competitor)
        sorted_ads = []
        for business_name in sorted(grouped_ads.keys()):
            sorted_ads.extend(grouped_ads[business_name])

        # Build rows
        headers = ADS_COLUMN_ORDER
        rows = []
        for ad in sorted_ads:
            row = [ad.get(col, "") for col in ADS_COLUMN_ORDER]
            rows.append(row)

        all_rows = [headers] + rows

        # Clear existing data and write new data
        try:
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1000"
            ).execute()
        except Exception as e:
            logger.warning(f"Could not clear sheet (may be empty): {e}")

        body = {'values': all_rows}
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()

        logger.info(f"Wrote {len(rows)} ads to Google Sheets")

        # Apply formatting
        apply_ads_formatting(service, spreadsheet_id, sheet_name, len(all_rows))

        return True

    except HttpError as e:
        logger.error(f"Google Sheets API error: {e}")
        return False
    except Exception as e:
        logger.error(f"Error writing ads to Google Sheets: {e}", exc_info=True)
        return False


def apply_ads_formatting(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    num_rows: int
):
    """
    Apply formatting to the Advertisements tab.

    Args:
        service: Google Sheets API service
        spreadsheet_id: Spreadsheet ID
        sheet_name: Sheet name
        num_rows: Number of rows (including header)
    """
    try:
        sheet_id = get_sheet_id(service, spreadsheet_id, sheet_name)
        if not sheet_id:
            logger.warning("Could not find sheet ID for formatting")
            return

        requests = []

        # 1. Format header row (bold, dark background, white text, Times New Roman, size 12)
        requests.append({
            'repeatCell': {
                'range': {
                    'sheetId': sheet_id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': hex_to_rgb("#1F1F1F"),
                        'textFormat': {
                            'foregroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},
                            'fontSize': 12,
                            'bold': True,
                            'fontFamily': 'Times New Roman'
                        },
                        'horizontalAlignment': 'LEFT',
                        'verticalAlignment': 'MIDDLE',
                        'wrapStrategy': 'WRAP'
                    }
                },
                'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
            }
        })

        # 2. Freeze header row
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {
                        'frozenRowCount': 1
                    }
                },
                'fields': 'gridProperties.frozenRowCount'
            }
        })

        # 3. Format data rows with Times New Roman, alternating colors
        for row_idx in range(1, num_rows):
            is_even = row_idx % 2 == 0
            base_bg = {'red': 0.956, 'green': 0.956, 'blue': 0.956} if is_even else {'red': 1.0, 'green': 1.0, 'blue': 1.0}

            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': row_idx,
                        'endRowIndex': row_idx + 1,
                        'startColumnIndex': 0,
                        'endColumnIndex': len(ADS_COLUMN_ORDER)
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': base_bg,
                            'textFormat': {
                                'foregroundColor': hex_to_rgb("#222222"),
                                'fontSize': 10,
                                'fontFamily': 'Times New Roman'
                            },
                            'horizontalAlignment': 'LEFT',
                            'verticalAlignment': 'MIDDLE',
                            'wrapStrategy': 'WRAP'
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)'
                }
            })

        # 4. Auto-resize columns
        requests.append({
            'autoResizeDimensions': {
                'dimensions': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': 0,
                    'endIndex': len(ADS_COLUMN_ORDER)
                }
            }
        })

        # Apply all formatting requests
        body = {'requests': requests}
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()

        logger.info("Applied formatting to Advertisements tab")

    except Exception as e:
        logger.warning(f"Error applying ads formatting: {e}")

