# Competitor Intelligence Dashboard

Automated web scraping system for competitor promotion tracking with Google Sheets integration.

## 🚀 Features

- **Multi-competitor scraping** - Tracks 10+ auto service competitors
- **Firecrawl + OCR extraction pipeline** - Advanced text and image extraction
- **AI Overview fallback** - SerpAPI integration for missing promotions
- **Google Sheets dashboard** - Automatic merging and formatting
- **Smart deduplication** - Fuzzy matching across competitors
- **NEW/UPDATED/SAME tracking** - Change detection system
- **Unified extraction flow** - Firecrawl first, AI Overview fallback

## 📋 Competitors Tracked

1. Good News Auto
2. Midas
3. Kal Tire
4. Jiffy Lube
5. Fountain Tire
6. Speedy Auto Service
7. Trail Tire
8. Integra Tire
9. Valvoline Express Care
10. Mr. Lube (AI Overview primary)

## 🛠️ Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the project root:

```bash
# API Keys
FIRECRAWL_API_KEY=your_firecrawl_key
SERPAPI_KEY=your_serpapi_key
PERPLEXITY_API_KEY=your_perplexity_key
ZENROWS_API_KEY=your_zenrows_key
SCRAPERAPI_KEY=your_scraperapi_key

# Google Sheets
GOOGLE_SHEETS_ID=your_sheet_id
GOOGLE_SHEETS_CREDENTIALS_PATH=service_account.json

# Optional Settings
PLAYWRIGHT_MODE=headless
MAX_CONCURRENCY=3
PROMO_TIMEOUT=30
```

### 3. Google Cloud Credentials

Place `service_account.json` in the project root with Google Sheets API access.

## 🎯 Usage

### Run All Competitors

```bash
python run_all_competitors.py
```

This will:
1. Scrape all 10 competitors sequentially
2. Extract promotions using Firecrawl + OCR
3. Fallback to AI Overview if needed
4. Merge and deduplicate all promotions
5. Write to Google Sheets with dashboard formatting

### Run Individual Competitor

```bash
python run_goodnews.py
python run_speedy.py
python run_midas.py
# ... etc
```

## 📊 Output Format

Results are automatically:
- Merged across all competitors
- Deduplicated using fuzzy matching
- Formatted for Google Sheets dashboard
- Tagged with NEW/UPDATED/SAME status

### Google Sheets Columns

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

## 🔄 Extraction Flow

1. **Primary**: Firecrawl HTML/text extraction
2. **Secondary**: Google Cloud Vision OCR (images, PDFs)
3. **Fallback**: Tesseract OCR (if Vision fails)
4. **Final Fallback**: SerpAPI AI Overview

**Special Case**: Mr. Lube always uses AI Overview as primary method.

## 📁 Project Structure

```
├── app/
│   ├── config/          # Configuration and competitor list
│   ├── extractors/     # Firecrawl, OCR, SerpAPI extractors
│   ├── scrapers/        # Individual competitor scrapers
│   └── utils/           # Sheets writer, merger, promo builder
├── data/
│   ├── images/          # Downloaded promo images
│   └── promotions/      # JSON output files
├── logs/                # Scraper logs
└── run_*.py            # Individual scraper runners
```

## 🔒 Security

Sensitive files are excluded via `.gitignore`:
- `.env` (API keys)
- `service_account.json` (Google credentials)
- `data/` (scraped data)
- `logs/` (log files)

## 📝 Requirements

- Python 3.10+
- Google Cloud Vision API access
- Service account with Google Sheets API permissions
- API keys for Firecrawl, SerpAPI, Perplexity

## 🐛 Troubleshooting

### Google Sheets not updating
- Verify `GOOGLE_SHEETS_ID` in `.env`
- Check `service_account.json` has Sheets API access
- Ensure service account has edit permissions on the sheet

### OCR failures
- Verify Google Cloud Vision API is enabled
- Check `service_account.json` has Vision API permissions
- Tesseract will be used as fallback

### No promotions found
- System automatically falls back to AI Overview
- Check API keys are valid
- Review logs in `logs/` directory

## 📄 License

[Your License Here]

## 👤 Author

[Your Name/Organization]

