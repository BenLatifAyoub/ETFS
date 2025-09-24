#!/usr/bin/env python3
"""
Unified AI-Enhanced ETF Scraper (With Detailed Logging)
----------------------------------------------------------------------
This script combines and fixes the issues in the provided codes to scrape two ETFs from each of four providers: Vanguard, iShares, DWS Xtrackers, and Amundi.
Enhanced with detailed terminal logging to track each step and diagnose issues.
Fixes include:
- Adding .first to locators to avoid strict mode violations.
- Refining URL getters to ensure valid product URLs.
- Improving AI prompt to strictly avoid non-holdings links.
- Adjusting HTML preparation for better AI analysis.
- Site-specific tweaks for reliable extraction.
"""
import asyncio
import json
import os
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import traceback
import google.generativeai as genai
from urllib.parse import urljoin, urlparse
# --- 1. GLOBAL CONFIGURATION ---
TOTAL_ETFS_PER_PROVIDER = 2
BATCH_SIZE = 1
DELAY_BETWEEN_BATCHES_SECONDS = 61
DOWNLOAD_TIMEOUT_MS = 60000
PAGE_TIMEOUT_MS = 90000
OUTPUT_DIRECTORY = "etf_data"
DOWNLOAD_DIRECTORY = "downloads"
GEMINI_MODEL = "gemini-2.0-flash-lite"
def log_step(message):
    """Helper function to print timestamped log messages."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
try:
    log_step("🔧 Initializing Gemini API...")
    genai.configure(api_key="put_your_api_key_here")
    model = genai.GenerativeModel(GEMINI_MODEL)
    log_step("✅ Gemini API initialized successfully.")
except (ValueError, TypeError) as e:
    log_step(f"❌ API Key Error: {e}. Please ensure you have set your Gemini API key.")
    exit()
# --- 2. UNIVERSAL AI PROMPT (Refined to strictly avoid wrong links) ---
UNIVERSAL_AI_PROMPT = """
You are an expert web scraping AI assistant. Analyze the provided HTML from an ETF product page to find the portfolio holdings.
Your response MUST be in a strict JSON format with no additional text, code fences, or markdown.
And make sure the css selectorer is correct to avoid ambiguity.
**Priority 1: Find Holdings Download Link (CSV/XLS/XLSX)**
- Look for an `<a>` tag to download the complete holdings list.
- Search for specific keywords like "Download Holdings", "Alle Positionen herunterladen", "Komplette Wertpapierliste", "Portfolio herunterladen","
Komplette Wertpapierliste herunterladen", "CSV", "XLS", "Fondspositionen und Kennzahlen", "KOMPONENTEN DES ETFS HERUNTERLADEN".
- The link should be specifically for holdings, often with class 'icon-xls-export' or href containing '/excel/' or 'download' or with class 'm-download-button' or 'download-link'. If found, provide the best CSS selector, e.g., 'a:has-text("KOMPONENTEN DES ETFS HERUNTERLADEN")' or more specific if possible. Prefer download as it's the full list.
- **STRICTLY AVOID** any links related to "Prospectus", "KIID", "Report", "View prospectus and reports", "Berichte", "Factsheet", or anything not explicitly for holdings/portfolio composition.
- If a valid download link is found, provide the CSS selector for the <a> tag, e.g., 'a.icon-xls-export:has-text("Fondspositionen")' or 'a[href*="/excel/index/constituent/"]' or 'a.d-base-link.d-base-link--font-size-16.d-base-text--font-dws-slab-bold.d-base-link--icon.pdp-section__content-textblock'.
Common Amundi selectors to try:
   - 'a:has-text("KOMPONENTEN DES ETFS HERUNTERLADEN")'
   - 'button:has-text("Download")'
   - 'a[class*="download"]'
   - '.download-link'
- Respond with: {{"action": "download", "selector": "<css-selector>"}}
**Priority 2: Extract from an HTML Table**
- If no download link exists, find the HTML table displaying holdings (e.g., 'Top-Positionen', 'All Holdings', 'Wertpapiere des Wertpapierkorbs').
- Extract as many holdings as possible. For each, get: name (required), weight (required, as float), isin, sector, securityType, country, currency. Use 'N/A' for missing.
- Respond with: {{"action": "extract", "holdings": [<list of dicts>]}}
**Priority 3: No Data Found**
- If neither, respond with: {{"action": "none"}}
HTML content:
{html_to_analyze}
"""
# --- 3. SITE-SPECIFIC HELPER FUNCTIONS ---
# Consent Handlers
async def handle_vanguard_consent(page):
    log_step(" - Handling Vanguard consent...")
    try:
        await page.locator('button#onetrust-accept-btn-handler').click(timeout=7000)
        log_step(" - ✅ Vanguard: Cookies accepted.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ Vanguard: No cookie banner found.")
    log_step(" - Consent handling complete.")
async def handle_ishares_consent(page):
    log_step(" - Handling iShares consent...")
    try:
        await page.locator('#onetrust-accept-btn-handler').click(timeout=7000)
        log_step(" - ✅ iShares: Cookies accepted.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ iShares: No cookie banner found.")
    try:
        await page.locator('a[data-link-event="Accept t&c: individual"]:has-text("Weiter")').click(timeout=7000)
        log_step(" - ✅ iShares: Clicked 'Weiter'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ iShares: No 'Weiter' link found.")
    log_step(" - Consent handling complete.")
async def handle_dws_consent(page):
    log_step(" - Handling DWS consent...")
    try:
        await page.locator('button:has-text("Accept all cookies")').click(timeout=7000)
        log_step(" - ✅ DWS: Clicked 'Accept all cookies'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ DWS: Cookie banner not found.")
    try:
        await page.locator('button:has-text("Akzeptieren & weiter")').click(timeout=7000)
        log_step(" - ✅ DWS: Clicked 'Akzeptieren & weiter'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ DWS: Disclaimer pop-up not found.")
    log_step(" - Consent handling complete.")
async def handle_amundi_consent(page):
    log_step(" - Handling Amundi consent...")
    try:
        await page.locator('button[data-profile="INSTIT"]').click(timeout=10000)
        log_step(" - ✅ Amundi: Clicked 'Professioneller Anleger'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ Amundi: Profile selection pop-up not found.")
    try:
        await page.locator('button:has-text("Akzeptieren und fortfahren")').click(timeout=10000)
        log_step(" - ✅ Amundi: Clicked 'Akzeptieren und fortfahren'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ Amundi: Final acceptance pop-up not found.")
    try:
        await page.locator('button:has-text("Alle annehmen")').click(timeout=7000)
        log_step(" - ✅ Amundi: Clicked 'Alle annehmen'.")
    except PlaywrightTimeoutError:
        log_step(" - ⚠️ Amundi: Cookie banner not found.")
    log_step(" - Consent handling complete.")
# URL Getters
async def get_vanguard_urls(page, base_url):
    log_step(" - Collecting Vanguard URLs...")
    urls = set()
    try:
        await page.goto(base_url, wait_until="domcontentloaded")
        log_step(" - ✅ Navigated to Vanguard ETF list page.")
        await handle_vanguard_consent(page)
        log_step(" - Waiting for ETF list to load...")
        await page.wait_for_selector("tr[data-rpa-tag-id]", timeout=60000)
        log_step(" - ✅ ETF list loaded.")
        while await page.locator('button:has-text("Show more")').is_visible():
            log_step(" - Clicking 'Show more' button...")
            await page.locator('button:has-text("Show more")').click()
            await page.wait_for_load_state('networkidle', timeout=15000)
            log_step(" - ✅ Loaded more ETFs.")
        for link in await page.locator("tr[data-rpa-tag-id] a[data-rpa-tag-id='longName']").all():
            if href := await link.get_attribute("href"):
                urls.add(urljoin(base_url, href))
        log_step(f" - ✅ Collected {len(urls)} Vanguard URLs.")
        return list(urls)
    except Exception as e:
        log_step(f" - ❌ Error collecting Vanguard URLs: {e}")
        return []
async def get_ishares_urls(page, base_url):
    log_step(" - Collecting iShares URLs...")
    urls = set()
    try:
        await page.goto(base_url, wait_until="domcontentloaded")
        log_step(" - ✅ Navigated to iShares ETF list page.")
        await handle_ishares_consent(page)
        log_step(" - Waiting for ETF list to load...")
        await page.wait_for_selector("a.link-to-product-page", timeout=60000)
        log_step(" - ✅ ETF list loaded.")
        for link in await page.locator("a.link-to-product-page").all():
            if href := await link.get_attribute("href"):
                clean_href = href.split('?')[0]
                urls.add(f"https://www.ishares.com{clean_href}?switchLocale=y&siteEntryPassthrough=true")
        log_step(f" - ✅ Collected {len(urls)} iShares URLs.")
        return list(urls)
    except Exception as e:
        log_step(f" - ❌ Error collecting iShares URLs: {e}")
        return []
async def get_dws_urls(page, base_url):
    log_step(" - Collecting DWS URLs...")
    urls = set()
    try:
        await page.goto(base_url, wait_until="domcontentloaded")
        log_step(" - ✅ Navigated to DWS ETF list page.")
        await handle_dws_consent(page)
        selector = 'td a.d-base-link[href*="/de-de/LU"]'
        log_step(" - Waiting for ETF list to load...")
        await page.wait_for_selector(selector, timeout=60000)
        log_step(" - ✅ ETF list loaded.")
        for link in await page.locator(selector).all():
            if href := await link.get_attribute("href"):
                urls.add(urljoin(base_url, href))
        log_step(f" - ✅ Collected {len(urls)} DWS URLs.")
        return list(urls)
    except Exception as e:
        log_step(f" - ❌ Error collecting DWS URLs: {e}")
        return []
async def get_amundi_urls(page, base_url):
    """Navigate Amundi ETF search page and collect product page URLs."""
    etf_urls = []
    try:
        await page.goto(base_url, timeout=90000)
        await handle_amundi_consent(page)
        await page.wait_for_load_state('networkidle', timeout=15000)  # ensures table loaded


        log_step("⏳ Waiting for ETF list to load...")
        await page.wait_for_selector("div.FinderResultsSection__Datatable table tbody tr", timeout=60000)
        log_step("✅ ETF table loaded.")

        links = await page.locator("div.FinderResultsSection__Datatable table tbody tr td a").all()
        for link in links[:TOTAL_ETFS_PER_PROVIDER]:
            href = await link.get_attribute("href")
            if href:
                full_url = urljoin("https://www.amundietf.de", href)
                etf_urls.append(full_url)

        log_step(f"✅ Collected {len(etf_urls)} Amundi ETF URLs.")
        return etf_urls

    except Exception as e:
        log_step(f"❌ Error collecting Amundi URLs: {e}")
        return []


# Metadata Extractors
async def extract_vanguard_metadata(page):
    log_step(" - Extracting Vanguard metadata...")
    try:
        name_selector = "span[data-rpa-tag-id='longName']"
        h1_selector = "h1[data-rpa-tag-id='dashboard-symbol']"
        await page.wait_for_selector(h1_selector, timeout=20000)
        name = (await page.locator(name_selector).first.inner_text()).strip()
        h1_text = (await page.locator(h1_selector).first.inner_text()).strip()
        ticker = h1_text.split(' ')[0]
        log_step(f" - ✅ Vanguard metadata extracted: Name={name}, ISIN={ticker}")
        return {"name": name, "isin": ticker}
    except Exception as e:
        log_step(f" - ❌ Error extracting Vanguard metadata: {e}")
        return {"name": "N/A", "isin": "N/A"}
async def extract_ishares_metadata(page):
    log_step(" - Extracting iShares metadata...")
    try:
        name_selector = "#fundHeader > header.main-header span.product-title-main"
        await page.wait_for_selector(name_selector, timeout=20000)
        name = await page.locator(name_selector).first.inner_text()
        isin = await page.locator("div.col-isin div.data").first.inner_text()
        log_step(f" - ✅ iShares metadata extracted: Name={name.strip()}, ISIN={isin.strip()}")
        return {"name": name.strip(), "isin": isin.strip()}
    except Exception as e:
        log_step(f" - ❌ Error extracting iShares metadata: {e}")
        return {"name": "N/A", "isin": "N/A"}
async def extract_dws_metadata(page):
    log_step(" - Extracting DWS metadata...")
    try:
        name_selector = "h1#product-header-title"
        await page.wait_for_selector(name_selector, timeout=20000)
        name = (await page.locator(name_selector).first.inner_text()).strip()
        isin = await page.locator("div.product-header__identifier__row:has-text('ISIN:') strong").first.inner_text()
        log_step(f" - ✅ DWS metadata extracted: Name={name}, ISIN={isin.strip()}")
        return {"name": name, "isin": isin.strip()}
    except Exception as e:
        log_step(f" - ❌ Error extracting DWS metadata: {e}")
        return {"name": "N/A", "isin": "N/A"}
async def extract_amundi_metadata(page):
    log_step("Extracting Amundi metadata...")
    try:
        # Wait for page to fully load
        await page.wait_for_load_state('networkidle', timeout=10000)
        
        # Extract name with fallback selectors
        name_selectors = ["h1.ProductHero__title", "h1.text-uppercase", "h1"]
        name = "N/A"
        
        for selector in name_selectors:
            try:
                name_element = page.locator(selector).first
                if await name_element.is_visible():
                    name = (await name_element.inner_text()).strip()
                    if name and name != "N/A" and "---" not in name:
                        break
            except:
                continue
        
        # Extract ISIN
        isin = "N/A"
        try:
            isin_container = page.locator("div:has(> span:has-text('ISIN / WKN'))").first
            if await isin_container.is_visible():
                isin_text_content = await isin_container.locator("div.m-isin-wkn").inner_text()
                isin = isin_text_content.split('/')[0].strip()
        except:
            # Fallback: look for ISIN pattern in page
            try:
                isin_elements = await page.locator("text=/LU\\d{10}/").all()
                if isin_elements:
                    isin = await isin_elements[0].inner_text()
            except:
                pass

        log_step(f"Amundi metadata extracted: Name={name}, ISIN={isin}")
        return {"name": name, "isin": isin}
    except Exception as e:
        log_step(f"Error extracting Amundi metadata: {e}")
        return {"name": "N/A", "isin": "N/A"}


# HTML Preparers
def prepare_html_full(soup):
    log_step(" - Preparing HTML (full body)...")
    result = str(soup.body)
    log_step(f" - ✅ HTML prepared ({len(result)} chars).")
    return result
def prepare_html_ishares(soup):
    log_step(" - Preparing iShares HTML (holdings section)...")
    holdings_section = soup.select_one("div#holdings") or soup.select_one("div#allHoldings") or soup
    result = str(holdings_section)
    log_step(f" - ✅ iShares HTML prepared ({len(result)} chars).")
    return result
def prepare_html_dws(soup):
    log_step(" - Preparing DWS HTML (top divs)...")
    main = soup.select_one("main")
    if main:
        child_divs = main.find_all('div', recursive=False)
        sorted_divs = sorted(child_divs, key=lambda d: len(str(d)), reverse=True)[2:4]
        result = "".join(str(d) for d in sorted_divs)
        log_step(f" - ✅ DWS HTML prepared ({len(result)} chars).")
        return result
    result = str(soup.body)
    log_step(f" - ⚠️ DWS: No main tag found, using full body ({len(result)} chars).")
    return result


def prepare_html_amundi(soup):
    log_step(" - Preparing Amundi HTML (full body)...")
    result = str(soup.body)
    log_step(f" - ✅ Amundi HTML prepared ({len(result)} chars).")
    return result
# File Processors
def _process_dataframe(df):
    log_step(" - Processing dataframe...")
    try:
        df.columns = [str(col).strip().lower() for col in df.columns]
        col_map = {
            'name': 'name', 'bezeichnung': 'name',
            'weight': 'weight', 'gewichtung (%)': 'weight',
            'isin': 'isin', 'sector': 'sector',
            'securityType': 'securityType', 'country': 'country',
            'currency': 'currency'
        }
        df.rename(columns=lambda c: next((new for old, new in col_map.items() if old in c.lower()), c), inplace=True)
        required = ['name', 'weight']
        if all(col in df.columns for col in required):
            df['weight'] = pd.to_numeric(df['weight'].astype(str).str.replace(',', '.').str.replace('%', ''), errors='coerce')
            df.dropna(subset=required, inplace=True)
            for col in col_map.values():
                if col not in df.columns:
                    df[col] = 'N/A'
            result = df[list(set(col_map.values()))].to_dict('records')
            log_step(f" - ✅ Dataframe processed: {len(result)} holdings extracted.")
            return result
        log_step(f" - ❌ Required columns {required} not found in dataframe. Columns: {list(df.columns)}")
        return []
    except Exception as e:
        log_step(f" - ❌ Error processing dataframe: {e}")
        return []
def process_generic_download(file_path):
    log_step(f" - Processing generic download: {file_path}")
    try:
        if file_path.endswith('.csv'):
            log_step(" - Reading CSV file...")
            df = pd.read_csv(file_path, skiprows=lambda x: x < next(i for i, line in enumerate(open(file_path)) if 'Name' in line or 'Bezeichnung' in line))
        else:
            log_step(" - Reading Excel file...")
            df = pd.read_excel(file_path, skiprows=lambda x: x < next(i for i, row in pd.read_excel(file_path, header=None).iterrows() if 'Name' in ' '.join(map(str, row)) or 'Bezeichnung' in ' '.join(map(str, row))))
        result = _process_dataframe(df)
        log_step(f" - ✅ Generic file processed: {len(result)} holdings.")
        return result
    except Exception as e:
        log_step(f" - ❌ Error processing generic file {file_path}: {e}")
        return []
def process_ishares_download(file_path, html_content=None):
    """
    Process an iShares CSV (or Excel/HTML fallback) file into holdings,
    replicating the scraper logic exactly.
    Optionally enrich with ISINs from HTML.
    """
    log_step(f" - Processing iShares download: {file_path}")

    try:
        # --- Detect CSV header start row ---
        start_row = 0
        if file_path.endswith(".csv"):
            with open(file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                for i, line in enumerate(f):
                    if "Emittententicker" in line or "Ticker" in line:
                        start_row = i
                        break
            df = pd.read_csv(file_path, skiprows=start_row, encoding="utf-8-sig", sep=",")
        elif file_path.endswith((".xls", ".xlsx")):
            log_step(" - Reading Excel file...")
            df = pd.read_excel(file_path)
        else:
            log_step(" - Attempting to read HTML table...")
            df = pd.read_html(file_path)[0]

        # --- Normalize columns ---
        column_mapping = {
            "Name": "name",
            "Sektor": "sector",
            "Anlageklasse": "securityType",
            "Gewichtung (%)": "weight",
        }
        df.rename(columns=column_mapping, inplace=True)

        required_cols = ["name", "sector", "securityType", "weight"]
        if not all(col in df.columns for col in required_cols):
            log_step(" - ❌ Missing expected columns, returning empty result.")
            return []

        # --- Clean data ---
        df["weight"] = pd.to_numeric(
            df["weight"].astype(str).str.replace(",", "."), errors="coerce"
        )
        df.dropna(subset=["weight", "name"], inplace=True)
        df.fillna({"sector": "N/A", "securityType": "N/A"}, inplace=True)

        holdings = df[required_cols].to_dict("records")

        # --- Optional ISIN enrichment from HTML ---
        if html_content:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html_content, "html.parser")
            isin_map = {}
            table = soup.select_one("table#allHoldingsTable")
            if table:
                for row in table.select("tbody tr"):
                    name_cell = row.select_one("td.colIssueName")
                    isin_cell = row.select_one("td.colIsin")
                    if name_cell and isin_cell:
                        isin_map[name_cell.get_text(strip=True)] = isin_cell.get_text(strip=True)

            for holding in holdings:
                holding["isin"] = isin_map.get(holding["name"], "N/A")
        else:
            for holding in holdings:
                holding["isin"] = "N/A"

        log_step(f" - ✅ Processed iShares file: {len(holdings)} holdings.")
        return holdings

    except Exception as e:
        log_step(f" - ❌ Error processing iShares file {file_path}: {e}")
        return []

def process_amundi_download(file_path):
    log_step(f"Processing Amundi download: {file_path}")
    try:
        df = None
        # Using calamine as it's modern and fast for .xlsx
        try:
            # Directly read the Excel file specifying that the header is on row 22 (index 21)
            df = pd.read_excel(file_path, header=21, engine='calamine')
            log_step("Successfully read file with calamine engine, using row 22 as header.")
        except Exception as e:
            log_step(f"Failed to read Excel file {file_path}: {e}")
            return []

        # Clean column names by removing leading/trailing spaces
        df.columns = [str(col).strip() for col in df.columns]

        column_mapping = {
            'Name': 'name', 'Bezeichnung': 'name', 'Wertpapierbezeichnung': 'name',
            'Währung': 'currency', 'Currency': 'currency',
            'Gewichtung': 'weight', 'Weight': 'weight', 'Gewichtung (%)': 'weight',
            'Sektor': 'sector', 'Sector': 'sector', 'Branche': 'sector',
            'Anlageklasse': 'securityType', 'Asset Class': 'securityType',
            'Land': 'country', 'Country': 'country',
            'ISIN': 'isin'
        }
        
        # Rename columns based on the mapping, converting to lowercase for robust matching
        df.rename(columns=lambda c: next((new for old, new in column_mapping.items() if old.lower() == c.lower()), c.lower()), inplace=True)

        # Check if essential columns exist after renaming
        if 'name' not in df.columns or 'weight' not in df.columns:
            available_cols = list(df.columns)
            log_step(f"Required columns 'name' or 'weight' not found. Available: {available_cols}")
            return []

        # Convert weight column to a numeric type
        df['weight'] = pd.to_numeric(
            df['weight'].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False).str.strip(),
            errors='coerce'
        )

        # Drop any rows where the essential 'name' or 'weight' fields are empty
        df.dropna(subset=['weight', 'name'], inplace=True)
        
        # Ensure all columns from our mapping exist in the dataframe. If not, add them with a default value.
        for col in column_mapping.values():
            if col not in df.columns:
                df[col] = 'N/A'
        
        # --- THIS IS THE ADDED LINE ---
        # Replace all remaining NaN (empty) cells with the string 'N/A'
        df.fillna('N/A', inplace=True)
        
        # Select and order the columns for the final output
        final_columns = list(column_mapping.values())
        holdings = df[final_columns].to_dict('records')
        
        log_step(f"Successfully processed {len(holdings)} holdings.")
        return holdings

    except Exception as e:
        log_step(f"An unexpected error occurred in process_amundi_download for {file_path}: {e}")
        traceback.print_exc()
        return []
# --- 4. CORE SCRAPER FUNCTION ---
async def scrape_etf_page(browser, url, site_config):
    log_step(f"📌 Starting scrape for URL: {url}")
    if not urlparse(url).scheme:
        log_step(f" - ❌ Invalid URL: {url}")
        return None
    context = await browser.new_context(accept_downloads=True)
    page = await context.new_page()
    try:
        log_step(" - Navigating to ETF page...")
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        log_step(" - ✅ Page loaded.")
        await site_config["consent_handler"](page)
        log_step(" - Extracting metadata...")
        metadata = await site_config["metadata_extractor"](page)
        etf_name, etf_isin = metadata["name"], metadata["isin"]
        log_step(f" - 📄 Found ETF: {etf_name} ({etf_isin})")
        log_step(" - Attempting to click holdings tab...")
        try:
            tab = page.locator('a[href="#holdings"], a:has-text("Zusammensetzung"), li[data-tab="holdings"]')
            await tab.first.click(timeout=10000)
            await page.wait_for_load_state('networkidle')
            log_step(" - ✅ Holdings tab clicked.")
        except Exception as e:
            log_step(f" - ⚠️ No holdings tab found or already loaded")
        log_step(" - Fetching page content...")
        html_content = await page.content()
        log_step(" - Preparing HTML for AI analysis...")
        soup = BeautifulSoup(html_content, "html.parser")
        html_to_analyze = site_config["html_preparer"](soup)
        log_step(" - Sending HTML to AI for analysis...")
        prompt = UNIVERSAL_AI_PROMPT.format(html_to_analyze=html_to_analyze)
        response = model.generate_content(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        try:
            data = json.loads(response_text)
            log_step(" - ✅ AI response parsed successfully.")
        except json.JSONDecodeError as e:
            log_step(f" - ❌ Error parsing AI response: {e}\n - Raw response: {response_text}")
            return None
        holdings = []
        if data['action'] == 'download':
            selector = data['selector']
            log_step(f" - ⏳ Downloading holdings via selector: {selector}")
            try:
                async with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                    await page.locator(selector).first.click(force=True)
                download = await download_info.value
                path = os.path.join(DOWNLOAD_DIRECTORY, download.suggested_filename)
                log_step(f" - Saving download to: {path}")
                await download.save_as(path)
                log_step(" - Processing downloaded file...")
                holdings = site_config["file_processor"](path)
                log_step(f" - ✅ Processed {len(holdings)} holdings from file.")
                os.remove(path)
                log_step(f" - ✅ Cleaned up file: {path}")
            except Exception as e:
                log_step(f"Download failed: {e}")
                # Try to extract from HTML as fallback
                log_step("Attempting HTML extraction as fallback...")
                try:
                    # Look for holdings table in the current page HTML
                    holdings_table = soup.select_one("table")
                    if holdings_table:
                        rows = holdings_table.select("tbody tr")[:20]  # Limit to top 20 holdings
                        for row in rows:
                            cells = row.select("td")
                            if len(cells) >= 2:
                                try:
                                    name = cells[0].get_text(strip=True)
                                    # Weight is often in the last column
                                    weight_text = cells[-1].get_text(strip=True)
                                    # Clean weight text
                                    weight_clean = weight_text.replace('%', '').replace(',', '.').strip()
                                    if weight_clean and weight_clean.replace('.', '').isdigit():
                                        weight = float(weight_clean)
                                        holdings.append({
                                            "name": name,
                                            "weight": weight,
                                            "sector": "N/A",
                                            "securityType": "N/A", 
                                            "isin": "N/A",
                                            "currency": "N/A",
                                            "country": "N/A"
                                        })
                                except (ValueError, AttributeError):
                                    continue
                        log_step(f"Extracted {len(holdings)} holdings from HTML fallback.")
                    else:
                        log_step("No holdings table found in HTML.")
                except Exception as fallback_error:
                    log_step(f"HTML fallback also failed: {fallback_error}")
        elif data['action'] == 'extract':
            holdings = data.get('holdings', [])
            log_step(f" - ✅ AI extracted {len(holdings)} holdings from HTML.")
        else:
            log_step(" - ⚠️ AI found no holdings data.")
       
        log_step(f" - ✅ Scrape complete for {etf_name}: {len(holdings)} holdings extracted.")
        return {"isin": etf_isin, "name": etf_name, "holdings": holdings}
    except Exception as e:
        log_step(f" - ❌ Error scraping {url}: {e}")
        traceback.print_exc()
        return None
    finally:
        log_step(" - Closing browser context...")
        await context.close()
        log_step(" - ✅ Context closed.")
# --- 5. MAIN ORCHESTRATION ---
async def main():
    log_step("🚀 Starting ETF Scraper...")
    log_step(f"📁 Creating directories: {OUTPUT_DIRECTORY}, {DOWNLOAD_DIRECTORY}")
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)
    SITE_CONFIGS = [
        {"name": "Vanguard", "start_url": "https://investor.vanguard.com/investment-products/list/etfs", "url_getter": get_vanguard_urls, "consent_handler": handle_vanguard_consent, "metadata_extractor": extract_vanguard_metadata, "html_preparer": prepare_html_full, "file_processor": process_generic_download},
        {"name": "iShares", "start_url": "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments#/?productView=all&pageNumber=1&sortColumn=totalFundSizeInMillions&sortDirection=desc&dataView=keyFacts&keyFacts=all", "url_getter": get_ishares_urls, "consent_handler": handle_ishares_consent, "metadata_extractor": extract_ishares_metadata, "html_preparer": prepare_html_ishares, "file_processor": process_ishares_download},
        {"name": "DWS Xtrackers", "start_url": "https://etf.dws.com/de-de/produktfinder/", "url_getter": get_dws_urls, "consent_handler": handle_dws_consent, "metadata_extractor": extract_dws_metadata, "html_preparer": prepare_html_dws, "file_processor": process_generic_download},
        {"name": "Amundi", "start_url": "https://www.amundietf.de/de/professionell/etf-products/search", "url_getter": get_amundi_urls, "consent_handler": handle_amundi_consent, "metadata_extractor": extract_amundi_metadata, "html_preparer": prepare_html_amundi, "file_processor": process_amundi_download}
    ]
    combined_etf_data = {}
    async with async_playwright() as p:
        log_step("🌐 Launching browser...")
        browser = await p.chromium.launch(headless=False)
        log_step("✅ Browser launched.")
        for config in SITE_CONFIGS:
            site_etf_data = {}
            log_step(f"\n{'='*20} STARTING SCRAPE FOR: {config['name'].upper()} {'='*20}")
            log_step(f" - Creating new page for {config['name']}...")
            page = await browser.new_page()
            log_step(f" - Collecting URLs from {config['start_url']}...")
            urls = await config["url_getter"](page, config["start_url"])
            urls_to_scrape = urls[:TOTAL_ETFS_PER_PROVIDER]
            log_step(f" - ✅ Collected {len(urls_to_scrape)} URLs for {config['name']}.")
            await page.close()
            log_step(" - ✅ Page closed.")
           
            for i, url in enumerate(urls_to_scrape):
                log_step(f" - Processing ETF {i+1}/{len(urls_to_scrape)}: {url}")
                result = await scrape_etf_page(browser, url, config)
                if result:
                    site_etf_data[result["isin"]] = result
                    log_step(f" - ✅ Added ETF {result['name']} ({result['isin']}) with {len(result['holdings'])} holdings.")
                else:
                    log_step(f" - ⚠️ No data scraped for {url}.")
                if (i + 1) % BATCH_SIZE == 0 and i + 1 < len(urls_to_scrape):
                    log_step(f" - ⏳ Waiting {DELAY_BETWEEN_BATCHES_SECONDS} seconds before next batch...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)
           
            if site_etf_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                save_path = os.path.join(OUTPUT_DIRECTORY, f"{config['name'].lower().replace(' ', '_')}_data_{timestamp}.json")
                log_step(f" - Saving {len(site_etf_data)} ETFs to {save_path}...")
                with open(save_path, "w", encoding="utf-8") as f:
                    json.dump(site_etf_data, f, indent=4)
                log_step(f" - ✅ Saved to {save_path}.")
                combined_etf_data.update(site_etf_data)
            else:
                log_step(f" - ⚠️ No ETF data collected for {config['name']}.")
       
        log_step("🌐 Closing browser...")
        await browser.close()
        log_step("✅ Browser closed.")
    if combined_etf_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = os.path.join(OUTPUT_DIRECTORY, f"combined_etf_data_{timestamp}.json")
        log_step(f"📁 Saving combined data for {len(combined_etf_data)} ETFs to {save_path}...")
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(combined_etf_data, f, indent=4)
        log_step(f"✅ Combined data saved to {save_path}.")
    else:
        log_step("❌ No ETFs scraped successfully.")
    log_step("🎉 ETF Scraper completed.")
if __name__ == "__main__":
    asyncio.run(main())