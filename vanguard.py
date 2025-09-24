#!/usr/bin/env python3
"""
Unified AI-Enhanced ETF Scraper (V3 - Final & Robust)
----------------------------------------------------------------------
This script integrates the proven, site-specific logic from four original,
working scripts into a single, robust, and maintainable application.

Key Features:
- A unified main loop orchestrates the entire process.
- All site-specific logic (consent, URL finding, metadata extraction,
  file processing) is delegated to specialized functions derived from
  the original working scripts.
- This hybrid approach provides the reliability of specialized scripts
  with the convenience of a single, unified codebase.
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

try:
    # IMPORTANT: Replace with your actual Gemini API key or use environment variables
    genai.configure(api_key="put_your_api_key_here") # <-- IMPORTANT: REPLACE WITH YOUR KEY
    model = genai.GenerativeModel(GEMINI_MODEL)
except (ValueError, TypeError) as e:
    print(f"API Key Error: {e}. Please ensure you have set your Gemini API key.")
    exit()

# --- 2. UNIVERSAL AI PROMPT (Refined based on original scripts) ---
UNIVERSAL_AI_PROMPT = """
You are an expert web scraping AI assistant. Analyze the provided HTML from an ETF product page to find the portfolio holdings.

Your response MUST be in a strict JSON format.

**Priority 1: Find Holdings Download Link (CSV/XLS/XLSX)**
- Look for an `<a>` tag to download the complete holdings list.
- Search for specific keywords like "Download Holdings", "Alle Positionen herunterladen", "Komplette Wertpapierliste", "Portfolio herunterladen", "CSV", "XLS".
- **AVOID** links for "Prospectus", "KIID", "Report", "View prospectus and reports", or "Berichte" as they are not holdings files.
- If a valid download link is found, respond with:
  `{{"action": "download", "selector": "<a precise CSS selector for the link>"}}`

**Priority 2: Extract from an HTML Table**
- If no download link is found, find the HTML table listing the fund's holdings.
- For each row, extract `name`, `weight` (required, as float), `isin`, `sector`, etc.
- If a table is found, respond with:
  `{{"action": "extract", "holdings": [{{"name": "...", "weight": ...}}, ...]}}`

**Priority 3: No Data Found**
- If neither is present, respond with:
  `{{"action": "none"}}`

**HTML to Analyze:**
{html_to_analyze}
"""

# --- 3. SITE-SPECIFIC HELPER FUNCTIONS (Derived from original scripts) ---

# --- Consent Handlers ---
async def handle_vanguard_consent(page):
    try:
        await page.locator('button#onetrust-accept-btn-handler').click(timeout=7000)
    except PlaywrightTimeoutError: pass

async def handle_ishares_consent(page):
    try:
        await page.locator('#onetrust-accept-btn-handler').click(timeout=7000)
        await page.wait_for_selector('.onetrust-pc-dark-filter', state='detached', timeout=5000)
    except PlaywrightTimeoutError: pass
    try:
        await page.locator('a[data-link-event="Accept t&c: individual"]:has-text("Weiter")').click(timeout=7000)
    except PlaywrightTimeoutError: pass

async def handle_dws_consent(page):
    try:
        await page.locator('button:has-text("Accept all cookies")').click(timeout=7000)
        await page.wait_for_load_state('networkidle', timeout=5000)
    except PlaywrightTimeoutError: pass
    try:
        await page.locator('button:has-text("Akzeptieren & weiter")').click(timeout=7000)
    except PlaywrightTimeoutError: pass

async def handle_amundi_consent(page):
    try:
        await page.locator('button[data-profile="INSTIT"]').click(timeout=10000)
        await page.wait_for_load_state('networkidle', timeout=5000)
    except PlaywrightTimeoutError: pass
    try:
        await page.locator('button:has-text("Akzeptieren und fortfahren")').click(timeout=10000)
    except PlaywrightTimeoutError: pass
    try:
        await page.locator('button:has-text("Alle annehmen")').click(timeout=7000)
    except PlaywrightTimeoutError: pass

# --- URL Getters ---
async def get_vanguard_urls(page, base_url):
    urls = set()
    await page.goto(base_url, wait_until="domcontentloaded")
    await handle_vanguard_consent(page)
    await page.wait_for_selector("tr[data-rpa-tag-id]", timeout=60000)
    while await page.locator('button:has-text("Show more")').is_visible():
        await page.locator('button:has-text("Show more")').click()
        await page.wait_for_load_state('networkidle', timeout=15000)
    for link in await page.locator("tr[data-rpa-tag-id] a[data-rpa-tag-id='longName']").all():
        if href := await link.get_attribute("href"): urls.add(urljoin(base_url, href))
    return list(urls)

async def get_ishares_urls(page, base_url):
    urls = set()
    await page.goto(base_url, wait_until="domcontentloaded")
    await handle_ishares_consent(page)
    await page.wait_for_selector("a.link-to-product-page", timeout=60000)
    for link in await page.locator("a.link-to-product-page").all():
        if href := await link.get_attribute("href"):
            clean_href = href.split('?')[0]
            urls.add(f"https://www.ishares.com{clean_href}?switchLocale=y&siteEntryPassthrough=true")
    return list(urls)

async def get_dws_urls(page, base_url):
    urls = set()
    await page.goto(base_url, wait_until="domcontentloaded")
    await handle_dws_consent(page)
    selector = 'td a.d-base-link[href*="/de-de/LU"]'
    await page.wait_for_selector(selector, timeout=60000)
    for link in await page.locator(selector).all():
        if href := await link.get_attribute("href"): urls.add(urljoin(base_url, href))
    return list(urls)

async def get_amundi_urls(page, base_url):
    urls = set()
    await page.goto(base_url, wait_until="domcontentloaded")
    await handle_amundi_consent(page)
    await page.wait_for_selector("div.FinderResultsSection__Datatable table tbody tr", timeout=60000)
    for link in await page.locator("div.FinderResultsSection__Datatable table tbody tr td a").all():
        href = await link.get_attribute("href")
        # Robust check from original script's logic to prevent bad URLs
        if href and isinstance(href, str) and href.startswith('/'):
            urls.add(urljoin("https://www.amundietf.de", href))
    return list(urls)

# --- Site-Specific Metadata Extractors ---
async def extract_vanguard_metadata(page):
    # Logic from original Vanguard script
    name_selector = "span[data-rpa-tag-id='longName']"
    h1_selector = "h1[data-rpa-tag-id='dashboard-symbol']"
    await page.wait_for_selector(h1_selector, timeout=20000)
    name = (await page.locator(name_selector).first.inner_text()).strip()
    h1_text = (await page.locator(h1_selector).first.inner_text()).strip()
    ticker = h1_text.split(' ')[0]
    return {"name": name, "isin": ticker}

async def extract_ishares_metadata(page):
    # Logic from original iShares script + .first fix
    name_selector = "#fundHeader span.product-title-main"
    await page.wait_for_selector(name_selector, timeout=20000)
    name = await page.locator(name_selector).first.inner_text()
    isin = await page.locator("div.col-isin div.data").first.inner_text()
    return {"name": name.strip(), "isin": isin.strip()}

async def extract_dws_metadata(page):
    # Logic from original DWS script
    name_selector = "h1#product-header-title"
    await page.wait_for_selector(name_selector, timeout=20000)
    name = (await page.locator(name_selector).first.inner_text()).strip()
    isin = await page.locator("div.product-header__identifier__row:has-text('ISIN:') strong").first.inner_text()
    return {"name": name, "isin": isin.strip()}

async def extract_amundi_metadata(page):
    # Logic from original Amundi script
    name_selector = "h1.ProductHero__title, h1.text-uppercase"
    await page.wait_for_selector(name_selector, timeout=20000)
    name = (await page.locator(name_selector).first.inner_text()).strip()
    isin_text = await page.locator("div:has(> span:has-text('ISIN / WKN')) div.m-isin-wkn").first.inner_text()
    isin = isin_text.split('/')[0].strip()
    return {"name": name, "isin": isin}

# --- HTML Preparers & File Processors ---
def prepare_html_full(soup): return str(soup.body)
def prepare_html_ishares(soup): return str(soup.select_one("div#holdings, div#allHoldings") or soup.body)
def prepare_html_dws(soup):
    # Smart logic from original DWS script to avoid token limits
    if main_content := soup.select_one("main, #main-content"):
        return str(main_content)
    return str(soup.body)

def _process_dataframe(df):
    df.columns = [str(col).strip().lower() for col in df.columns]
    col_map = {'name': 'name', 'holding': 'name', 'bezeichnung': 'name', 'emittentenname': 'name', 'weight': 'weight', 'gewichtung': 'weight', '% assets': 'weight', 'gewichtung (%)': 'weight', 'anteil': 'weight', 'ticker': 'isin', 'isin': 'isin', 'emittententicker': 'isin', 'sector': 'sector', 'sektor': 'sector', 'asset class': 'securityType', 'anlageklasse': 'securityType', 'wertpapiertyp': 'securityType', 'country': 'country', 'land': 'country', 'currency': 'currency', 'währung': 'currency'}
    df.rename(columns=lambda c: next((new for old, new in col_map.items() if old in c), c), inplace=True)
    if 'name' not in df.columns or 'weight' not in df.columns: return []
    df['weight'] = pd.to_numeric(df['weight'].astype(str).str.replace('%', '').str.replace(',', '.').str.strip(), errors='coerce')
    df.dropna(subset=['weight', 'name'], inplace=True)
    target_cols = set(col_map.values())
    for col in target_cols:
        if col not in df.columns: df[col] = 'N/A'
    df.fillna("N/A", inplace=True)
    return df[list(target_cols)].to_dict('records')

def process_generic_download(file_path):
    try:
        # Default for Vanguard, DWS (standard CSV/XLSX)
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path, on_bad_lines='warn')
        else:
            df = pd.read_excel(file_path)
        return _process_dataframe(df)
    except Exception as e:
        print(f"     - ❌ Error in generic processing: {e}")
        return []

def process_ishares_download(file_path):
    # Logic from iShares script (try HTML first)
    try:
        df = pd.read_html(file_path)[0]
        return _process_dataframe(df)
    except Exception:
        try:
            df = pd.read_excel(file_path, engine='xlrd')
            return _process_dataframe(df)
        except Exception as e:
            print(f"     - ❌ Failed to process iShares file: {e}")
            return []

def process_amundi_download(file_path):
    # Logic from Amundi script (use calamine engine)
    try:
        df = pd.read_excel(file_path, engine="calamine")
        return _process_dataframe(df)
    except Exception as e:
        print(f"     - ❌ Failed to process Amundi file: {e}")
        return []

# --- 4. CORE SCRAPER FUNCTION ---
async def scrape_etf_page(browser, url, site_config):
    print(f"\n Scraping URL: {url}")
    if not url or not urlparse(url).scheme in ["http", "https"]:
        print(f"   - ❌ Invalid URL found: '{url}'. Skipping.")
        return None
    
    context = await browser.new_context(accept_downloads=True)
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT_MS)
        await site_config["consent_handler"](page)

        metadata = await site_config["metadata_extractor"](page)
        etf_name, etf_isin = metadata["name"], metadata["isin"]
        print(f"📄 Found ETF: {etf_name} ({etf_isin})")

        try:
            tab_selector = 'a[href*="portfolio"], a[href*="holdings"], a:has-text("Zusammensetzung")'
            await page.locator(tab_selector).first.click(timeout=10000)
            await page.wait_for_load_state('networkidle', timeout=10000)
        except PlaywrightTimeoutError: pass

        html_to_analyze = site_config["html_preparer"](BeautifulSoup(await page.content(), "html.parser"))

        print(f"🤖 Asking AI for extraction method (analyzing {len(html_to_analyze)} chars)...")
        prompt = UNIVERSAL_AI_PROMPT.format(html_to_analyze=html_to_analyze)
        response = await model.generate_content_async(prompt)
        ai_decision = json.loads(response.text.strip().replace("```json", "").replace("```", ""))
        
        holdings = []
        action = ai_decision.get('action')

        if action == 'download' and (selector := ai_decision.get('selector')):
            print(f"   - ⏳ AI suggests downloading via selector: '{selector}'")
            try:
                async with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                    await page.locator(selector).first.click(force=True, timeout=15000)
                download = await download_info.value
                temp_path = os.path.join(DOWNLOAD_DIRECTORY, download.suggested_filename)
                await download.save_as(temp_path)
                holdings = site_config["file_processor"](temp_path)
                if os.path.exists(temp_path): os.unlink(temp_path)
            except Exception as e:
                print(f"   - ❌ Download or processing failed: {e}")
        elif action == 'extract':
            holdings = ai_decision.get('holdings', [])
            print(f"   - ✅ AI extracted {len(holdings)} holdings from HTML.")
        else:
            print("   - ⚠️ AI found no data or returned an invalid action.")

        return {"isin": etf_isin, "name": etf_name, "holdings": holdings}
    except Exception as e:
        print(f"❌ An unhandled error occurred for {url}: {e}")
        # traceback.print_exc() # Uncomment for deep debugging
        return None
    finally:
        await context.close()

# --- 5. MAIN ORCHESTRATION ---
async def main():
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
    os.makedirs(DOWNLOAD_DIRECTORY, exist_ok=True)

    SITE_CONFIGS = [
        {"name": "Vanguard", "start_url": "https://investor.vanguard.com/investment-products/list/etfs", "url_getter": get_vanguard_urls, "consent_handler": handle_vanguard_consent, "metadata_extractor": extract_vanguard_metadata, "html_preparer": prepare_html_full, "file_processor": process_generic_download},
        {"name": "iShares", "start_url": "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments", "url_getter": get_ishares_urls, "consent_handler": handle_ishares_consent, "metadata_extractor": extract_ishares_metadata, "html_preparer": prepare_html_ishares, "file_processor": process_ishares_download},
        {"name": "DWS Xtrackers", "start_url": "https://etf.dws.com/de-de/produktfinder/", "url_getter": get_dws_urls, "consent_handler": handle_dws_consent, "metadata_extractor": extract_dws_metadata, "html_preparer": prepare_html_dws, "file_processor": process_generic_download},
        {"name": "Amundi", "start_url": "https://www.amundietf.de/de/professionell/etf-products/search", "url_getter": get_amundi_urls, "consent_handler": handle_amundi_consent, "metadata_extractor": extract_amundi_metadata, "html_preparer": prepare_html_full, "file_processor": process_amundi_download}
    ]

    combined_etf_data = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        for config in SITE_CONFIGS:
            site_etf_data = {}
            print(f"\n{'='*20} STARTING SCRAPE FOR: {config['name'].upper()} {'='*20}")
            try:
                page = await browser.new_page()
                urls = await config["url_getter"](page, config["start_url"])
                urls_to_scrape = list(urls)[:TOTAL_ETFS_PER_PROVIDER]
                print(f"✅ Collected {len(urls)} URLs. Will process {len(urls_to_scrape)}.")
                await page.close()
            except Exception as e:
                print(f"❌ Failed to collect URLs for {config['name']}: {e}")
                if 'page' in locals() and not page.is_closed(): await page.close()
                continue

            for i in range(0, len(urls_to_scrape), BATCH_SIZE):
                tasks = [scrape_etf_page(browser, url, config) for url in urls_to_scrape[i:i + BATCH_SIZE]]
                for result in await asyncio.gather(*tasks):
                    if result and result.get("isin"): site_etf_data[result["isin"]] = result
                if i + BATCH_SIZE < len(urls_to_scrape): await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)
            
            if site_etf_data:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                save_path = os.path.join(OUTPUT_DIRECTORY, f"{config['name'].lower().replace(' ', '_')}_data_{timestamp}.json")
                with open(save_path, "w", encoding="utf-8") as f: json.dump(site_etf_data, f, indent=4, ensure_ascii=False)
                print(f"\n✅ Saved data for {config['name']} to {save_path}")
                combined_etf_data.update(site_etf_data)

        await browser.close()

    if combined_etf_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_path = os.path.join(OUTPUT_DIRECTORY, f"combined_etf_data_{timestamp}.json")
        with open(save_path, "w", encoding="utf-8") as f: json.dump(combined_etf_data, f, indent=4, ensure_ascii=False)
        print(f"\n{'='*25}\n✅ MASTER SCRIPT COMPLETE! Scraped {len(combined_etf_data)} total ETFs.")
        print(f"   Combined data saved to: {save_path}\n{'='*25}")

if __name__ == "__main__":
    asyncio.run(main())