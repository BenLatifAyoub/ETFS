#!/usr/bin/env python3
"""
Amundi ETF Scraper (Hybrid Firecrawl Version - Table Only)
-------------------------------------------------
Uses Playwright for collecting ETF URLs (classic method), and Firecrawl to extract name, ISIN, and top 10 holdings from the HTML table on each ETF product page.
Extracts data directly from the page's table (e.g., 'table.top-holdings') without using CSV downloads.

Requirements:
- pip install playwright firecrawl-py
- playwright install
- Set FIRECRAWL_API_KEY as an environment variable (sign up at firecrawl.dev for a key)

WARNING: Do not hardcode API keys in scripts. Use environment variables for security.
"""
import asyncio
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin

# --- CONFIGURATION ---
TOTAL_ETFS_TO_SCRAPE = 1  # Set back to 5
BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES_SECONDS = 25
# --- END CONFIGURATION ---

AMUNDI_URL = "https://www.amundietf.de/de/professionell/etf-products/search"

from firecrawl import FirecrawlApp
app = FirecrawlApp(api_key="FIRECRAWL_API_KEY")

async def handle_consent_flow(page):
    """Handles the precise 3-step consent process sequentially."""
    print("🔎 Handling sequential consent process...")
    try:
        profile_selector = 'button[data-profile="INSTIT"]'
        print("   - Step 1: Waiting for profile selection pop-up...")
        await page.locator(profile_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Professioneller Anleger'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Profile selection pop-up not found (Step 1 skipped).")
    try:
        accept_selector = 'button:has-text("Akzeptieren und fortfahren")'
        print("   - Step 2: Waiting for final acceptance pop-up...")
        await page.locator(accept_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Akzeptieren und fortfahren'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Final acceptance pop-up not found (Step 2 skipped).")
    try:
        cookie_selector = 'button:has-text("Alle annehmen")'
        print("   - Step 3: Waiting for cookie banner...")
        await page.locator(cookie_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Alle annehmen'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Cookie banner not found (Step 3 skipped).")
    print("✅ Consent flow complete.")

async def get_etf_urls():
    """Navigate Amundi ETF search page and collect product page URLs."""
    etf_urls = []
    print("🚀 Launching browser to collect ETF URLs...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=100)
        page = await browser.new_page()
        await page.goto(AMUNDI_URL, timeout=90000)
        await handle_consent_flow(page)
        print(f"⏳ Waiting for ETF list to load...")
        await page.wait_for_selector("div.FinderResultsSection__Datatable table tbody tr", timeout=60000)
        print("✅ ETF table loaded.")
        links = await page.locator("div.FinderResultsSection__Datatable table tbody tr td a").all()
        for link in links[:TOTAL_ETFS_TO_SCRAPE]:
            href = await link.get_attribute("href")
            if href:
                full_url = urljoin("https://www.amundietf.de", href)
                etf_urls.append(full_url)
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} ETF URLs.")
    return etf_urls

def scrape_etf_page(url):
    """Use Firecrawl to extract name, ISIN, and top 10 holdings from the ETF page's HTML table."""
    print(f"📄 Processing {url}...")
    prompt = """
Analyze the Amundi ETF product page to extract:

- name: ETF full name (from header, typically within 'h1.ProductHero__title' or 'h1.text-uppercase')
- isin: ETF ISIN code (from 'div.m-isin-wkn', often within a div containing 'ISIN / WKN')

For holdings:
- Extract the top 10 holdings from the HTML table (typically in 'table.top-holdings' or a table under a section with 'Zusammensetzung' tab). For each holding:
  - name: string (from first column, often the company name)
  - sector: string or 'N/A' (from sector column)
  - securityType: string or 'N/A' (from asset class column, if available)
  - weight: float (percentage, from weight column, convert to float)
  - isin: string or 'N/A' (from ISIN column, if available)
- If no table is found or fewer than 10 holdings are available, return as many as possible.
- If no holdings are found, set holdings to an empty list.

Output strictly in JSON with no additional text:
{"name": "string", "isin": "string", "holdings": [{"name": "string", "sector": "string", "securityType": "string", "weight": float, "isin": "string"}] or []}
"""
    formats = [{"type": "json", "prompt": prompt}]
    try:
        result = app.scrape(url, formats=formats, only_main_content=False, timeout=120000)
        print(f"🔍 Firecrawl result keys: {dir(result)}")
        print(f"🔍 Firecrawl json attribute: {getattr(result, 'json', 'Not found')}")
    except Exception as e:
        print(f"❌ Scrape failed: {e}. Retrying with minimal wait.")
        try:
            result = app.scrape(url, formats=formats, actions=[{"type": "wait", "milliseconds": 5000}], only_main_content=False, timeout=120000)
            print(f"🔍 Firecrawl retry result: {getattr(result, 'json', 'Not found')}")
        except Exception as e2:
            print(f"❌ Scrape retry failed: {e2}.")
            return None
    
    # Access json as attribute (Firecrawl SDK returns ScrapeResult object)
    try:
        if hasattr(result, 'json'):
            data = result.json
        else:
            raise AttributeError("No 'json' attribute in result")
    except (AttributeError, json.JSONDecodeError) as e:
        print(f"❌ Failed to parse extracted JSON: {e}. Result attributes: {dir(result)}")
        return None
    
    etf_name = data.get('name', 'Unknown')
    etf_isin = data.get('isin', 'Unknown')
    holdings = data.get('holdings', [])
    
    if not holdings:
        print("⚠️ No holdings extracted from table.")
        return None
    
    print(f"✅ Extracted {len(holdings)} holdings from table.")
    return {"isin": etf_isin, "name": etf_name, "holdings": holdings[:10]}  # Limit to top 10 holdings

async def main():
    """Main function to orchestrate the scraping process."""
    urls = await get_etf_urls()
    if not urls:
        print("No ETFs found, exiting.")
        return

    final_json_output = {}
    for i, url in enumerate(urls):
        print(f"\n--- Processing ETF {i + 1}/{len(urls)} ---")
        
        scrape_result = scrape_etf_page(url)
        if scrape_result and scrape_result.get("isin"):
            final_json_output[scrape_result["isin"]] = scrape_result
            print(f"📊 Processed {scrape_result['name']} ({len(scrape_result['holdings'])} holdings)")
        else:
            print(f"❌ Failed to process {url} or no data returned.")
        
        is_last_item = (i + 1) == len(urls)
        if (i + 1) % BATCH_SIZE == 0 and not is_last_item:
            print(f"\n✅ Batch of {BATCH_SIZE} complete. Waiting for {DELAY_BETWEEN_BATCHES_SECONDS} seconds...")
            await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)

    if final_json_output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"amundi_etf_data_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(final_json_output, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Done! Scraped {len(final_json_output)} ETFs and saved to {filename}.")
    else:
        print("\n❌ No ETFs were scraped successfully.")

if __name__ == "__main__":
    asyncio.run(main())