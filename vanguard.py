#!/usr/bin/env python3
"""
Vanguard ETF Scraper (Hybrid Firecrawl Version - Table Only)
-------------------------------------------------
Uses Playwright for collecting ETF URLs, and Firecrawl to extract name, ISIN, and top 10 holdings from the HTML table on each ETF product page.
Extracts data directly from the page's table without using CSV downloads.

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
TOTAL_ETFS_TO_SCRAPE = 2  # Set back to 5
BATCH_SIZE = 1
DELAY_BETWEEN_BATCHES_SECONDS = 61
# --- END CONFIGURATION ---

VANGUARD_URL = "https://investor.vanguard.com/investment-products/list/etfs"

from firecrawl import FirecrawlApp
app = FirecrawlApp(api_key="FIRECRAWL_API_KEY")

async def handle_consent_flow(page):
    """Handle Vanguard-specific consent process."""
    print("🔎 Handling consent process...")
    try:
        cookie_selector = 'button#onetrust-accept-btn-handler'
        print("   - Waiting for cookie banner...")
        await page.locator(cookie_selector).click(timeout=7000)
        print("   - ✅ Cookies accepted.")
        await page.wait_for_selector('.onetrust-pc-dark-filter', state='detached', timeout=5000)
    except:
        print("   - ✓ No cookie banner found.")
    print("✅ Consent flow complete.")

async def get_etf_urls():
    """Navigate Vanguard ETF list page and collect product page URLs."""
    etf_urls = []
    print("🚀 Launching browser to collect ETF URLs...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=100)
        page = await browser.new_page()
        await page.goto(VANGUARD_URL, timeout=90000, wait_until="domcontentloaded")
        await handle_consent_flow(page)
        print(f"⏳ Waiting for ETF list to load...")
        try:
            await page.wait_for_selector("tr[data-rpa-tag-id]", timeout=60000)
            print("✅ ETF table loaded.")
            while await page.locator('button:has-text("Show more")').is_visible():
                await page.locator('button:has-text("Show more")').click()
                await page.wait_for_load_state('networkidle', timeout=15000)
            links = await page.locator("tr[data-rpa-tag-id] a[data-rpa-tag-id='longName']").all()
            for link in links[:TOTAL_ETFS_TO_SCRAPE]:
                href = await link.get_attribute("href")
                if href:
                    full_url = urljoin(VANGUARD_URL, href)
                    etf_urls.append(full_url)
        except Exception as e:
            print(f"❌ Failed to load ETF list: {e}")
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} ETF URLs.")
    return etf_urls

def scrape_etf_page(url):
    """Use Firecrawl to extract name, ISIN, and top 10 holdings from the ETF page's HTML table."""
    print(f"📄 Processing {url}...")
    prompt = """
Analyze the Vanguard ETF product page to extract:

- name: ETF full name (from 'span[data-rpa-tag-id="longName"]' or 'h1[data-rpa-tag-id="dashboard-symbol"]')
- isin: ETF ISIN code (from a div or span containing 'ISIN', often near 'Fund facts')

For holdings:
- Extract the top 10 holdings from the HTML table (typically under a 'Holdings' or 'Portfolio' section, e.g., 'table.holdings-table'). For each holding:
  - name: string (from column 'Holding' or 'Name')
  - sector: string or 'N/A' (from column 'Sector')
  - securityType: string or 'N/A' (from column 'Asset Class' or inferred as 'Equity')
  - weight: float (percentage, from column '% Assets' or 'Weight', convert to float)
  - isin: string or 'N/A' (from column 'ISIN' or 'Ticker' if available)
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
        filename = f"vanguard_etf_data_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(final_json_output, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Done! Scraped {len(final_json_output)} ETFs and saved to {filename}.")
    else:
        print("\n❌ No ETFs were scraped successfully.")

if __name__ == "__main__":
    asyncio.run(main())