#!/usr/bin/env python3
"""
iShares ETF Scraper (Hybrid Firecrawl Version - Table Only - Fixed)
-------------------------------------------------
Uses Playwright for collecting ETF URLs (classic method), and Firecrawl to extract name, ISIN, and top 10 holdings from the HTML table on each ETF product page.
Extracts data directly from the page's table (e.g., '#tabsTen-largest') without using CSV downloads.

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

# --- CONFIGURATION ---
TOTAL_ETFS_TO_SCRAPE = 1  # Set back to 5
BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES_SECONDS = 25
# --- END CONFIGURATION ---

ISHARES_URL = "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments#/?productView=all&pageNumber=1&sortColumn=totalFundSizeInMillions&sortDirection=desc&dataView=keyFacts&keyFacts=all"

from firecrawl import FirecrawlApp
app = FirecrawlApp(api_key="FIRECRAWL_API_KEY")

async def get_etf_urls():
    """Navigate iShares ETF list and collect product page URLs using Playwright."""
    etf_urls = []
    print("🚀 Launching browser to collect ETF URLs...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=150)
        page = await browser.new_page()
        await page.goto(ISHARES_URL, timeout=90000)

        try:
            await page.locator('#onetrust-accept-btn-handler').click(timeout=10000)
            print("✅ Cookies accepted")
        except:
            print("⚠️ No cookie banner found.")

        try:
            await page.locator('a[data-link-event="Accept t&c: individual"]:has-text("Weiter")').click(timeout=10000)
            print("✅ Clicked Weiter")
        except:
            print("⚠️ No Weiter link found.")

        print(f"⏳ Waiting for ETF list to load to collect the top {TOTAL_ETFS_TO_SCRAPE} URLs...")
        await page.wait_for_selector("a.link-to-product-page", timeout=60000)
        links = await page.locator("a.link-to-product-page").all()

        for link in links[:TOTAL_ETFS_TO_SCRAPE]:
            href = await link.get_attribute("href")
            if href:
                clean_href = href.split('?')[0]
                url = f"https://www.ishares.com{clean_href}?switchLocale=y&siteEntryPassthrough=true"
                etf_urls.append(url)

        await browser.close()
    print(f"✅ Collected {len(etf_urls)} ETF URLs to be processed.")
    return etf_urls

def scrape_etf_page(url):
    """Use Firecrawl to extract name, ISIN, and top 10 holdings from the ETF page's HTML table."""
    print(f"📄 Processing {url}...")
    prompt = """
Analyze the iShares ETF product page to extract:

- name: ETF full name (from header, typically within 'span.product-title-main' under '#fundHeader')
- isin: ETF ISIN code (from 'div.col-isin div.data')

For holdings:
- Extract the top 10 holdings from the HTML table (typically in '#tabsTen-largest' or 'table#allHoldingsTable'). For each holding:
  - name: string (from 'td.colIssueName' or 'td.colAssetClassName')
  - sector: string or 'N/A' (from 'td.colSector')
  - securityType: string or 'N/A' (from 'td.colSecurityType')
  - weight: float (percentage, from 'td.colFundPercentage', convert to float)
  - isin: string or 'N/A' (from 'td.colIsin')
- If no table is found or fewer than 10 holdings are available, return as many as possible.
- If no holdings are found, set holdings to an empty list.

Output strictly in JSON with no additional text:
{"name": "string", "isin": "string", "holdings": [{"name": "string", "sector": "string", "securityType": "string", "weight": float, "isin": "string"}] or []}
"""
    formats = [{"type": "json", "prompt": prompt}]
    try:
        result = app.scrape(url, formats=formats, only_main_content=False, timeout=120000)
        print(f"🔍 Firecrawl result keys: {dir(result)}")  # Debug: Show available attributes
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
        filename = f"ishares_data_{timestamp}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(final_json_output, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Done! Scraped {len(final_json_output)} ETFs and saved to {filename}.")
    else:
        print("\n❌ No ETFs were scraped successfully.")

if __name__ == "__main__":
    asyncio.run(main())