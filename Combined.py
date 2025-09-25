#!/usr/bin/env python3
"""
Unified ETF Scraper with Final JSON Combination
-------------------------------------------------
A single, combined script to scrape ETF data from Vanguard, DWS, iShares, and Amundi.
It uses Playwright to collect ETF URLs and Firecrawl to extract holdings data.

Process:
1. Scrapes each provider sequentially.
2. Saves an individual JSON file for each provider's data.
3. Collects all results into a master dictionary.
4. Saves one final, combined JSON file containing the data from all providers.

Requirements:
- pip install playwright firecrawl-py
- playwright install
- Set the FIRECRAWL_API_KEY as an environment variable (sign up at firecrawl.dev for a key).

WARNING: Do not hardcode API keys in scripts. Use environment variables for security.
"""
import asyncio
import json
import os
from datetime import datetime
from playwright.async_api import async_playwright
from urllib.parse import urljoin
from firecrawl import FirecrawlApp

# --- CENTRAL CONFIGURATION ---
# Note: TOTAL_ETFS_TO_SCRAPE applies to EACH provider.
TOTAL_ETFS_TO_SCRAPE = 2  # Set the number of ETFs to scrape per provider
BATCH_SIZE = 2            # Number of ETFs to process before pausing
DELAY_BETWEEN_BATCHES_SECONDS = 1 # Pause duration in seconds between batches

# --- FIRECRAWL INITIALIZATION ---
FIRECRAWL_API_KEY = "FIRECRAWL_API_KEY"
if not FIRECRAWL_API_KEY:
    raise ValueError("FIRECRAWL_API_KEY environment variable not set. Please get a key from firecrawl.dev.")

app = FirecrawlApp(api_key=FIRECRAWL_API_KEY)


# --- PROVIDER-SPECIFIC LOGIC ---

# 1. Vanguard
# ---------------------------------
VANGUARD_URL = "https://investor.vanguard.com/investment-products/list/etfs"
VANGUARD_PROMPT = """
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

async def handle_vanguard_consent(page):
    print("🔎 Handling Vanguard consent process...")
    try:
        await page.locator('button#onetrust-accept-btn-handler').click(timeout=7000)
        print("   - ✅ Cookies accepted.")
        await page.wait_for_selector('.onetrust-pc-dark-filter', state='detached', timeout=5000)
    except Exception:
        print("   - ✓ No cookie banner found.")
    print("✅ Vanguard consent flow complete.")

async def get_vanguard_etf_urls(playwright):
    etf_urls = []
    print("🚀 Launching browser for Vanguard URLs...")
    browser = await playwright.chromium.launch(headless=False, slow_mo=100)
    page = await browser.new_page()
    try:
        await page.goto(VANGUARD_URL, timeout=90000, wait_until="domcontentloaded")
        await handle_vanguard_consent(page)
        print("⏳ Waiting for Vanguard ETF list to load...")
        await page.wait_for_selector("tr[data-rpa-tag-id]", timeout=60000)
        while await page.locator('button:has-text("Show more")').is_visible():
            await page.locator('button:has-text("Show more")').click()
            await page.wait_for_load_state('networkidle', timeout=15000)
        links = await page.locator("tr[data-rpa-tag-id] a[data-rpa-tag-id='longName']").all()
        for link in links[:TOTAL_ETFS_TO_SCRAPE]:
            href = await link.get_attribute("href")
            if href:
                etf_urls.append(urljoin(VANGUARD_URL, href))
    except Exception as e:
        print(f"❌ Failed to get Vanguard URLs: {e}")
    finally:
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} Vanguard ETF URLs.")
    return etf_urls

# 2. DWS Xtrackers
# ---------------------------------
DWS_URL = "https://etf.dws.com/de-de/produktfinder/"
DWS_PROMPT = """
Analyze the DWS Xtrackers ETF product page to extract:
- name: ETF full name (from header, typically within 'h1#product-header-title')
- isin: ETF ISIN code (from 'div.product-header__identifier__row strong' near 'ISIN:')
For holdings:
- Extract the top 10 holdings from the HTML table (typically under a section with 'Top 10' or 'Wertpapiere des Wertpapierkorbs'). For each holding:
  - name: string (from column 'Name' or 'Bezeichnung')
  - sector: string or 'N/A' (from column 'Sektor' or 'Industry Classification')
  - securityType: string or 'N/A' (from column 'Anlageklasse' or 'Asset Class')
  - weight: float (percentage, from column 'Gewichtung %' or 'Gewichtung', convert to float)
  - isin: string or 'N/A' (from column 'ISIN')
- If no holdings are found, set holdings to an empty list.
Output strictly in JSON with no additional text:
{"name": "string", "isin": "string", "holdings": [{"name": "string", "sector": "string", "securityType": "string", "weight": float, "isin": "string"}] or []}
"""

async def handle_dws_consent(page):
    print("🔎 Handling DWS consent process...")
    try:
        await page.locator('button:has-text("Accept all cookies")').click(timeout=10000)
        print("   - ✅ Clicked 'Accept all cookies'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Cookie banner not found.")
    try:
        await page.locator('button:has-text("Akzeptieren & weiter")').click(timeout=10000)
        print("   - ✅ Clicked 'Akzeptieren & weiter'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Disclaimer pop-up not found.")
    print("✅ DWS consent flow complete.")

async def get_dws_etf_urls(playwright):
    etf_urls = []
    print("🚀 Launching browser for DWS URLs...")
    browser = await playwright.chromium.launch(headless=False, slow_mo=50)
    page = await browser.new_page()
    try:
        await page.goto(DWS_URL, timeout=90000, wait_until="domcontentloaded")
        await handle_dws_consent(page)
        print("⏳ Waiting for DWS ETF list to load...")
        link_selector = 'td a.d-base-link[href*="/de-de/LU"]'
        await page.wait_for_selector(link_selector, timeout=60000)
        links = await page.locator(link_selector).all()
        unique_urls = set()
        for link in links:
            href = await link.get_attribute("href")
            if href:
                unique_urls.add(urljoin(DWS_URL, href))
        etf_urls = list(unique_urls)[:TOTAL_ETFS_TO_SCRAPE]
    except Exception as e:
        print(f"❌ Failed to get DWS URLs: {e}")
    finally:
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} DWS ETF URLs.")
    return etf_urls

# 3. iShares
# ---------------------------------
ISHARES_URL = "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments#/?productView=all&pageNumber=1&sortColumn=totalFundSizeInMillions&sortDirection=desc&dataView=keyFacts&keyFacts=all"
ISHARES_PROMPT = """
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
- If no holdings are found, set holdings to an empty list.
Output strictly in JSON with no additional text:
{"name": "string", "isin": "string", "holdings": [{"name": "string", "sector": "string", "securityType": "string", "weight": float, "isin": "string"}] or []}
"""
async def get_ishares_etf_urls(playwright):
    etf_urls = []
    print("🚀 Launching browser for iShares URLs...")
    browser = await playwright.chromium.launch(headless=False, slow_mo=150)
    page = await browser.new_page()
    try:
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
        print("⏳ Waiting for iShares ETF list to load...")
        await page.wait_for_selector("a.link-to-product-page", timeout=60000)
        links = await page.locator("a.link-to-product-page").all()
        for link in links[:TOTAL_ETFS_TO_SCRAPE]:
            href = await link.get_attribute("href")
            if href:
                clean_href = href.split('?')[0]
                etf_urls.append(f"https://www.ishares.com{clean_href}?switchLocale=y&siteEntryPassthrough=true")
    except Exception as e:
        print(f"❌ Failed to get iShares URLs: {e}")
    finally:
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} iShares ETF URLs.")
    return etf_urls

# 4. Amundi
# ---------------------------------
AMUNDI_URL = "https://www.amundietf.de/de/professionell/etf-products/search"
AMUNDI_PROMPT = """
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
- If no holdings are found, set holdings to an empty list.
Output strictly in JSON with no additional text:
{"name": "string", "isin": "string", "holdings": [{"name": "string", "sector": "string", "securityType": "string", "weight": float, "isin": "string"}] or []}
"""

async def handle_amundi_consent(page):
    print("🔎 Handling Amundi consent process...")
    try:
        await page.locator('button[data-profile="INSTIT"]').click(timeout=10000)
        print("   - ✅ Clicked 'Professioneller Anleger'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Profile selection not found.")
    try:
        await page.locator('button:has-text("Akzeptieren und fortfahren")').click(timeout=10000)
        print("   - ✅ Clicked 'Akzeptieren und fortfahren'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Final acceptance pop-up not found.")
    try:
        await page.locator('button:has-text("Alle annehmen")').click(timeout=10000)
        print("   - ✅ Clicked 'Alle annehmen'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except:
        print("   - ✓ Cookie banner not found.")
    print("✅ Amundi consent flow complete.")

async def get_amundi_etf_urls(playwright):
    etf_urls = []
    print("🚀 Launching browser for Amundi URLs...")
    browser = await playwright.chromium.launch(headless=False, slow_mo=100)
    page = await browser.new_page()
    try:
        await page.goto(AMUNDI_URL, timeout=90000)
        await handle_amundi_consent(page)
        print("⏳ Waiting for Amundi ETF list to load...")
        await page.wait_for_selector("div.FinderResultsSection__Datatable table tbody tr", timeout=60000)
        links = await page.locator("div.FinderResultsSection__Datatable table tbody tr td a").all()
        for link in links[:TOTAL_ETFS_TO_SCRAPE]:
            href = await link.get_attribute("href")
            if href:
                etf_urls.append(urljoin("https://www.amundietf.de", href))
    except Exception as e:
        print(f"❌ Failed to get Amundi URLs: {e}")
    finally:
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} Amundi ETF URLs.")
    return etf_urls


# --- GENERIC SCRAPING FUNCTION ---

def scrape_etf_page(url, prompt):
    """
    Generic function to scrape an ETF page using Firecrawl with a specific prompt.
    """
    print(f"📄 Processing {url} with Firecrawl...")
    formats = [{"type": "json", "prompt": prompt}]
    try:
        result = app.scrape(url, formats=formats, only_main_content=False, timeout=120000)
    except Exception as e:
        print(f"❌ Scrape failed: {e}. Retrying with a wait action...")
        try:
            result = app.scrape(url, formats=formats, actions=[{"type": "wait", "milliseconds": 5000}], only_main_content=False, timeout=120000)
        except Exception as e2:
            print(f"❌ Scrape retry also failed: {e2}.")
            return None

    try:
        if hasattr(result, 'json'):
            data = result.json
            etf_name = data.get('name', 'Unknown')
            etf_isin = data.get('isin', 'Unknown')
            holdings = data.get('holdings', [])
            if not holdings:
                print("⚠️ No holdings were extracted from the page.")
            print(f"✅ Extracted data for {etf_name} ({len(holdings)} holdings).")
            return {"isin": etf_isin, "name": etf_name, "holdings": holdings[:10]}
        else:
            raise AttributeError("Result object has no 'json' attribute.")
    except (AttributeError, json.JSONDecodeError, TypeError) as e:
        print(f"❌ Failed to parse extracted JSON from Firecrawl: {e}.")
        return None


# --- MAIN ORCHESTRATOR ---

async def main():
    """
    Main function to orchestrate the scraping process for all configured providers.
    """
    # --- ADD THIS SECTION ---
    # Define the output directory and create it if it doesn't exist
    OUTPUT_DIR = "output"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"📂 Output will be saved to the '{OUTPUT_DIR}' folder.")
    # --- END OF ADDED SECTION ---

    providers = [
        {"name": "Vanguard", "get_urls_func": get_vanguard_etf_urls, "prompt": VANGUARD_PROMPT, "filename_prefix": "vanguard_etf_data"},
        {"name": "DWS", "get_urls_func": get_dws_etf_urls, "prompt": DWS_PROMPT, "filename_prefix": "dws_etf_data"},
        {"name": "iShares", "get_urls_func": get_ishares_etf_urls, "prompt": ISHARES_PROMPT, "filename_prefix": "ishares_data"},
        {"name": "Amundi", "get_urls_func": get_amundi_etf_urls, "prompt": AMUNDI_PROMPT, "filename_prefix": "amundi_etf_data"},
    ]

    all_providers_data = {}

    async with async_playwright() as p:
        for provider in providers:
            print(f"\n{'='*20} Starting Scraper for {provider['name'].upper()} {'='*20}")

            urls = await provider["get_urls_func"](p)
            if not urls:
                print(f"No URLs found for {provider['name']}. Skipping to next provider.")
                continue

            provider_json_output = {}
            for i, url in enumerate(urls):
                print(f"\n--- Processing ETF {i + 1}/{len(urls)} for {provider['name']} ---")
                scrape_result = scrape_etf_page(url, provider["prompt"])

                if scrape_result and scrape_result.get("isin") not in [None, "Unknown", ""]:
                    provider_json_output[scrape_result["isin"]] = scrape_result
                    print(f"📊 Successfully processed {scrape_result.get('name', 'N/A')}")
                else:
                    print(f"❌ Failed to process {url} or essential data (like ISIN) was missing.")

                is_last_item = (i + 1) == len(urls)
                if (i + 1) % BATCH_SIZE == 0 and not is_last_item:
                    print(f"\n✅ Batch of {BATCH_SIZE} complete. Waiting for {DELAY_BETWEEN_BATCHES_SECONDS} seconds...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)

            # Step 3: Save individual provider file and store data in the master dictionary
            if provider_json_output:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # --- CHANGE THIS LINE ---
                # Prepend the output directory to the filename
                filename = os.path.join(OUTPUT_DIR, f"{provider['filename_prefix']}_{timestamp}.json")
                # --- END OF CHANGE ---

                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(provider_json_output, f, indent=2, ensure_ascii=False)
                print(f"\n✅ Saved individual file for {provider['name']} to {filename}.")

                all_providers_data[provider['name']] = provider_json_output
            else:
                print(f"\n❌ No ETFs were scraped successfully for {provider['name']}.")

    # Step 4: After the loop, save the combined JSON file
    print("\n" + "="*50)
    print("  All scraping tasks complete. Consolidating results...")
    print("="*50)

    if all_providers_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # --- CHANGE THIS LINE ---
        # Prepend the output directory to the combined filename
        combined_filename = os.path.join(OUTPUT_DIR, f"combined_etf_data_{timestamp}.json")
        # --- END OF CHANGE ---

        with open(combined_filename, "w", encoding="utf-8") as f:
            json.dump(all_providers_data, f, indent=2, ensure_ascii=False)
        print(f"\n🎉 SUCCESS! All provider data has been combined and saved to: {combined_filename}")
    else:
        print("\n❌ No data was collected from any provider. No combined file was created.")

if __name__ == "__main__":
    if FIRECRAWL_API_KEY == "FIRECRAWL_API_KEY":
        print("⚠️ WARNING: You are using a placeholder Firecrawl API key.")
        print("   Please replace it with your own key or set the FIRECRAWL_API_KEY environment variable.")
    asyncio.run(main())