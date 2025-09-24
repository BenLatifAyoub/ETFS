#!/usr/bin/env python3
"""
iShares ETF Scraper (AI-Enhanced Version)
-------------------------------------------------
Uses AI (Gemini 2.5 Flash) to analyze the HTML and decide whether to download the holdings CSV or extract from HTML tables.
Falls back gracefully if neither is available.

WARNING: Hardcoding API keys in scripts is insecure and not recommended. It can lead to accidental exposure (e.g., if you share the code or commit it to a repo). Use environment variables instead for production/security.
"""
import asyncio
import json
import os
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import traceback
import google.generativeai as genai

# --- CONFIGURATION ---
TOTAL_ETFS_TO_SCRAPE = 1  # Set back to 5
BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES_SECONDS = 25
CSV_DOWNLOAD_TIMEOUT_MS = 30000  # Increased timeout
GEMINI_MODEL = "gemini-2.0-flash-lite"  # Valid model
# --- END CONFIGURATION ---

genai.configure(api_key="put_your_api_key_here")
model = genai.GenerativeModel(GEMINI_MODEL)

ISHARES_URL = "https://www.ishares.com/de/privatanleger/de/produkte/etf-investments#/?productView=all&pageNumber=1&sortColumn=totalFundSizeInMillions&sortDirection=desc&dataView=keyFacts&keyFacts=all"


async def get_etf_urls():
    """Navigate iShares ETF list and collect product page URLs."""
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


async def scrape_etf_page_and_download_csv(playwright, url):
    """
    Uses AI to analyze the page HTML and decide whether to download CSV, extract from table, or skip.
    """
    browser = await playwright.chromium.launch(headless=False)
    page = await browser.new_page()
    
    try:
        page.set_default_timeout(90000)
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="domcontentloaded")

        # Handle cookie banner and Weiter on product page more robustly
        try:
            await page.wait_for_selector('#onetrust-accept-btn-handler', timeout=10000)
            await page.locator('#onetrust-accept-btn-handler').click()
            print("✅ Cookies accepted on product page")
        except:
            print("⚠️ No cookie banner found on product page.")
        name_selector = "#fundHeader > header.main-header span.product-title-main"
        await page.wait_for_selector(name_selector)
        
        etf_name = await page.locator(name_selector).inner_text()
        etf_isin = await page.locator("div.col-isin div.data").inner_text()
        
        print(f"📄 Found ETF: {etf_name} ({etf_isin})")

        # Click holdings tab if exists to ensure content is loaded
        try:
            holdings_tab = page.locator('a[href="#holdings"]') or page.locator('#holdingsTab a') or page.locator('li[data-tab="holdings"] a') or page.locator('a[data-tab="holdings"]')
            await holdings_tab.click()
            await page.wait_for_load_state('networkidle', timeout=10000)
            print("✅ Clicked holdings tab.")
        except:
            print("⚠️ No holdings tab found or already loaded.")

        html_content = await page.content()
        soup = BeautifulSoup(html_content, "html.parser")
        
        # Extract only the relevant holdings section to reduce token count
        holdings_section = soup.select_one("div#holdings") or soup.select_one("div#allHoldings") or soup.select_one("section#holdingsTab") or soup
        html_to_analyze = str(holdings_section)
        
        # Feed extracted HTML to Gemini for decision
        prompt = f"""
You are an AI assistant analyzing the HTML of an iShares ETF product page to extract holdings information.

Your task is to decide the best way to get the holdings data.

Options:
1. If there is a CSV/XLS download link specifically for holdings (often labeled as 'Fondspositionen und Kennzahlen' or 'Holdings' or 'Alle Holdings' with an XLS icon or class 'icon-xls-export', and there may be multiple CSVs—select the one for holdings), provide the CSS selector for the <a> tag that triggers the download, e.g., 'a.icon-xls-export[data-link-event="holdings:holdings"]' or 'div#holdings a.icon-xls-export'.
2. If no such download link exists, but there is an HTML table displaying the top holdings (like 'Top-Positionen' or 'All Holdings'), extract the data from the table. Extract as many holdings as possible, focusing on top holdings if many. For each holding, get:
   - name: The name of the holding
   - sector: The sector (if available)
   - securityType: The asset class or type
   - weight: The weight percentage (as float)
   - isin: The ISIN code (if available)
   Use 'N/A' for missing fields.
3. If neither a download link nor a holdings table is present, indicate that nothing exists.

Output strictly in JSON format with no additional text, code fences, or markdown:
- If option 1: {{"action": "download", "selector": "<css-selector>"}}
- If option 2: {{"action": "extract", "holdings": [<list of dicts>]}}
- If option 3: {{"action": "none"}}

HTML content:
{html_to_analyze}
"""
        
        response = model.generate_content(prompt)
        if not response.text:
            print("⚠️ LLM response.text is empty. This may be due to token limits, invalid model, or API issues. Skipping.")
            return None
        
        # Clean the response text to handle potential markdown wrapping
        response_text = response.text.strip()
        if response_text.startswith('```json'):
            response_text = response_text[7:].strip()  # Remove ```json
        if response_text.startswith('```'):
            response_text = response_text[3:].strip()  # Remove any remaining ```
        if response_text.endswith('```'):
            response_text = response_text[:-3].strip()
        
        try:
            data = json.loads(response_text)
        except Exception as e:
            print(f"❌ Error parsing LLM response: {e}")
            print(f"Raw response text: {response.text}")
            return None
        
        enriched_holdings = []
        
        if data.get('action') == 'download':
            selector = data.get('selector')
            if not selector:
                print("⚠️ No selector provided by AI.")
                return None
            
            # Refine selector to target holdings if not specific
            if '#holdings' not in selector and 'holdings' not in selector.lower():
                selector = f"div#holdings {selector}"
            
            print(f"⏳ AI decided to download CSV using selector: {selector}")
            try:
                async with page.expect_download(timeout=CSV_DOWNLOAD_TIMEOUT_MS) as download_info:
                    download_link = page.locator(selector)
                    await download_link.scroll_into_view_if_needed()
                    await download_link.click(force=True)  # Use force=True to click even if intercepted
                    
                download = await download_info.value
                path = await download.path()
                print(f"✅ CSV downloaded successfully.")

                # Process the downloaded CSV with Pandas
                start_row = 0
                with open(path, 'r', encoding='utf-8-sig', errors='ignore') as f:
                    for i, line in enumerate(f):
                        if "Emittententicker" in line or "Ticker" in line:
                            start_row = i
                            break
                
                if start_row > 0:
                    df = pd.read_csv(path, skiprows=start_row, encoding='utf-8-sig', sep=',')
                    column_mapping = {'Name': 'name', 'Sektor': 'sector', 'Anlageklasse': 'securityType', 'Gewichtung (%)': 'weight'}
                    df.rename(columns=column_mapping, inplace=True)
                    required_cols = ['name', 'sector', 'securityType', 'weight']

                    if all(col in df.columns for col in required_cols):
                        df['weight'] = pd.to_numeric(df['weight'].astype(str).str.replace(',', '.'), errors='coerce')
                        df.dropna(subset=['weight', 'name'], inplace=True)
                        df.fillna({'sector': 'N/A', 'securityType': 'N/A'}, inplace=True)
                        holdings_from_csv = df[required_cols].to_dict('records')
                        
                        print("Enriching CSV data with ISINs from HTML...")
                        soup = BeautifulSoup(html_content, "html.parser")
                        isin_map = {}
                        table = soup.select_one("table#allHoldingsTable")
                        if table:
                            for row in table.select("tbody tr"):
                                name_cell = row.select_one("td.colIssueName")
                                isin_cell = row.select_one("td.colIsin")
                                if name_cell and isin_cell:
                                    isin_map[name_cell.get_text(strip=True)] = isin_cell.get_text(strip=True)
                        
                        for holding in holdings_from_csv:
                            holding['isin'] = isin_map.get(holding['name'], 'N/A')
                            enriched_holdings.append(holding)
                os.remove(path)
            except TimeoutError as e:
                print(f"⚠️ CSV download timed out: {e}. Falling back to HTML extraction.")
                # Fallback to scraping HTML
                top_holdings_container = soup.select_one("#tabsTen-largest")
                if top_holdings_container:
                    print("Found 'Largest positions' table structure...")
                    rows = top_holdings_container.select("table.holdingTable tbody tr")
                    for row in rows:
                        name_elem = row.select_one("td.colName")
                        weight_elem = row.select_one("td.colFundPercentage")
                        if name_elem and weight_elem:
                            try:
                                enriched_holdings.append({
                                    "name": name_elem.get_text(strip=True),
                                    "weight": float(weight_elem.get_text(strip=True).replace(',', '.')),
                                    "isin": "N/A", "sector": "N/A", "securityType": "N/A"
                                })
                            except (ValueError, AttributeError):
                                continue
                
                # If nothing, try 'All Holdings' table
                if not enriched_holdings:
                    print("Could not find 'Largest positions' structure, trying 'All Holdings' table...")
                    all_holdings_table = soup.select_one("table#allHoldingsTable")
                    if all_holdings_table:
                        rows = all_holdings_table.select("tbody tr")
                        for row in rows:
                            name_elem = row.select_one("td.colIssueName") or row.select_one("td.colAssetClassName")
                            weight_elem = row.select_one("td.colFundPercentage")
                            isin_elem = row.select_one("td.colIsin")
                            sector_elem = row.select_one("td.colSector")
                            security_type_elem = row.select_one("td.colSecurityType")
                            name = name_elem.get_text(strip=True) if name_elem else 'N/A'
                            weight = float(weight_elem.get_text(strip=True).replace(',', '.')) if weight_elem else 0.0
                            isin = isin_elem.get_text(strip=True) if isin_elem else 'N/A'
                            sector = sector_elem.get_text(strip=True) if sector_elem else 'N/A'
                            securityType = security_type_elem.get_text(strip=True) if security_type_elem else 'N/A'
                            try:
                                enriched_holdings.append({
                                    "name": name,
                                    "weight": weight,
                                    "isin": isin,
                                    "sector": sector,
                                    "securityType": securityType
                                })
                            except (ValueError, AttributeError):
                                continue

                if not enriched_holdings:
                    print("⚠️ Fallback failed: Could not find any holdings table on the page.")
            except Exception as e:
                print(f"❌ Error during download: {e}")

        elif data.get('action') == 'extract':
            enriched_holdings = data.get('holdings', [])
            print(f"✅ AI extracted {len(enriched_holdings)} holdings from HTML table.")

        elif data.get('action') == 'none':
            print("⚠️ AI found no holdings data (neither CSV nor table).")
        
        else:
            print("❌ Invalid action from AI.")

        return { "isin": etf_isin, "name": etf_name, "holdings": enriched_holdings }
    finally:
        try:
            await browser.close()
        except Exception as e:
            print(f"⚠️ Error closing browser: {e}. This may be due to a Playwright driver issue; ignoring for now.")


async def main():
    try:
        urls = await get_etf_urls()
        if not urls:
            print("No ETFs found, exiting.")
            return

        final_json_output = {}
        async with async_playwright() as p:
            for i, url in enumerate(urls):
                print(f"\n--- Processing ETF {i + 1}/{len(urls)} ---")
                
                try:
                    scrape_result = await scrape_etf_page_and_download_csv(p, url)
                    if scrape_result and scrape_result.get("isin"):
                        final_json_output[scrape_result["isin"]] = scrape_result
                        print(f"📊 Processed {scrape_result['name']} ({len(scrape_result['holdings'])} holdings)")
                    else:
                        print(f"❌ Failed to process {url} or no data returned.")
                
                except Exception as e:
                    print(f"❌ A critical error occurred while processing {url}: {e}")
                    traceback.print_exc()

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

    except Exception as e:
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())