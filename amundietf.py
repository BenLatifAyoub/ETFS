#!/usr/bin/env python3
"""
Amundi ETF Scraper (AI-Enhanced Version)
-------------------------------------------------
Uses AI (Gemini 1.5 Flash) to analyze the HTML and decide whether to download the holdings XLS or extract from HTML tables.
Falls back gracefully if neither is available.

WARNING: Hardcoding API keys in scripts is insecure and not recommended. It can lead to accidental exposure (e.g., if you share the code or commit it to a repo). Use environment variables instead for production/security.
"""
import asyncio
import json
import os
import tempfile
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import traceback
import google.generativeai as genai
from urllib.parse import urljoin

# --- CONFIGURATION ---
TOTAL_ETFS_TO_SCRAPE = 1
BATCH_SIZE = 5
DELAY_BETWEEN_BATCHES_SECONDS = 25
DOWNLOAD_TIMEOUT_MS = 60000
GEMINI_MODEL = "gemini-2.0-flash"
OUTPUT_DIRECTORY = "etf_data"
DOWNLOAD_DIRECTORY = "downloads"
# --- END CONFIGURATION ---

# IMPORTANT: Replace with your actual Gemini API key
try:
    genai.configure(api_key="put_your_api_key_here")
except (ValueError, TypeError) as e:
    print(f"API Key Error: {e}. Please ensure you have set your Gemini API key.")
    exit()

model = genai.GenerativeModel(GEMINI_MODEL)
AMUNDI_URL = "https://www.amundietf.de/de/professionell/etf-products/search"

async def handle_consent_flow(page):
    """Handles the precise 3-step consent process sequentially."""
    print("🔎 Handling sequential consent process...")
    try:
        profile_selector = 'button[data-profile="INSTIT"]'
        print("   - Step 1: Waiting for profile selection pop-up...")
        await page.locator(profile_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Professioneller Anleger'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except PlaywrightTimeoutError:
        print("   - ✓ Profile selection pop-up not found (Step 1 skipped).")
    try:
        accept_selector = 'button:has-text("Akzeptieren und fortfahren")'
        print("   - Step 2: Waiting for final acceptance pop-up...")
        await page.locator(accept_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Akzeptieren und fortfahren'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except PlaywrightTimeoutError:
        print("   - ✓ Final acceptance pop-up not found (Step 2 skipped).")
    try:
        cookie_selector = 'button:has-text("Alle annehmen")'
        print("   - Step 3: Waiting for cookie banner...")
        await page.locator(cookie_selector).click(timeout=10000)
        print("   - ✅ Clicked 'Alle annehmen'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except PlaywrightTimeoutError:
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
            print("aaaaa", link)
            href = await link.get_attribute("href")
            if href:
                full_url = urljoin("https://www.amundietf.de", href)
                etf_urls.append(full_url)
        await browser.close()
    print(f"✅ Collected {len(etf_urls)} ETF URLs.")
    return etf_urls

async def scrape_etf_page(browser, url):
    """Uses AI to analyze the page and robustly handle downloads or table extraction."""
    context = await browser.new_context(accept_downloads=True)
    page = await context.new_page()
    temp_path = None
    
    try:
        page.set_default_timeout(60000)
        print(f"Navigating to product page: {url}...")
        await page.goto(url, wait_until="domcontentloaded")
        
        await handle_consent_flow(page)

        name_selector = "h1.ProductHero__title, h1.text-uppercase"
        await page.wait_for_selector(name_selector)
        etf_name = (await page.locator(name_selector).first.inner_text()).strip()
        isin_container_locator = page.locator("div:has(> span:has-text('ISIN / WKN'))").first
        isin_text_content = await isin_container_locator.locator("div.m-isin-wkn").inner_text()
        etf_isin = isin_text_content.split('/')[0].strip()
        print(f"📄 Found ETF: {etf_name} ({etf_isin})")

        # Click composition tab to load the holdings section and download link
        try:
            composition_tab = page.locator('a:has-text("Zusammensetzung")')
            await composition_tab.click()
            await page.wait_for_load_state('networkidle', timeout=10000)
            print("✅ Clicked composition tab to load download link.")
        except:
            print("⚠️ No composition tab found or already loaded.")

        html_content = await page.content()
        soup = BeautifulSoup(html_content, "html.parser")
        html_to_analyze = html_content  # Use full HTML page content
        
        prompt = f"""
        You are an AI assistant analyzing the HTML of an Amundi ETF product page to extract holdings information.

        Your primary goal is to find the download link for the full holdings if available, as it provides complete data.

        Options:
        1. Look for a XLS/CSV download link specifically for holdings. It is often an <a> tag with text exactly 'KOMPONENTEN DES ETFS HERUNTERLADEN' or similar, possibly with class 'm-download-button' or 'download-link'. If found, provide the best CSS selector, e.g., 'a:has-text("KOMPONENTEN DES ETFS HERUNTERLADEN")' or more specific if possible. Prefer download as it's the full list.
        2. If no download link is found, but there is an HTML table displaying holdings (like top holdings), extract the data from the table. Extract all available holdings. For each holding, get:
           - name: The name of the holding
           - sector: The sector (if available)
           - securityType: The asset class or type (if available)
           - weight: The weight percentage (as float)
           - isin: The ISIN code (if available)
           - currency: The currency (if available)
           - country: The country (if available)
           Use 'N/A' for missing fields.
        3. If neither a download link nor a holdings table is present, indicate none.

        Output strictly in JSON format with no additional text:
        - If option 1: {{"action": "download", "selector": "<css-selector>"}}
        - If option 2: {{"action": "extract", "holdings": [<list of dicts>]}}
        - If option 3: {{"action": "none"}}

        HTML content:
        {html_to_analyze}
        """
        
        print("🤖 Asking AI for the best extraction method...")
        response = await model.generate_content_async(prompt)
        response_text = response.text.strip().replace("```json", "").replace("```", "")
        
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing LLM response: {e}\nRaw response: {response.text}")
            return None
        
        holdings = []
        
        if data.get('action') == 'download':
            selector = data.get('selector')
            if not selector:
                raise ValueError("AI suggested download but did not provide a valid selector.")

            print(f"⏳ AI suggests downloading via selector: '{selector}'")
            try:
                async with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                    await page.locator(selector).first.click(force=True)
                
                download = await download_info.value
                
                if not os.path.exists(DOWNLOAD_DIRECTORY):
                    os.makedirs(DOWNLOAD_DIRECTORY)

                temp_path = os.path.join(DOWNLOAD_DIRECTORY, download.suggested_filename)
                await download.save_as(temp_path)
                print(f"✅ Download successful. File temporarily saved to: {temp_path}")
            except PlaywrightTimeoutError as e:
                print(f"⚠️ Download timed out: {e}. Falling back to HTML extraction.")
                # Fallback to extracting from HTML table
                top_holdings_table = soup.select_one("table.top-holdings") or soup.select_one("table")
                if top_holdings_table:
                    print("Found holdings table for fallback...")
                    rows = top_holdings_table.select("tbody tr")
                    for row in rows:
                        cells = row.select("td")
                        if len(cells) >= 4:
                            try:
                                name = cells[0].get_text(strip=True)
                                sector = cells[1].get_text(strip=True)
                                country = cells[2].get_text(strip=True)
                                weight_str = cells[3].get_text(strip=True).replace('%', '').replace(',', '.')
                                weight = float(weight_str)
                                holdings.append({
                                    "name": name,
                                    "sector": sector,
                                    "country": country,
                                    "weight": weight,
                                    "securityType": "N/A",
                                    "isin": "N/A",
                                    "currency": "N/A"
                                })
                            except (ValueError, AttributeError):
                                continue
                if not holdings:
                    print("⚠️ Fallback failed: No holdings table found.")
            except Exception as e:
                print(f"❌ Download failed: {e}")
                traceback.print_exc()

            if temp_path and os.path.exists(temp_path):
                try:
                    print("🔄 Processing downloaded Excel file with Pandas (using calamine engine)...")
                    df_temp = pd.read_excel(temp_path, header=None, engine="calamine")
                    
                    header_row_index = -1
                    for i, row in df_temp.iterrows():
                        row_str = ' '.join([str(v) for v in row.values if pd.notna(v)])
                        if 'ISIN' in row_str and ('Name' in row_str or 'Bezeichnung' in row_str):
                            header_row_index = i
                            break
                    
                    if header_row_index != -1:
                        df = pd.read_excel(temp_path, skiprows=header_row_index, engine="calamine")
                        print(f"✓ Found table header at row {header_row_index}.")
                        
                        df.columns = [str(col).strip() for col in df.columns]
                        
                        column_mapping = {
                            'Name': 'name', 'Bezeichnung': 'name', 'Währung': 'currency', 
                            'Currency': 'currency', 'Gewichtung': 'weight', 'Weight': 'weight', 
                            'Gewichtung (%)': 'weight', 'Sektor': 'sector', 'Sector': 'sector',
                            'Anlageklasse': 'securityType', 'Asset Class': 'securityType',
                            'Land': 'country', 'Country': 'country', 'ISIN': 'isin'
                        }
                        
                        df.rename(columns=lambda c: next((new for old, new in column_mapping.items() if old in c), c), inplace=True)
                        
                        if 'name' in df.columns and 'weight' in df.columns:
                            df['weight'] = pd.to_numeric(
                                df['weight'].astype(str).str.replace('%', '').str.replace(',', '.').str.strip(), 
                                errors='coerce'
                            )
                            df.dropna(subset=['weight', 'name'], inplace=True)
                            
                            for col in column_mapping.values():
                                if col not in df.columns: df[col] = 'N/A'
                            
                            df.fillna("N/A", inplace=True)
                            
                            holdings = df[list(column_mapping.values())].to_dict('records')
                            print(f"✅ Successfully processed {len(holdings)} holdings.")
                        else:
                            print(f"⚠️ Required columns not found. Found columns: {list(df.columns)}")
                    else:
                        print("⚠️ Could not find table header in the downloaded file.")
                except Exception as e:
                    print(f"❌ Error processing Excel file: {e}")
                    traceback.print_exc()
            else:
                print("❌ Download failed - no file was saved.")

        elif data.get('action') == 'extract':
            holdings = data.get('holdings', [])
            print(f"✅ AI extracted {len(holdings)} holdings from HTML table.")

        elif data.get('action') == 'none':
            print("⚠️ AI found no holdings data.")

        else:
            print("❌ Invalid action from AI.")

        return {"isin": etf_isin, "name": etf_name, "holdings": holdings}
    
    except Exception as e:
        print(f"❌ Error scraping page {url}: {e}")
        traceback.print_exc()
        return None
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
            print(f"✓ Cleaned up temporary file")
        await page.close()
        await context.close()


async def main():
    """Main function to orchestrate the scraping process."""
    try:
        urls = await get_etf_urls()
        if not urls:
            print("No ETFs found, exiting.")
            return

        final_json_output = {}
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=False,
                downloads_path=tempfile.gettempdir()
            )
            
            for i, url in enumerate(urls):
                print(f"\n--- Processing ETF {i + 1}/{len(urls)} ---")
                try:
                    scrape_result = await scrape_etf_page(browser, url)
                    if scrape_result and scrape_result.get("isin"):
                        final_json_output[scrape_result["isin"]] = scrape_result
                        print(f"📊 Processed '{scrape_result['name']}' ({len(scrape_result['holdings'])} holdings)")
                    else:
                        print(f"❌ Failed to process {url} or no data was returned.")
                except Exception:
                    print(f"❌ A critical error occurred while processing {url}:")
                    traceback.print_exc()

                if (i + 1) % BATCH_SIZE == 0 and (i + 1) < len(urls):
                    print(f"\n✅ Batch of {BATCH_SIZE} complete. Waiting {DELAY_BETWEEN_BATCHES_SECONDS} seconds...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)
            
            await browser.close()

        if final_json_output:
            if not os.path.exists(OUTPUT_DIRECTORY):
                os.makedirs(OUTPUT_DIRECTORY)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"amundi_etf_data_{timestamp}.json"
            save_path = os.path.join(OUTPUT_DIRECTORY, filename)
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(final_json_output, f, indent=2, ensure_ascii=False)
            print(f"\n✅ Done! Scraped {len(final_json_output)} ETFs and saved data to {save_path}.")
        else:
            print("\n❌ No ETFs were scraped successfully.")

    except Exception:
        print("\n--- A fatal error occurred in the main process ---")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())