#!/usr/bin/env python3
"""
DWS Xtrackers ETF Scraper (AI-Enhanced Version) - Final Version
-------------------------------------------------
Uses AI (Gemini 1.5 Flash) to analyze a focused HTML snippet from the <main> tag to decide on the best data extraction method.
This version sends only the five largest top-level <div> elements from the <main> tag to the AI for analysis.

WARNING: Hardcoding API keys in scripts is insecure and not recommended.
Use environment variables instead for production/security.
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
TOTAL_ETFS_TO_SCRAPE = 2
BATCH_SIZE = 1
DELAY_BETWEEN_BATCHES_SECONDS = 60
DOWNLOAD_TIMEOUT_MS = 60000
GEMINI_MODEL = "gemini-2.0-flash-lite"  # Using a valid and powerful model
OUTPUT_DIRECTORY = "etf_data"
DOWNLOAD_DIRECTORY = "downloads"
# --- END CONFIGURATION ---

# IMPORTANT: Replace with your actual Gemini API key or use environment variables
try:
    # It's better to load from environment variables: os.environ.get("GEMINI_API_KEY")
    genai.configure(api_key="AIzaSyACQwN6IEzGeB59hUvVhGwpWJQiaHr5q9k") # <-- PUT YOUR API KEY HERE
except (ValueError, TypeError) as e:
    print(f"API Key Error: {e}. Please ensure you have set your Gemini API key.")
    exit()

model = genai.GenerativeModel(GEMINI_MODEL)
DWS_URL = "https://etf.dws.com/de-de/produktfinder/"

async def handle_consent_flow(page):
    """Handles the consent and disclaimer pop-ups sequentially."""
    print("🔎 Handling consent process...")
    try:
        cookie_selector = 'button:has-text("Accept all cookies")'
        print(" - Waiting for cookie banner...")
        await page.locator(cookie_selector).click(timeout=10000)
        print(" - ✅ Clicked 'Accept all cookies'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except PlaywrightTimeoutError:
        print(" - ✓ Cookie banner not found (Step 1 skipped).")
    try:
        accept_selector = 'button:has-text("Akzeptieren & weiter")'
        print(" - Waiting for disclaimer pop-up...")
        await page.locator(accept_selector).click(timeout=10000)
        print(" - ✅ Clicked 'Akzeptieren & weiter'.")
        await page.wait_for_load_state('networkidle', timeout=15000)
    except PlaywrightTimeoutError:
        print(" - ✓ Disclaimer pop-up not found (Step 2 skipped).")




print("✅ Consent flow complete.")
async def get_etf_urls():
    """Navigate DWS ETF finder page and collect product page URLs."""
    etf_urls = []
    print("🚀 Launching browser to collect ETF URLs...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=50)
        page = await browser.new_page()
        await page.goto(DWS_URL, timeout=90000, wait_until="domcontentloaded")
        
        await handle_consent_flow(page)
        
        print("⏳ Waiting for ETF list to load...")
        
        # --- MODIFICATION START ---
        # Define the specific link selector we are looking for.
        link_selector = 'td a.d-base-link[href*="/de-de/LU"]'
        
        try:
            # Wait for the FIRST link of this type to appear. This confirms the table data has been loaded.
            print("   - Waiting for ETF data rows to render...")
            await page.wait_for_selector(link_selector, timeout=60000)
            print("✅ ETF data rows loaded.")
        except PlaywrightTimeoutError:
            print("❌ Timed out waiting for ETF data rows to appear. The page structure may have changed.")
            await browser.close()
            return []
        # --- MODIFICATION END ---

        # Now that we know the links are present, we can safely locate them all.
        links = await page.locator(link_selector).all()
        
        unique_urls = set()
        for link in links:
            href = await link.get_attribute("href")
            if href:
                full_url = urljoin(DWS_URL, href)
                unique_urls.add(full_url)
        
        etf_urls = list(unique_urls)[:TOTAL_ETFS_TO_SCRAPE]
        await browser.close()

    print(f"✅ Collected {len(etf_urls)} unique ETF URLs.")
    return etf_urls
async def scrape_etf_page(browser, url):
    """Uses AI to analyze a focused part of the page and robustly handle downloads or table extraction."""
    context = await browser.new_context(accept_downloads=True)
    page = await context.new_page()
    temp_path = None
    
    try:
        page.set_default_timeout(60000)
        print(f"Navigating to product page: {url}...")
        await page.goto(url, wait_until="domcontentloaded")
        
        await handle_consent_flow(page)

        name_selector = "h1#product-header-title"
        await page.wait_for_selector(name_selector)
        etf_name = (await page.locator(name_selector).first.inner_text()).strip()
        
        isin_locator = page.locator("div.product-header__identifier__row:has(h2:has-text('ISIN:')) strong").first
        etf_isin = await isin_locator.inner_text()
        
        print(f"📄 Found ETF: {etf_name} ({etf_isin})")

        html_content = await page.content()
        
        soup = BeautifulSoup(html_content, "html.parser")
        main_container = soup.find("main")
        
        if main_container:
            child_divs = main_container.find_all('div', recursive=False)
            sorted_divs = sorted(child_divs, key=lambda d: len(str(d)), reverse=True)
            top_five_divs = sorted_divs[2:4]
            print(f"🔍 Found {len(top_five_divs)} top-level <div> elements in <main> for analysis.")
            html_to_analyze = "".join([str(d) for d in top_five_divs])
            
            if html_to_analyze:
                print(f"🔎 Using the top {len(top_five_divs)} largest <div>s from <main> for AI analysis (length: {len(html_to_analyze)} chars).")
            else:
                html_to_analyze = str(main_container)
                print(f"⚠️ No direct <div> children found in <main>. Using the full <main> tag instead (length: {len(html_to_analyze)} chars).")
        else:
            html_to_analyze = html_content
            print("⚠️ Could not find <main> tag, analyzing full page as a fallback.")

        prompt = f"""
        You are an AI assistant analyzing the HTML of a DWS Xtrackers ETF product page to extract holdings data.

        Your primary goal is to find the download link for the full holdings XLS file. This is the most reliable source.
        


        Options:
        1.  Look for an Excel download link. It is an <a> tag, often with text like "Komplette Wertpapierliste herunterladen". If you find it, provide its CSS selector. The href might contain '/excel/index/constituent/'. This is the preferred method.
        2.  If no download link is found, but there is an HTML table under a heading like "Top 10" or "Wertpapiere des Wertpapierkorbs", extract the data from that table. For each holding, get:
            - name: The name of the holding (from column 'Name' or 'Bezeichnung')
            - isin: The ISIN code (from column 'ISIN')
            - weight: The weight percentage as a float (from column 'Gewichtung %' or 'Gewichtung')
            - securityType: The asset class (from column 'Anlageklasse')
            - country: The country (from column 'Land')
            Use 'N/A' for missing fields.
        3.  If neither a download link nor a holdings table is present, indicate 'none'.

        Output strictly in JSON format with no additional text or markdown:
        - If option 1: {{"action": "download", "selector": "your-css-selector-here"}}
        - If option 2: {{"action": "extract", "holdings": [<list of dicts>]}}
        - If option 3: {{"action": "none"}}

        HTML content snippet to analyze:
        {html_to_analyze}
        """
        
        print("🤖 Asking AI for the best extraction method...")
        response = await model.generate_content_async(prompt)
        # A more robust way to clean the response
        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing LLM response: {e}\nRaw response: {response.text}")
            return None
        
        holdings = []
        
        if data.get('action') == 'download':
            selector = data.get('selector')
            if not selector:
                # Fallback selector if AI fails to provide one
                selector = 'a[href*="/excel/index/constituent/"], a[href*="/excel/product/constituent/"]'
                print(f"   - AI did not provide a selector. Using fallback: '{selector}'")

            print(f"⏳ AI suggests downloading via selector: '{selector}'")
            try:
                async with page.expect_download(timeout=DOWNLOAD_TIMEOUT_MS) as download_info:
                    await page.locator(selector).first.click(force=True, timeout=15000)
                
                download = await download_info.value
                
                if not os.path.exists(DOWNLOAD_DIRECTORY):
                    os.makedirs(DOWNLOAD_DIRECTORY)

                safe_isin = etf_isin.replace(" ", "_")
                timestamp = datetime.now().strftime("%Y%m%d")
                filename = f"{safe_isin}_{timestamp}.xlsx"

                temp_path = os.path.join(DOWNLOAD_DIRECTORY, filename)
                await download.save_as(temp_path)
                print(f"✅ Download successful. File temporarily saved to: {temp_path}")
            
            except Exception as e:
                print(f"❌ Download failed: {e}")
                traceback.print_exc()

            if temp_path and os.path.exists(temp_path):
                try:
                    print("🔄 Processing downloaded Excel file...")
                    df_temp = pd.read_excel(temp_path, header=None, engine="openpyxl")
                    header_row_index = -1
                    
                    # --- MODIFICATION START: More robust header detection ---
                    header_keywords = {'isin', 'name', 'country', 'currency', 'exchange', 'bezeichnung', 'land', 'währung', 'rating'}
                    for i, row in df_temp.iterrows():
                        # Clean the row content and convert to a list of lowercase strings
                        row_content = [str(cell).strip().lower() for cell in row.values if pd.notna(cell)]
                        # Count how many of our keywords appear in the row's content
                        matches = sum(1 for keyword in header_keywords if keyword in row_content)
                        # If we find 3 or more matches, we are confident this is the header
                        if matches >= 3:
                            header_row_index = i
                            print(f"✓ Found potential table header at row {header_row_index} with {matches} keyword matches.")
                            break
                    # --- MODIFICATION END ---
                    
                    if header_row_index != -1:
                        df = pd.read_excel(temp_path, skiprows=header_row_index, engine="openpyxl")
                        df.columns = [str(col).strip().lower() for col in df.columns]
                        
                        column_mapping = {
                            'name': 'name', 'bezeichnung': 'name', 
                            'währung': 'currency', 'currency': 'currency', 
                            'gewichtung %': 'weight', 'gewichtung': 'weight', 'weighting': 'weight',
                            'sektor': 'sector', 'industry classification': 'sector',
                            'anlageklasse': 'securityType', 'asset class': 'securityType', 'type of security': 'securityType',
                            'land': 'country', 'country': 'country', 
                            'isin': 'isin'
                        }
                        
                        df.rename(columns=lambda c: next((new for old, new in column_mapping.items() if old in c), c), inplace=True)
                        
                        # --- MODIFICATION START: Make 'weight' column optional ---
                        if 'name' in df.columns and 'isin' in df.columns:
                            # Process weight column only if it exists
                            if 'weight' in df.columns:
                                df['weight'] = pd.to_numeric(
                                    df['weight'].astype(str).str.replace('%', '').str.replace(',', '.').str.strip(), 
                                    errors='coerce'
                                )
                            
                            # Drop rows only if essential columns are missing
                            df.dropna(subset=['name', 'isin'], inplace=True)
                            
                            # Ensure all potential target columns exist, creating them if necessary
                            target_cols = list(set(column_mapping.values()))
                            for col in target_cols:
                                if col not in df.columns:
                                    df[col] = 'N/A'
                            
                            df.fillna("N/A", inplace=True)
                            
                            # Ensure we only select columns that actually exist now
                            final_cols = [col for col in target_cols if col in df.columns]
                            holdings = df[final_cols].to_dict('records')
                            print(f"✅ Successfully processed {len(holdings)} holdings from file.")
                        # --- MODIFICATION END ---
                        else:
                            print(f"⚠️ Required columns ('name', 'isin') not found. Found columns: {list(df.columns)}")
                    else:
                        print("⚠️ Could not find a valid table header in the downloaded file.")
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
            print(f"❌ Invalid action from AI: {data.get('action')}")

        return {"isin": etf_isin, "name": etf_name, "holdings": holdings}
    
    except Exception as e:
        print(f"❌ An unhandled error occurred while scraping {url}: {e}")
        traceback.print_exc()
        return None
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
            print("✓ Cleaned up temporary file.")
        await page.close()
        await context.close()
async def main():
    """Main function to orchestrate the scraping process."""
    try:
        urls = await get_etf_urls()
        if not urls:
            print("No ETF URLs collected, exiting.")
            return

        final_json_output = {}
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            
            all_urls = urls
            total_urls = len(all_urls)
            
            for i in range(0, total_urls, BATCH_SIZE):
                batch_urls = all_urls[i:i + BATCH_SIZE]
                print(f"\n--- Processing Batch {i//BATCH_SIZE + 1}/{(total_urls + BATCH_SIZE - 1)//BATCH_SIZE} ---")

                tasks = [scrape_etf_page(browser, url) for url in batch_urls]
                results = await asyncio.gather(*tasks)

                for result in results:
                    if result and result.get("isin"):
                        final_json_output[result["isin"]] = result
                        print(f"📊 Processed '{result['name']}' ({len(result['holdings'])} holdings)")
                    else:
                        print(f"❌ Failed to process a URL in the batch or no data was returned.")
                
                if i + BATCH_SIZE < total_urls:
                    print(f"\n✅ Batch complete. Waiting {DELAY_BETWEEN_BATCHES_SECONDS} seconds...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES_SECONDS)
            
            await browser.close()

        if final_json_output:
            if not os.path.exists(OUTPUT_DIRECTORY):
                os.makedirs(OUTPUT_DIRECTORY)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dws_xtrackers_data_{timestamp}.json"
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
    # Ensure the downloads directory exists before starting
    if not os.path.exists(DOWNLOAD_DIRECTORY):
        os.makedirs(DOWNLOAD_DIRECTORY)
    asyncio.run(main())