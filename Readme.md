# ETF Data Scraper

A collection of Python scripts designed to scrape and process ETF data from various providers. This project utilizes Firecrawl for scraping and outputs the data in a clean JSON format.

## ✨ Features

- **Multiple Providers:** Scrapes data from Amundi, iShares, Vanguard, and Xtrackers.
- **Combined Script:** Includes a `Combined.py` script to run all scrapers at once.
- **JSON Output:** Saves processed data in a structured JSON format in the `output/` directory.
- **Easy to Use:** Simple setup and execution process.

## 🚀 Getting Started

### Prerequisites

- Python 3.12
- An API key from [Firecrawl](https://www.firecrawl.dev/)

### 1. Clone the Repository
   ```sh
git clone https://github.com/BenLatifAyoub/ETFS.git
cd Scrap
   ```


### 2. **Install dependencies:**
   ```sh
pip install -r requirements.txt
playwright install
   ```

### 3. **Add your API key:**  
   All scripts require an API key.  
   Open the script you want to run and find the line:
   ```python
   FIRECRAWL_API_KEY = "FIRECRAWL_API_KEY"
   ```
   Replace `"FIRECRAWL_API_KEY"` with your Firecrawl key.

### 4. **Run a script:**
   ```sh
   python Combined.py
   ```
   Or run any of the provider-specific scripts.

### Folder Structure

- `Combined.py`, `amundietf.py`, `Ishare.py`, `vanguard.py`, `Xtrackers.py`: Main scraping scripts for different ETF providers.
- `requirements.txt`: Python dependencies.
- `output/`: Processed ETF data in JSON format.

### Output

- Scraped data is saved in `output/` as JSON files.

---