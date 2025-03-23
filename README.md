# Florence Press Scraper

A tool for scraping, extracting, and storing press releases from the Florence Municipality website.

## Technologies

- **Python 3.12**: Core programming language
- **BeautifulSoup4**: HTML parsing and data extraction
- **Requests**: HTTP requests handling
- **SQLite**: Local database storage

## Project Structure

- `00_extract_links.py`: Scrapes press release metadata (URLs, titles, dates)
- `01_extract_content.py`: Downloads and extracts the content of press releases

## How to Run

1. **Setup the environment**:
   ```bash
   # Create a virtual environment
   python -m venv .venv
   
   # Activate the virtual environment
   # On Windows:
   .venv\Scripts\activate
   # On macOS/Linux:
   source .venv/bin/activate
   
   # Install dependencies
   pip install beautifulsoup4 requests

   # OR, if you are using uv, just run 
   uv sync
   ```

2. **Run the scripts in sequence**:
   ```bash
   # First, extract all links (creates the database and populates metadata)
   python 00_extract_links.py
   
   # Then, extract content for all press releases
   python 01_extract_content.py
   ```

## Database

The scraped data is stored in a SQLite database (`press_releases.db`) with the following structure:
- `id`: Primary key (comunicato ID)
- `url`: Full URL to the press release
- `title`: Title of the press release
- `date`: Publication date
- `content`: Full text content of the press release

## Notes

- The full scrape includes over 7,000 pages and may take several hours to complete
- To test with fewer pages, modify the `start_page` and `end_page` variables in `extract_links.py`
