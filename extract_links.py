import os
import csv
import time
import requests
import sqlite3
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

def extract_communication_data(html_content, base_url):
    """
    Extract communication urls, titles, and dates from HTML content using BeautifulSoup.
    
    Args:
        html_content (str): HTML content of the page
        base_url (str): Base URL for creating absolute URLs
        
    Returns:
        list: List of dictionaries containing url, title, and date
    """
    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all list items in the view content
    communications = []
    
    # Find the view-content div that contains the list of press releases
    view_content = soup.find('div', class_='view-content')
    if view_content:
        # Find all list items that represent press releases
        items = view_content.find_all('li', class_='views-row')
        
        for item in items:
            # Extract date
            date_elem = item.find('span', class_='views-field-field-data-comunicato')
            date_text = date_elem.find('time')['datetime'] if date_elem and date_elem.find('time') else ''
            
            # Format date if it exists
            if date_text:
                try:
                    date_obj = datetime.strptime(date_text, '%Y-%m-%dT%H:%M:%SZ')
                    date_formatted = date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    date_formatted = date_text
            else:
                date_formatted = ''
            
            # Extract title and url
            title_elem = item.find('span', class_='views-field-field-titolo')
            if title_elem and title_elem.find('a'):
                url_elem = title_elem.find('a')
                relative_url = url_elem['href']
                absolute_url = urljoin(base_url, relative_url)
                title = url_elem.text.strip()
                
                communications.append({
                    'url': absolute_url,
                    'title': title,
                    'date': date_formatted
                })
    
    return communications

def extract_comunicato_id(url):
    """
    Extract the comunicato ID from the URL.
    
    Args:
        url (str): URL string that may contain '/comunicato/123456'
        
    Returns:
        int or None: The extracted ID as an integer, or None if not found
    """
    match = re.search(r'/comunicato/(\d+)', url)
    if match:
        return int(match.group(1))
    return None

def initialize_database(db_file='press_releases.db'):
    """
    Initialize the SQLite database if it doesn't exist.
    
    Args:
        db_file (str): Path to the SQLite database file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure the data directory exists
        os.makedirs(os.path.dirname(os.path.abspath(db_file)), exist_ok=True)
        
        # Connect to SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS press_releases (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT NOT NULL,
            date TEXT,
            content TEXT
        )
        ''')
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        print(f"Database initialized at {db_file}")
        return True
    except Exception as e:
        print(f"Error initializing database: {str(e)}")
        return False

def save_page_to_sqlite(data, conn=None, db_file='press_releases.db'):
    """
    Save a single page of communication data to a SQLite database.
    
    Args:
        data (list): List of dictionaries containing url, title, and date
        conn (sqlite3.Connection, optional): Existing database connection
        db_file (str): Path to the SQLite database file (used only if conn is None)
    
    Returns:
        int: Number of records successfully saved
    """
    if not data:
        return 0
    
    close_conn = False
    try:
        # Use provided connection or create a new one
        if conn is None:
            conn = sqlite3.connect(db_file)
            close_conn = True
        
        cursor = conn.cursor()
        
        # Insert data
        saved_count = 0
        for item in data:
            comunicato_id = extract_comunicato_id(item['url'])
            if comunicato_id:
                cursor.execute(
                    "INSERT OR IGNORE INTO press_releases (id, url, title, date, content) VALUES (?, ?, ?, ?, ?)",
                    (comunicato_id, item['url'], item['title'], item['date'], None)
                )
                saved_count += 1
        
        # Commit changes
        conn.commit()
        
        # Close connection only if we created it
        if close_conn:
            conn.close()
        
        return saved_count
    except Exception as e:
        print(f"Error saving to database: {str(e)}")
        # Close connection only if we created it and an error occurred
        if close_conn and conn:
            try:
                conn.close()
            except:
                pass
        return 0

def scrape_and_save_pages(start_page=0, end_page=7226, base_url='https://press.comune.fi.it', db_file='press_releases.db'):
    """
    Scrape all pages from start_page to end_page and save each page immediately to the database.
    
    Args:
        start_page (int): Starting page number
        end_page (int): Ending page number
        base_url (str): Base URL of the website
        db_file (str): Path to the SQLite database file
        
    Returns:
        int: Total number of records saved
    """
    total_saved = 0
    
    # Create a session for better performance
    session = requests.Session()
    
    # Create a single database connection to reuse
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        
        for page_num in range(start_page, end_page + 1):
            url = f"{base_url}/?page={page_num}"
            
            try:
                print(f"Scraping page {page_num} of {end_page}...")
                response = session.get(url)
                
                if response.status_code == 200:
                    page_data = extract_communication_data(response.text, base_url)
                    print(f"Found {len(page_data)} press releases on page {page_num}")
                    
                    # Save the current page data using the existing connection
                    saved_count = save_page_to_sqlite(page_data, conn=conn)
                    total_saved += saved_count
                    print(f"Saved {saved_count} press releases from page {page_num} to database")
                    
                    # Add a small delay to avoid overwhelming the server
                    time.sleep(2)
                else:
                    print(f"Failed to retrieve page {page_num}. Status code: {response.status_code}")
                    
            except Exception as e:
                print(f"Error processing page {page_num}: {str(e)}")
                # Continue with the next page
                
    except Exception as e:
        print(f"Database connection error: {str(e)}")
    finally:
        # Ensure the connection is closed properly
        if conn:
            try:
                conn.close()
                print("Database connection closed.")
            except Exception as e:
                print(f"Error closing database connection: {str(e)}")
    
    return total_saved

def save_to_csv(data, output_file='press_releases.csv'):
    """
    Save communication data to a CSV file.
    
    Args:
        data (list): List of dictionaries containing url, title, and date
        output_file (str): Path to the output CSV file
    """
    if not data:
        print("No data to save.")
        return
    
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # Write data to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['url', 'title', 'date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in data:
            writer.writerow(item)
    
    print(f"Data saved to {output_file}")

def main():
    # Define parameters
    db_file = os.path.join(os.getcwd(), 'press_releases.db')
    
    # For testing with fewer pages, uncomment and modify these lines:
    # start_page = 0
    # end_page = 5
    
    # For the full dataset:
    start_page = 0
    end_page = 7226
    
    # Initialize the database first
    if initialize_database(db_file):
        # Scrape pages and save data incrementally
        total_saved = scrape_and_save_pages(start_page, end_page, db_file=db_file)
        print(f"\nTotal press releases saved to database: {total_saved}")
        
        # You can uncomment the following lines to immediately extract content after scraping links
        # from extract_content import update_press_release_content
        # print("\nNow updating content for press releases with NULL content...")
        # updated_count = update_press_release_content(db_file)
        # print(f"Total press releases with updated content: {updated_count}")
    else:
        print("Failed to initialize database. Exiting.")

if __name__ == "__main__":
    main()
