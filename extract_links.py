import os
import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime

def extract_communication_data(html_content, base_url):
    """
    Extract communication links, titles, and dates from HTML content using BeautifulSoup.
    
    Args:
        html_content (str): HTML content of the page
        base_url (str): Base URL for creating absolute URLs
        
    Returns:
        list: List of dictionaries containing link, title, and date
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
            
            # Extract title and link
            title_elem = item.find('span', class_='views-field-field-titolo')
            if title_elem and title_elem.find('a'):
                link_elem = title_elem.find('a')
                relative_link = link_elem['href']
                absolute_link = urljoin(base_url, relative_link)
                title = link_elem.text.strip()
                
                communications.append({
                    'link': absolute_link,
                    'title': title,
                    'date': date_formatted
                })
    
    return communications

def scrape_all_pages(start_page=0, end_page=7226, base_url='https://press.comune.fi.it'):
    """
    Scrape all pages from start_page to end_page.
    
    Args:
        start_page (int): Starting page number
        end_page (int): Ending page number
        base_url (str): Base URL of the website
        
    Returns:
        list: List of all communication data
    """
    all_data = []
    
    # Create a session for better performance
    session = requests.Session()
    
    for page_num in range(start_page, end_page + 1):
        url = f"{base_url}/?page={page_num}"
        
        try:
            print(f"Scraping page {page_num} of {end_page}...")
            response = session.get(url)
            
            if response.status_code == 200:
                page_data = extract_communication_data(response.text, base_url)
                all_data.extend(page_data)
                print(f"Found {len(page_data)} press releases on page {page_num}")
                
                # Add a small delay to avoid overwhelming the server
                time.sleep(1)
            else:
                print(f"Failed to retrieve page {page_num}. Status code: {response.status_code}")
                
        except Exception as e:
            print(f"Error processing page {page_num}: {str(e)}")
            
    return all_data

def save_to_csv(data, output_file='press_releases.csv'):
    """
    Save communication data to a CSV file.
    
    Args:
        data (list): List of dictionaries containing link, title, and date
        output_file (str): Path to the output CSV file
    """
    if not data:
        print("No data to save.")
        return
    
    # Ensure the data directory exists
    os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
    
    # Write data to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['link', 'title', 'date']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for item in data:
            writer.writerow(item)
    
    print(f"Data saved to {output_file}")

def main():
    # Define parameters
    output_file = os.path.join(os.getcwd(), 'press_releases.csv')
    
    # For testing with fewer pages, uncomment and modify these lines:
    # start_page = 0
    # end_page = 5
    
    # For the full dataset:
    start_page = 0
    end_page = 5
    
    # Scrape all pages
    all_data = scrape_all_pages(start_page, end_page)
    
    # Save data to CSV
    save_to_csv(all_data, output_file)
    
    print(f"\nTotal press releases extracted: {len(all_data)}")

if __name__ == "__main__":
    main()
