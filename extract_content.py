import os
import time
import sqlite3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def extract_page_content(url, max_retries=3, timeout=10):
    """
    Extract content from a press release page.
    
    Args:
        url (str): URL of the press release page
        max_retries (int): Maximum number of retry attempts
        timeout (int): Request timeout in seconds
        
    Returns:
        str or None: Extracted content as text, or None if extraction failed
    """
    session = requests.Session()
    retries = 0
    
    while retries < max_retries:
        try:
            print(f"Fetching content from: {url}")
            response = session.get(url, timeout=timeout)
            
            if response.status_code == 200:
                # Parse HTML with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find the view-content div that contains the press release content
                view_content = soup.find('div', class_='view-content')
                
                if view_content:
                    # Extract all text content, preserving some structure
                    content_text = view_content.get_text(" ", strip=True)
                    return content_text
                else:
                    print(f"Warning: No view-content div found at {url}")
                    return None
            else:
                print(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
                retries += 1
                time.sleep(2)  # Wait before retrying
                
        except Exception as e:
            print(f"Error fetching content from {url}: {str(e)}")
            retries += 1
            time.sleep(2)  # Wait before retrying
    
    print(f"Failed to retrieve content after {max_retries} attempts from {url}")
    return None

def update_press_release_content(db_file='press_releases.db', limit=None):
    """
    Update press release content for entries with NULL content.
    
    Args:
        db_file (str): Path to the SQLite database file
        limit (int, optional): Maximum number of entries to process
        
    Returns:
        int: Number of entries successfully updated
    """
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        batch_size = 5  # Process 5 entries at a time
        updated_count = 0
        total_processed = 0
        
        while True:
            # Get entries with NULL content without using OFFSET
            # Always get the first batch_size rows where content is NULL
            if limit and total_processed + batch_size > limit:
                # Adjust batch size for the last batch if we're approaching the limit
                current_batch_size = limit - total_processed
            else:
                current_batch_size = batch_size
                
            cursor.execute(
                "SELECT id, url FROM press_releases WHERE content IS NULL LIMIT ?", 
                (current_batch_size,)
            )
            
            entries = cursor.fetchall()
            print(f"Processing batch of {len(entries)} entries")
            
            if not entries:
                break  # No more entries to process
                
            for release_id, url in entries:
                # Extract content
                content = extract_page_content(url)
                
                if content:
                    # Update the database
                    cursor.execute(
                        "UPDATE press_releases SET content = ? WHERE id = ?",
                        (content, release_id)
                    )
                    conn.commit()
                    updated_count += 1
                    print(f"Updated content for ID {release_id}")
                else:
                    print(f"Could not extract content for ID {release_id}")
                
                # Small delay to avoid hammering the server
                time.sleep(1)
                
                total_processed += 1
                
                # Break if we've reached the specified limit
                if limit and total_processed >= limit:
                    break
            
            # Break if we've reached the specified limit
            if limit and total_processed >= limit:
                break
        
        print(f"Processed {total_processed} entries in total")
        conn.close()
        return updated_count
        
    except Exception as e:
        print(f"Error updating press release content: {str(e)}")
        if 'conn' in locals() and conn:
            conn.close()
        return 0

def main():
    # Define database file path
    db_file = os.path.join(os.getcwd(), 'press_releases.db')
    
    # For testing, limit to a small number of entries first
    # limit = 10
    
    # For processing all NULL entries
    limit = None
    
    # Update press release content
    updated_count = update_press_release_content(db_file, limit)
    print(f"\nTotal press releases updated: {updated_count}")

if __name__ == "__main__":
    main()
