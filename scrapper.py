import os
import aiohttp
import asyncio
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Database setup 
def setup_database():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",  
        password="",  
        database="dsscrapping"  
    )
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS papers (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title TEXT,
            authors TEXT,
            url TEXT UNIQUE,
            file_path TEXT,
            year TEXT
        )
    ''')
    conn.commit()
    conn.close()

async def save_to_db(title, authors, url, file_path, year):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",  # Change if needed
        password="",  # Add password if set in XAMPP
        database="dsscrapping"
    )
    cursor = conn.cursor()
    cursor.execute('''
        INSERT IGNORE INTO papers (title, authors, url, file_path, year)
        VALUES (%s, %s, %s, %s, %s)
    ''', (title, authors, url, file_path, year))
    conn.commit()
    conn.close()

async def download_file(session, url, folder, title, authors, year):
    """
    Download a file from a given URL and save it to a folder,
    then store metadata in the database.
    """
    try:
        async with session.get(url, ssl=False) as response:
            response.raise_for_status()
            file_name = os.path.basename(url)
            # Use absolute path instead of relative
            file_path = os.path.abspath(os.path.join(folder, file_name))
            # Read the entire content and write it out
            content = await response.read()
            with open(file_path, 'wb') as file:
                file.write(content)
            print(f"Downloaded: {file_name} to {file_path}")
            await save_to_db(title, authors, url, file_path, year)
    except Exception as e:
        print(f"Failed to download {url}: {e}")

async def extract_links(session, url):
    """
    Extract all links from a webpage.
    """
    try:
        async with session.get(url, ssl=False) as response:
            response.raise_for_status()
            html = await response.text()
        soup = BeautifulSoup(html, 'html.parser')
        links = [urljoin(url, a['href']) for a in soup.find_all('a', href=True)]
        return links, soup
    except Exception as e:
        print(f"Error extracting links from {url}: {e}")
        return [], None

async def scrape_paper_detail(session, url, folder, title, authors, visited):
    """
    Scrape the paper detail page to find the PDF link and download the file.
    """
    if url in visited:
        return
    visited.add(url)
    print(f"Scraping paper detail: {url}")
    links, soup = await extract_links(session, url)
    if not soup:
        return
    pdf_link = None
    # Look for any link that contains '.pdf' or '.txt' in its href
    for a in soup.find_all('a', href=True):
        href = a['href']
        if '.pdf' in href.lower() or '.txt' in href.lower():
            pdf_link = urljoin(url, href)
            break
    if pdf_link:
        year = "Unknown Year"  # Modify if you can extract the year from the detail page
        await download_file(session, pdf_link, folder, title, authors, year)
    else:
        print(f"No PDF found at {url}")

async def scrape_papers(session, url, folder, depth, max_depth, visited=None, year_filter=None):
    """
    Recursively scrape pages to eventually download papers.
    - Depth 0: Main page (typically contains year links).
    - Depth 1: Paper list page (each paper in a <li> element).
    """
    if visited is None:
        visited = set()
    if url in visited or depth > max_depth:
        return
    visited.add(url)
    print(f"Scraping: {url} (Depth: {depth})")
    links, soup = await extract_links(session, url)
    if not soup:
        return

    # Stage 1: If on the main page, follow links matching the desired years.
    if depth == 0:
        for link in links:
            if any(year in link for year in year_filter):
                await scrape_papers(session, link, folder, depth + 1, max_depth, visited, year_filter)
    # Stage 2: On the paper list page, look for paper entries in <li> tags.
    elif depth == 1:
        li_items = soup.find_all('li')
        for li in li_items:
            a_tag = li.find('a')
            i_tag = li.find('i')
            if a_tag:
                paper_detail_url = urljoin(url, a_tag['href'])
                title = a_tag.text.strip()
                authors = i_tag.text.strip() if i_tag else "Unknown Authors"
                # Now, go to the paper detail page to get the PDF link.
                await scrape_paper_detail(session, paper_detail_url, folder, title, authors, visited)
    else:
        # Further recursion if needed.
        for link in links:
            if link not in visited:
                await scrape_papers(session, link, folder, depth + 1, max_depth, visited, year_filter)

async def main():
    setup_database()
    base_url = "https://papers.nips.cc/"  # Main website
    download_folder = "downloaded_papers"
    os.makedirs(download_folder, exist_ok=True)
    year_filter = [str(year) for year in range(2019, 2021)]  # Adjust years as needed
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        # We set max_depth=2: main page -> paper list -> paper detail.
        await scrape_papers(session, base_url, download_folder, depth=0, max_depth=2, year_filter=year_filter)

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
        task = loop.create_task(main())
        loop.run_until_complete(task)
    except RuntimeError:
        asyncio.run(main())
