"""
RimTours Website Data Scraper and Cleaner
Main script for extracting and processing tour data
"""
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import os
import json
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scraping.log'),
        logging.StreamHandler()
    ]
)

class RimToursDataScraper:
    def __init__(self):
        self.base_url = "https://rimtours.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Create data directories if they don't exist
        os.makedirs("data/raw", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def scrape_tour_pages(self, tour_urls):
        """
        Scrape individual tour pages to extract detailed information
        """
        logging.info(f"Starting to scrape {len(tour_urls)} tour pages...")
        
        tour_data = []
        
        for i, url in enumerate(tour_urls):
            try:
                logging.info(f"Scraping {url} ({i+1}/{len(tour_urls)})")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract tour information
                tour_info = self.extract_tour_info(soup, url)
                tour_data.append(tour_info)
                
                # Save raw HTML for reference
                with open(f"data/raw/tour_page_{i+1}.html", "w", encoding="utf-8") as f:
                    f.write(str(soup))
                
                # Add delay to be respectful to the server
                import time
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Error scraping {url}: {str(e)}")
                continue
        
        return tour_data

    def extract_tour_info(self, soup, url):
        """
        Extract relevant information from tour page
        """
        # Extract title
        title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'.*title.*|.*name.*', re.I))
        title = title_elem.get_text(strip=True) if title_elem else "Unknown Tour"
        
        # Extract subtitle (often in a specific element)
        subtitle_elem = soup.find(class_=re.compile(r'.*subtitle.*|.*tagline.*', re.I))
        subtitle = subtitle_elem.get_text(strip=True) if subtitle_elem else ""
        
        # Extract description
        desc_elem = soup.find('div', class_=re.compile(r'.*description.*|.*content.*', re.I))
        if not desc_elem:
            desc_elem = soup.find('div', id=re.compile(r'.*description.*|.*content.*', re.I))
        if not desc_elem:
            # Try to find main content area
            desc_elem = soup.find('div', class_=re.compile(r'.*entry-content.*|.*main-content.*', re.I))
        
        description = desc_elem.get_text(strip=True) if desc_elem else ""
        
        # Extract images
        images = []
        img_tags = soup.find_all('img')
        for img in img_tags:
            src = img.get('src') or img.get('data-src')
            if src and ('tour' in src.lower() or 'gallery' in src.lower() or 'thumb' in src.lower()):
                images.append(src)
        
        # Extract pricing information (looking for price-related classes)
        price_elements = soup.find_all(class_=re.compile(r'.*price.*|.*cost.*|.*rate.*', re.I))
        prices = [elem.get_text(strip=True) for elem in price_elements if elem.get_text(strip=True)]
        
        # Extract other relevant information
        # Find elements with tour-specific classes
        region_elem = soup.find(class_=re.compile(r'.*region.*', re.I))
        region = region_elem.get_text(strip=True) if region_elem else ""
        
        skill_elem = soup.find(class_=re.compile(r'.*skill.*|.*level.*', re.I))
        skill = skill_elem.get_text(strip=True) if skill_elem else ""
        
        season_elem = soup.find(class_=re.compile(r'.*season.*', re.I))
        season = season_elem.get_text(strip=True) if season_elem else ""
        
        # Extract duration if available
        duration_elem = soup.find(class_=re.compile(r'.*duration.*|.*length.*', re.I))
        duration = duration_elem.get_text(strip=True) if duration_elem else ""
        
        # Extract depart info
        depart_elem = soup.find(class_=re.compile(r'.*depart.*|.*location.*', re.I))
        depart = depart_elem.get_text(strip=True) if depart_elem else ""
        
        # Extract distance
        distance_elem = soup.find(class_=re.compile(r'.*distance.*|.*mile.*', re.I))
        distance = distance_elem.get_text(strip=True) if distance_elem else ""
        
        # Extract special notes
        notes_elem = soup.find(class_=re.compile(r'.*note.*|.*special.*', re.I))
        notes = notes_elem.get_text(strip=True) if notes_elem else ""
        
        # Extract available dates
        dates_elem = soup.find(class_=re.compile(r'.*date.*|.*calendar.*|.*avail.*', re.I))
        dates = dates_elem.get_text(strip=True) if dates_elem else ""
        
        return {
            'title': title,
            'url': url,
            'subtitle': subtitle,
            'description': description,
            'images': images,
            'prices': prices,
            'region': region,
            'skill_level': skill,
            'season': season,
            'duration': duration,
            'depart_location': depart,
            'distance': distance,
            'special_notes': notes,
            'available_dates': dates,
            'scraped_at': datetime.now().isoformat()
        }

    def clean_data(self, raw_data):
        """
        Clean and structure the scraped data
        """
        logging.info("Cleaning and structuring scraped data...")
        
        cleaned_data = []
        
        for tour in raw_data:
            # Clean text fields
            cleaned_tour = {
                'title': self.clean_text(tour.get('title', '')),
                'url': tour.get('url', ''),
                'subtitle': self.clean_text(tour.get('subtitle', '')),
                'description': self.clean_text(tour.get('description', '')),
                'images': [self.normalize_url(img) for img in tour.get('images', [])],
                'prices': [self.clean_text(price) for price in tour.get('prices', [])],
                'region': self.clean_text(tour.get('region', '')),
                'skill_level': self.clean_text(tour.get('skill_level', '')),
                'season': self.clean_text(tour.get('season', '')),
                'duration': self.clean_text(tour.get('duration', '')),
                'depart_location': self.clean_text(tour.get('depart_location', '')),
                'distance': self.clean_text(tour.get('distance', '')),
                'special_notes': self.clean_text(tour.get('special_notes', '')),
                'available_dates': self.clean_text(tour.get('available_dates', '')),
                'scraped_at': tour.get('scraped_at', datetime.now().isoformat())
            }
            
            cleaned_data.append(cleaned_tour)
        
        return cleaned_data

    def clean_text(self, text):
        """
        Clean HTML text content
        """
        if not text:
            return ""
        
        # Remove HTML tags
        clean_text = re.sub(r'<[^>]+>', ' ', str(text))
        
        # Clean up whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        # Remove special characters while preserving punctuation
        clean_text = re.sub(r'[^\w\s\.\!\?\,\;\:\-\(\)\'\"]', ' ', clean_text)
        
        return clean_text

    def normalize_url(self, url):
        """
        Normalize URLs to absolute paths
        """
        if not url:
            return ""
        
        if url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return f"{self.base_url}{url}"
        elif url.startswith('http'):
            return url
        else:
            return f"{self.base_url}/{url}"

    def save_data(self, data, filename):
        """
        Save cleaned data to various formats
        """
        logging.info(f"Saving data to {filename}...")
        
        # Save as JSON
        json_path = f"data/processed/{filename}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(data)
        csv_path = f"data/processed/{filename}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        logging.info(f"Data saved as JSON and CSV: {filename}")
        
        return df

    def scrape_tour_listings(self):
        """
        Scrape tour listing pages to get individual tour URLs
        """
        logging.info("Scraping tour listing pages...")
        
        # Common tour listing URLs
        listing_urls = [
            f"{self.base_url}/day-tour-list/",
            f"{self.base_url}/multi-day-tours-list/",
            f"{self.base_url}/tours/"
        ]
        
        tour_urls = set()
        
        for url in listing_urls:
            try:
                logging.info(f"Scraping tour listing: {url}")
                
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for tour links in various common patterns
                link_selectors = [
                    'a[href*="/tours/"]',
                    '.tour-link',
                    '.tour-item a',
                    '.listing-item a',
                    'article a',
                    '.post-title a'
                ]
                
                for selector in link_selectors:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href and ('/tours/' in href or '/tour/' in href):
                            if not href.startswith('http'):
                                href = f"{self.base_url}{href}"
                            tour_urls.add(href)
            
            except Exception as e:
                logging.error(f"Error scraping listing {url}: {str(e)}")
                continue
        
        logging.info(f"Found {len(tour_urls)} unique tour URLs")
        return list(tour_urls)

def main():
    scraper = RimToursDataScraper()
    
    logging.info("Starting RimTours data scraping process...")
    
    # Step 1: Get tour URLs from listing pages
    tour_urls = scraper.scrape_tour_listings()
    
    if not tour_urls:
        logging.warning("No tour URLs found. Trying alternative methods...")
        # Add any fallback methods here if needed
        return
    
    # Step 2: Scrape individual tour pages
    raw_data = scraper.scrape_tour_pages(tour_urls)
    
    # Step 3: Clean and structure the data
    cleaned_data = scraper.clean_data(raw_data)
    
    # Step 4: Save the processed data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = scraper.save_data(cleaned_data, f"rimtours_data_{timestamp}")
    
    logging.info(f"Scraping completed! Processed {len(cleaned_data)} tours.")
    logging.info(f"Data saved to data/processed/rimtours_data_{timestamp}.[json|csv]")

if __name__ == "__main__":
    main()