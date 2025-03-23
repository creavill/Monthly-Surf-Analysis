"""
URL Builder Module

This module provides utilities for constructing and testing URLs for surf data images,
dealing with different formats and special characters in spot names.
"""

import requests
import re
import time
from typing import Optional, List
from urllib.parse import quote


class SurfGIFFinder:
    """
    A class to find working URLs for surf data images by testing different formatting options.
    """
    
    def __init__(self, delay: float = 0.5, direction: str = 'forward'):
        """
        Initialize the SurfGIFFinder.
        
        Args:
            delay (float): Delay in seconds between URL requests
            direction (str): Direction to process spots ('forward' or 'reverse')
        """
        self.delay = delay
        self.direction = direction
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.special_words = ['de', 'del', 'do', 'da']
        self.url_patterns = [
            "https://example.com/data/{}.surf.consistency.january.gif",
            "https://example.com/data/{}.surf.statistics.january.gif"
        ]

    def format_spot_name(self, spot_name: str) -> str:
        """
        Format spot name according to specific rules for URL construction.
        
        Args:
            spot_name (str): Original spot name
            
        Returns:
            str: Formatted spot name for URL
        """
        words = spot_name.split()
        formatted_words = []
        skip_next = False

        for i, word in enumerate(words):
            if skip_next:
                skip_next = False
                continue

            # Check if current word is followed by a special word
            if (i < len(words) - 1) and words[i + 1].lower() in self.special_words:
                # Combine current word with special word
                combined = words[i] + words[i + 1].lower()
                formatted_words.append(combined)
                skip_next = True
            else:
                # Just capitalize first letter if not a special word
                if word.lower() not in self.special_words:
                    formatted_words.append(word.capitalize())

        # Join with dashes
        combined = '-'.join(formatted_words)

        # Clean up multiple dashes
        combined = re.sub(r'-+', '-', combined)

        return combined

    def test_url(self, url: str) -> bool:
        """
        Test if a URL returns a valid response.
        
        Args:
            url (str): URL to test
            
        Returns:
            bool: True if the URL is valid, False otherwise
        """
        try:
            response = self.session.head(url, headers=self.headers, timeout=5)
            print(f"Status code: {response.status_code}")
            return response.status_code == 200
        except requests.RequestException:
            return False

    def find_working_url(self, spot_name: str) -> Optional[str]:
        """
        Find a working URL for a spot by trying different URL patterns.
        
        Args:
            spot_name (str): Name of the surf spot
            
        Returns:
            str or None: Working URL if found, None otherwise
        """
        formatted_name = self.format_spot_name(spot_name)
        print(f"\nFormatted {spot_name} to: {formatted_name}")

        for url_pattern in self.url_patterns:
            url = url_pattern.format(quote(formatted_name))
            print(f"\nTesting URL: {url}")

            if self.test_url(url):
                print(f"Found working URL!")
                return url

            time.sleep(self.delay)

        print(f"No working URL found for {spot_name}")
        return None

    def process_missing_locations(self, csv_path: str):
        """
        Process missing locations in specified direction and find working URLs.
        
        Args:
            csv_path (str): Path to CSV file with missing locations
        """
        import pandas as pd
        
        df = pd.read_csv(csv_path)

        if 'gif_url' not in df.columns:
            df['gif_url'] = ''

        # Filter for spots without working URLs
        missing_mask = df['gif_url'].isna() | (df['gif_url'] == '')
        missing_spots = df[missing_mask]

        if len(missing_spots) == 0:
            print("No missing URLs found - all spots have been processed!")
            return

        print(f"Found {len(missing_spots)} spots without URLs")
        print(f"Skipping {len(df) - len(missing_spots)} spots with existing URLs")
        print(f"Processing in {self.direction} direction")

        # Get indices in correct order
        missing_indices = missing_spots.index.tolist()
        if self.direction == 'reverse':
            missing_indices = missing_indices[::-1]

        # Process missing spots
        for idx in missing_indices:
            working_url = self.find_working_url(df.loc[idx, 'name'])

            if working_url:
                df.at[idx, 'gif_url'] = working_url
                df.to_csv(csv_path, index=False)
                print(f"Saved working URL for {df.loc[idx, 'name']}")

            time.sleep(self.delay)

        # Print summary
        total_found = len(df[df['gif_url'].notna() & (df['gif_url'] != '')])
        print(f"\nProcessing complete!")
        print(f"Total spots with URLs: {total_found}")
        print(f"Remaining missing URLs: {len(df) - total_found}")


def build_month_urls(base_url: str, spot_name: str) -> List[str]:
    """
    Build URLs for all months for a specific spot.
    
    Args:
        base_url (str): Base URL pattern
        spot_name (str): Name of the surf spot
        
    Returns:
        list: List of URLs for all months
    """
    months = ['january', 'february', 'march', 'april', 'may', 'june',
              'july', 'august', 'september', 'october', 'november', 'december']
    
    formatted_name = spot_name.replace(' ', '-')
    urls = []
    
    for month in months:
        url = base_url.format(spot_name=formatted_name, month=month)
        urls.append(url)
    
    return urls


def test_url_formats(spot_name: str, base_patterns: List[str]) -> Optional[str]:
    """
    Test different URL format patterns for a spot name.
    
    Args:
        spot_name (str): Name of the surf spot
        base_patterns (list): List of URL patterns to test
        
    Returns:
        str or None: Working base URL if found, None otherwise
    """
    formatter = SurfGIFFinder()
    formatted_name = formatter.format_spot_name(spot_name)
    
    for pattern in base_patterns:
        url = pattern.format(formatted_name)
        
        if formatter.test_url(url):
            # Extract base URL by removing the month and .gif
            base_url = url.rsplit('.', 2)[0]
            return base_url
        
        time.sleep(0.5)
    
    return None
