#!/usr/bin/env python3
"""
Diagnostic script to test individual venue scraping and identify issues
with CSS selectors and website structures.
"""

import sys
import os
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import time

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Headers for web scraping to appear as a regular browser
SCRAPING_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Test venues - a subset to diagnose issues
TEST_VENUES = {
    "T-Mobile Center": {
        "url": "https://www.t-mobilecenter.com/events",
        "selectors": {
            "event_container": ".event-item, .event-listing, .event",
            "title": ".event-title, .title, h3, h2",
            "date": ".event-date, .date, .event-time",
        },
    },
    "Union Station Kansas City": {
        "url": "https://unionstation.org/events/",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
        },
    },
    "Nelson-Atkins Museum": {
        "url": "https://www.nelson-atkins.org/events/",
        "selectors": {
            "event_container": ".event-item, .event",
            "title": ".event-title, h3, h2",
            "date": ".event-date, .date",
        },
    },
}


def safe_scrape_request(url, timeout=10):
    """Make a safe request for web scraping with proper headers and error handling"""
    try:
        print(f"  Requesting: {url}")
        response = requests.get(url, headers=SCRAPING_HEADERS, timeout=timeout)
        print(f"  Status Code: {response.status_code}")
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Failed to scrape {url}: {e}")
        return None


def analyze_page_structure(venue_name, url, selectors):
    """Analyze the structure of a venue's events page"""
    print(f"\n🔍 Analyzing: {venue_name}")
    print(f"📍 URL: {url}")

    response = safe_scrape_request(url)
    if not response:
        return

    try:
        soup = BeautifulSoup(response.content, "html.parser")
        print(f"  ✅ Successfully parsed HTML")

        # Test each selector
        for selector_name, selector in selectors.items():
            print(f"\n  🎯 Testing selector '{selector_name}': {selector}")

            # Try each selector in the comma-separated list
            selector_parts = [s.strip() for s in selector.split(",")]
            found_elements = []

            for part in selector_parts:
                elements = soup.select(part)
                if elements:
                    found_elements.extend(elements)
                    print(f"    ✅ Found {len(elements)} elements with '{part}'")

                    # Show first few examples
                    for i, elem in enumerate(elements[:3]):
                        text = elem.get_text(strip=True)[:100]
                        print(f"      [{i+1}] {text}...")
                else:
                    print(f"    ❌ No elements found with '{part}'")

            if not found_elements:
                print(f"    ⚠️  No elements found for any selector in '{selector}'")

        # Look for common event-related elements
        print(f"\n  🔍 Looking for common event patterns...")

        common_patterns = [
            "event",
            "show",
            "performance",
            "concert",
            "exhibition",
            "program",
            "activity",
            "calendar",
            "listing",
        ]

        for pattern in common_patterns:
            # Look for classes containing the pattern
            class_elements = soup.find_all(
                class_=lambda x: x and pattern in " ".join(x).lower()
            )
            if class_elements:
                print(
                    f"    📅 Found {len(class_elements)} elements with class containing '{pattern}'"
                )

                # Show examples
                for i, elem in enumerate(class_elements[:2]):
                    classes = " ".join(elem.get("class", []))
                    text = elem.get_text(strip=True)[:80]
                    print(f"      [{i+1}] Class: '{classes}' | Text: {text}...")

        # Look for date patterns
        print(f"\n  📅 Looking for date patterns...")
        date_patterns = [
            r"\b\d{1,2}/\d{1,2}/\d{4}\b",  # MM/DD/YYYY
            r"\b\w+\s+\d{1,2},?\s+\d{4}\b",  # Month DD, YYYY
            r"\b\d{1,2}\s+\w+\s+\d{4}\b",  # DD Month YYYY
        ]

        import re

        page_text = soup.get_text()
        for pattern in date_patterns:
            matches = re.findall(pattern, page_text)
            if matches:
                print(f"    📆 Found date pattern '{pattern}': {matches[:5]}")

        # Look for structured data
        print(f"\n  🏗️  Looking for structured data...")
        json_ld = soup.find_all("script", type="application/ld+json")
        if json_ld:
            print(f"    📊 Found {len(json_ld)} JSON-LD structured data blocks")

        microdata = soup.find_all(attrs={"itemtype": True})
        if microdata:
            print(f"    🏷️  Found {len(microdata)} microdata elements")

    except Exception as e:
        print(f"  ❌ Error analyzing page: {e}")


def suggest_improved_selectors(venue_name, url):
    """Suggest improved selectors based on page analysis"""
    print(f"\n💡 Suggesting improved selectors for {venue_name}...")

    response = safe_scrape_request(url)
    if not response:
        return

    try:
        soup = BeautifulSoup(response.content, "html.parser")

        # Look for elements that likely contain events
        potential_containers = []

        # Check for common event container patterns
        container_patterns = [
            {
                "selector": '[class*="event"]',
                "description": 'Elements with "event" in class',
            },
            {
                "selector": '[class*="show"]',
                "description": 'Elements with "show" in class',
            },
            {
                "selector": '[class*="listing"]',
                "description": 'Elements with "listing" in class',
            },
            {
                "selector": '[class*="card"]',
                "description": 'Elements with "card" in class',
            },
            {"selector": "article", "description": "Article elements"},
            {"selector": ".post", "description": "Post elements"},
        ]

        for pattern in container_patterns:
            elements = soup.select(pattern["selector"])
            if elements:
                print(f"  📦 {pattern['description']}: {len(elements)} found")

                # Analyze first element for structure
                if elements:
                    first_elem = elements[0]
                    classes = " ".join(first_elem.get("class", []))
                    print(f"    Example class: '{classes}'")

                    # Look for title elements within
                    titles = first_elem.select(
                        'h1, h2, h3, h4, h5, h6, .title, [class*="title"]'
                    )
                    if titles:
                        title_text = titles[0].get_text(strip=True)[:60]
                        print(f"    Example title: '{title_text}...'")

                    # Look for date elements within
                    dates = first_elem.select('.date, [class*="date"], time')
                    if dates:
                        date_text = dates[0].get_text(strip=True)[:40]
                        print(f"    Example date: '{date_text}'")

        print(f"\n  🎯 Recommended selectors:")
        print(
            f'    event_container: \'[class*="event"], [class*="show"], [class*="listing"], article\''
        )
        print(f"    title: 'h1, h2, h3, h4, .title, [class*=\"title\"]'")
        print(f'    date: \'.date, [class*="date"], time, [class*="time"]\'')

    except Exception as e:
        print(f"  ❌ Error suggesting selectors: {e}")


def main():
    """Main diagnostic function"""
    print("🔧 VENUE SCRAPING DIAGNOSTIC TOOL")
    print("=" * 50)

    for venue_name, config in TEST_VENUES.items():
        analyze_page_structure(venue_name, config["url"], config["selectors"])
        suggest_improved_selectors(venue_name, config["url"])

        print(f"\n" + "─" * 50)
        time.sleep(2)  # Be respectful to servers

    print(f"\n✅ Diagnostic complete!")
    print(f"\n💡 Next steps:")
    print(f"  1. Update CSS selectors based on findings above")
    print(f"  2. Test individual venue scrapers with new selectors")
    print(f"  3. Consider using structured data (JSON-LD) if available")
    print(f"  4. Add fallback selectors for robustness")


if __name__ == "__main__":
    main()
