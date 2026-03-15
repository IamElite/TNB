import logging
import json
from RareAnimes import RareAnimes

logging.basicConfig(level=logging.INFO)

def test_scraper():
    url = "https://rareanimes.app/attack-on-titan-season-1-hindi-dubbed-episodes-download-hd/"
    scraper = RareAnimes()
    print(f"Testing URL: {url}")
    res = scraper.get_links(url, ep_start=1, ep_end=1)
    
    if "error" in res:
        print(f"Error: {res['error']}")
    else:
        print(f"Success! Found {len(res['episodes'])} episodes.")
        print(json.dumps(res, indent=2))

if __name__ == "__main__":
    test_scraper()
