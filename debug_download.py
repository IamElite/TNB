
import logging
import json
import re
import sys
import io
import time
from urllib.parse import unquote, urlparse
from RareAnimes import RareAnimes
from curl_cffi import requests as currequests

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RareAnimes")

# Ensure UTF-8 for Windows
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def test_download(url, meta):
    strategies = [
        {"name": "A: Full Headers + Cookies", "headers": True, "cookies": True, "warmup": False},
        {"name": "B: Same UA only (No Cookies/Ref)", "headers": False, "cookies": False, "warmup": False},
        {"name": "C: Referer Only (No Cookies)", "headers": True, "cookies": False, "warmup": False},
        {"name": "D: Mirror Warmup + Full", "headers": True, "cookies": True, "warmup": True},
    ]
    
    for strtg in strategies:
        logger.info(f"--- Testing Strategy: {strtg['name']} ---")
        try:
            with currequests.Session(impersonate="chrome124") as s:
                if strtg['warmup']:
                    domain = urlparse(url).netloc
                    logger.info(f"   [Warmup] Visiting: https://{domain}/")
                    try: s.get(f"https://{domain}/", timeout=10)
                    except: pass
                
                if strtg['cookies'] and meta.get('cookies'):
                    s.cookies.update(meta['cookies'])
                
                headers = {
                    "User-Agent": meta.get('user_agent', "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
                }
                if strtg['headers']:
                    headers.update({
                        "Referer": meta.get('referer', ""),
                        "Accept": "*/*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Connection": "keep-alive",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "cross-site",
                    })
                
                resp = s.get(url, headers=headers, stream=True, timeout=15, allow_redirects=True)
                logger.info(f"   -> Result: {resp.status_code}")
                if resp.status_code == 200:
                    logger.info(f"   [SUCCESS] Received content length: {resp.headers.get('Content-Length')}")
                    return True
                else:
                    logger.warning(f"   [FAILED] Body Start: {resp.text[:200]}...")
        except Exception as e:
            logger.error(f"   [ERROR] {e}")
        time.sleep(2)
    return False

def main():
    target_url = "https://rareanimes.app/attack-on-titan-season-1-hindi-dubbed-episodes-download-hd/"
    ra = RareAnimes()
    logger.info("Starting Scraper to get fresh MQ link...")
    
    res = ra.get_links(target_url, ep_start=1, ep_end=1)
    episodes = res.get("episodes", [])
    if not episodes or not episodes[0].get('downloads'):
        logger.error("Failed to get quality links.")
        return

    dl = episodes[0]['downloads'][0]
    logger.info(f"Testing download for: {dl['label']} - {dl['link'][:60]}...")
    test_download(dl['link'], dl['metadata'])

if __name__ == "__main__":
    main()
