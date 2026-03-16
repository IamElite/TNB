
import logging
import json
import re
import sys
import io
from RareAnimes import RareAnimes

# Setup logging to see everything
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RareAnimes")

# Ensure UTF-8 for Windows
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

class DebugRareAnimes(RareAnimes):
    def process_multiquality(self, url):
        try:
            logger.info(f"[DEBUG] Fetching MQ page: {url}")
            resp = self.session.get(url, headers={"Referer": self.ROOT_URL}, timeout=15)
            
            # Save HTML for inspection
            with open("mq_page_debug.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            
            logger.info(f"[DEBUG] HTML saved to mq_page_debug.html (Length: {len(resp.text)})")
            
            # Try to extract JuicyData
            match = re.search(r"juicyData\s*=\s*(\{)", resp.text)
            if match:
                logger.info("[DEBUG] Found 'juicyData' string in HTML")
            else:
                logger.error("[DEBUG] 'juicyData' NOT found in HTML")
                # Look for any other suspicious JS objects
                objs = re.findall(r"window\.(\w+)\s*=", resp.text)
                logger.info(f"[DEBUG] Other window objects: {objs}")

            jd = self._extract_juicy_data(resp.text)
            if not jd:
                logger.error("[DEBUG] _extract_juicy_data returned None")
                return None
            
            logger.info(f"[DEBUG] JuicyData extracted: {json.dumps(jd, indent=2)[:500]}...")
            
            if not jd.get("token"):
                logger.error("[DEBUG] Token missing in JuicyData")
                return None
                
            # API interaction
            token = jd["token"]
            links_api = jd.get("routes", {}).get("links")
            if not links_api:
                logger.error("[DEBUG] Links route missing in JuicyData")
                return None
                
            links_url = f"https://swift.multiquality.click{links_api}" if links_api.startswith("/") else links_api
            logger.info(f"[DEBUG] Calling Links API: {links_url}")
            
            api_resp = self.session.post(
                links_url,
                headers={
                    "Referer": url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/json"
                },
                json={"captcha": None, "_token": token},
                timeout=15
            )
            
            logger.info(f"[DEBUG] API Status: {api_resp.status_code}")
            logger.info(f"[DEBUG] API Response: {api_resp.text}")
            
            return super().process_multiquality(url)
        except Exception as e:
            logger.error(f"[DEBUG] error: {e}", exc_info=True)
            return None

def main():
    target_url = "https://rareanimes.app/attack-on-titan-season-1-hindi-dubbed-episodes-download-hd/"
    ra = DebugRareAnimes()
    logger.info("Starting Debug Bypass...")
    res = ra.get_links(target_url, ep_start=1, ep_end=1)
    print("\n[RESULT]")
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
