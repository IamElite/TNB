import asyncio
import logging
from bot import RareAnimes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TestRare")

async def test_extraction():
    url = "https://rareanimes.app/attack-on-titan-season-1-hindi-dubbed-episodes-download-hd/"
    logger.info(f"[*] Testing extraction for: {url}")
    bypasser = RareAnimes()
    res = bypasser.get_links(url, 1, 1) # Just episode 1
    
    if "error" in res:
        logger.error(f"[!] Test Failed: {res['error']}")
        return

    episodes = res.get("episodes", [])
    logger.info(f"[+] Episodes extracted: {len(episodes)}")
    
    for ep in episodes:
        label = ep.get("episode")
        downloads = ep.get("downloads", [])
        logger.info(f"    - {label}: {len(downloads)} qualities found")
        for dl in downloads:
            logger.info(f"        * {dl.get('label')}: {dl.get('link')[:60]}...")

if __name__ == "__main__":
    asyncio.run(test_extraction())
