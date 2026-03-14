import os
import re
import io
import sys
import time
import json
import random
import requests
from curl_cffi import requests as currequests
import urllib.parse
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import logging

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

logger = logging.getLogger("RareAnimes")

EP_REGEX = re.compile(r"(Episode\s*\d+|Ep\s*\d+|S\d+\s*E\d+|Movie|Special|OVA)", re.I)
QUALITY_REGEX = re.compile(r'(\d{3,4}p|SD|HD|FHD|4K|Ultra\s*HD)', re.I)


class RareAnimes:
    def __init__(self):
        self.ROOT_URL = "https://codedew.com/"
        self.MQ_BASE_URL = "https://swift.multiquality.click/"
        self.UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        self.MAX_STEPS = 5
        self.STEP_DELAY = 1.0

        # Use curl_cffi to perfectly impersonate a real browser
        self.session = self._init_session()
        self.initialized = False
        self.last_mq_referer = None
        self.metadata = {} # Initialize metadata attribute

    def _init_session(self):
        session = currequests.Session(impersonate="chrome124")
        session.headers.update({
            "User-Agent": self.UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        return session

    def init_session(self):
        """Initialize session by visiting home page and codedew with retry logic."""
        if self.initialized: return
        try:
            logger.info("[*] Initializing session...")
            # Step 1: Hit RareAnimes
            self.session.get("https://rareanimes.app/", timeout=15, headers={
                "Sec-Fetch-Site": "none", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"
            })
            
            # Step 2: Hit codedew (ROOT_URL)
            def try_codedew(site_mode):
                return self.session.get(self.ROOT_URL, timeout=15, headers={
                    "Referer": "https://rareanimes.app/",
                    "Sec-Fetch-Site": site_mode,
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document"
                })

            res = try_codedew("cross-site")
            if res.status_code == 403:
                logger.warning("[!] codedew 403 with cross-site. Retrying with 'none'...")
                res = try_codedew("none")
                
            logger.info(f"[*] Session Inited. codedew Status: {res.status_code}")
            self.initialized = True
        except Exception as e:
            logger.error(f"[!] Session Init Error: {e}")
            self.initialized = True
    def get_session_cookies(self):
        return self.session.cookies.get_dict()

    def get_links(self, url, ep_start=None, ep_end=None, verbose=True):
        if verbose: logger.info(f"[*] ANALYZING: {url}")
        try:
            self.init_session()
            html = self.session.get(url, timeout=20).text
            raw_eps = self._extract_episodes(html, url)
            
            if not raw_eps:
                if verbose: logger.warning("[!] No episodes found on page.")
                return {"error": "No episodes found", "episodes": []}

            grouped_eps = self._group_episodes(raw_eps)
            unique_labels = list(grouped_eps.keys())
            
            # Sort labels numerically if possible
            try:
                unique_labels.sort(key=lambda x: int(re.search(r'(\d+)', x).group(1)) if re.search(r'(\d+)', x) else 0)
            except Exception: pass
            
            total_available = len(unique_labels)

            s_idx, e_idx = 0, total_available
            if ep_start is not None:
                s_idx = max(0, ep_start - 1)
                e_idx = (s_idx + 1) if ep_end is None else min(total_available, ep_end)
            
            selected_labels = unique_labels[s_idx:e_idx]
            total_selected = len(selected_labels)

            if verbose:
                logger.info(f"[SYSTEM] Unique Episodes   : {total_available}")
                logger.info(f"[SYSTEM] Selected Episodes : {total_selected}")

            results = []
            for idx, label in enumerate(selected_labels, 1):
                mirrors = grouped_eps[label]
                if verbose: logger.info(f"[*] Processing Item {idx}/{total_selected}: {label}")
                
                self.last_mq_referer = None
                links = self._try_mirrors(mirrors)
                
                entry = {
                    "episode": label, 
                    "url": mirrors[0]["url"], 
                    "referer": self.last_mq_referer,
                    "cookies": self.get_session_cookies(),
                    "user_agent": self.session.headers.get("User-Agent")
                }
                if links and isinstance(links, list):
                    entry["downloads"] = links
                    if verbose: self._print_qualities(links)
                else:
                    entry["downloads"], entry["error"] = [], "Failed to bypass"
                results.append(entry)

            if verbose: logger.info("\n" + "═"*65 + "\n  ✅  COMPLETED\n" + "═"*65)
            return {"episodes": results}
        except Exception as e:
            if verbose: logger.error(f"[!] System Error: {e}", exc_info=True)
            return {"error": str(e), "episodes": []}

    def _group_episodes(self, episodes):
        groups = {}
        for ep in episodes:
            groups.setdefault(ep['label'], []).append(ep)
        return groups

    def _try_mirrors(self, mirrors):
        logger.info(f"    [*] Trying {len(mirrors)} mirrors...")
        for i, m in enumerate(mirrors, 1):
            logger.info(f"    [*] Mirror {i}: {m['url'][:60]}...")
            links = self.process_zipper(m["url"], m["referer"])
            if links: 
                logger.info(f"    [+] Success with mirror {i}")
                return links
        return None

    def _print_qualities(self, links):
        logger.info(" ─" * 15)
        for item in links:
            q = item.get('label', 'N/A').ljust(10)
            s = item.get('size', 'N/A').rjust(10)
            logger.info(f"  ➜  {q} | {s}  |  {item.get('link')}")
        logger.info(" ─" * 15)

    def _quality_from_text(self, text):
        if not text: return None
        m = QUALITY_REGEX.search(text)
        return m.group(1).upper() if m else None

    def _extract_episodes(self, html, referer):
        soup = BeautifulSoup(html, "html.parser")
        raw_list, seen_hrefs = [], set()
        
        # 1. Collect all potential links
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            text = tag.get_text(strip=True)
            text_low = text.lower()
            
            is_zipper = "codedew.com/zipper/" in href
            is_hub = "store.animetoonhindi.com" in href or "/multiquality/" in href
            if not (is_zipper or is_hub): continue
            
            # Simple noise filter
            if is_zipper and len(text) < 4 and any(x in text_low for x in ["ep","e ","p"]):
                continue

            if not is_zipper and not any(kw in text_low for kw in ["watchmultquality","watchmultiquality","multiquality","download"]): continue
            if href in seen_hrefs: continue
            seen_hrefs.add(href)
            
            label = self._get_episode_label(tag)
            raw_list.append({"url": href, "referer": referer, "label": label, "is_hub": is_hub})

        # 2. Extract HUB links
        final_list = []
        for item in raw_list:
            if item["is_hub"]:
                hub_eps = self._extract_links_from_hub(item["url"], item["referer"])
                final_list.extend(hub_eps if hub_eps else [item])
            else:
                final_list.append(item)

        # 3. Deduplicate and clean by numbering to fix indexing shifts
        # We group by numeric part of the label
        processed = {}
        for item in final_list:
            lbl = item["label"]
            match = re.search(r'(\d+)', lbl)
            num = int(match.group(1)) if match else lbl # Use full label if no number
            
            if num not in processed:
                processed[num] = item
            else:
                # Prefer the one with longer label (usually "Episode X" vs "EP X")
                if len(lbl) > len(processed[num]["label"]):
                    processed[num] = item
        
        # Return sorted by number
        sorted_keys = sorted(processed.keys(), key=lambda x: x if isinstance(x, int) else 9999)
        return [processed[k] for k in sorted_keys]

    def _extract_links_from_hub(self, hub_url, referer):
        try:
            # Add specific headers for hub requests
            # Updated hub headers
            r = self.session.get(hub_url, headers={
                "Referer": "https://rareanimes.app/",
                "Sec-Fetch-Site": "cross-site",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Dest": "document"
            }, timeout=15)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            seen, eps = set(), []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "codedew.com/zipper/" in href and href not in seen:
                    seen.add(href)
                    label = re.sub(r'\[|\]', '', a.get_text(strip=True)).strip()
                    # Normalize labels from hubs
                    m = EP_REGEX.search(label)
                    label = m.group(1) if m else label
                    eps.append({"url": href, "referer": hub_url, "label": label, "is_hub": False})
            return eps
        except Exception:
            return []

    def _get_episode_label(self, tag):
        text = tag.get_text(strip=True)
        if len(text) > 3 and any(kw in text.lower() for kw in ["episode","ep ","mov","spec"]):
            m = EP_REGEX.search(text)
            if m: return m.group(1).title()

        curr = tag.parent
        for _ in range(5):
            if not curr: break
            p = curr.get_text(" ", strip=True)
            m = EP_REGEX.search(p)
            if m and len(p) < 200: return m.group(1).title()
            curr = curr.parent

        curr = tag.parent
        while curr:
            prev = curr.find_previous_sibling()
            while prev:
                m = EP_REGEX.search(prev.get_text(strip=True))
                if m: return m.group(1).title()
                prev = prev.find_previous_sibling()
            curr = curr.parent
            if curr and curr.name == "body": break
        return "Episode/Download"

    def process_zipper(self, url, referer):
        try:
            cur_url, cur_ref = url, referer
            for step in range(1, self.MAX_STEPS + 1):
                # Use robust headers per request
                step_headers = {
                    "Referer": cur_ref,
                    "Sec-Fetch-Site": "cross-site" if "codedew.com" in cur_url and "rareanimes.app" in cur_ref else "same-origin",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Dest": "document"
                }
                logger.info(f"      [#] Step {step}: Requesting {cur_url[:60]}...")
                
                resp = None
                for attempt in range(2):
                    try:
                        resp = self.session.get(cur_url, headers=step_headers, timeout=15)
                        if resp.status_code == 200:
                            break
                        if resp.status_code == 403:
                            logger.warning(f"      [!] 403 Forbidden at step {step} (Attempt {attempt+1})")
                        else:
                            logger.warning(f"      [!] Status {resp.status_code} at step {step}")
                        time.sleep(2)
                    except Exception as e:
                        logger.error(f"      [!] Request error: {e}")
                        if attempt == 1: raise
                
                if not resp or resp.status_code != 200: 
                    logger.warning(f"      [!] Failed at step {step}. Status: {resp.status_code if resp else 'No Resp'}")
                    return None

                token = re.search(r'name="rtiwatch"\s+value="([^"]+)"', resp.text)
                if token and token.group(1) != "notranslate":
                    target = f"{self.MQ_BASE_URL}downlead/{token.group(1)}/"
                    self.last_mq_referer = target
                    return self.process_multiquality(target)

                soup = BeautifulSoup(resp.text, "html.parser")
                nxt = self._find_next_step(soup, cur_url)
                if not nxt: 
                    logger.warning(f"    [!] Dead end at step {step} for {cur_url}. Status: {resp.status_code}")
                    logger.warning(f"    [!] HTML Snippet: {resp.text[:500]}")
                    return None
                cur_ref, cur_url = cur_url, nxt
                time.sleep(self.STEP_DELAY)
            logger.warning(f"    [!] Exceeded {self.MAX_STEPS} steps.")
        except Exception as e:
            logger.error(f"    [!] Zipper error: {e}", exc_info=False)
        return None

    def _find_next_step(self, soup, current_url):
        for btn_id in ["goBtn", "mainActionBtn", "btn-main"]:
            btn = soup.find("a", id=btn_id)
            if btn and btn.has_attr("href"):
                u = urljoin(self.ROOT_URL, btn["href"])
                if u != current_url: return u
        
        for a in soup.find_all("a", href=True):
            if "ad_step=" in a["href"]:
                u = urljoin(self.ROOT_URL, a["href"])
                if u != current_url: return u
        
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(kw in txt for kw in ["continue", "next", "get link", "skip"]):
                u = urljoin(self.ROOT_URL, a["href"])
                if "/zipper/" in u and u != current_url: return u
        return None

    def process_multiquality(self, downlead_url):
        try:
            hdrs = {"Referer": self.ROOT_URL}
            resp = self.session.get(downlead_url, headers=hdrs, timeout=15)
            jd = self._extract_juicy_data(resp.text)
            if not jd: 
                logger.error("    [!] juicyData not found")
                return None

            token = jd.get("token")
            links_route = jd.get("routes", {}).get("links")
            ping_route = jd.get("routes", {}).get("ping")
            if not token or not links_route: 
                logger.error("    [!] Token/route missing")
                return None

            if ping_route:
                self.session.post(urljoin(self.MQ_BASE_URL, ping_route), headers=hdrs, timeout=10)
            time.sleep(1.0)

            api_resp = self.session.post(
                urljoin(self.MQ_BASE_URL, links_route),
                headers={"Referer": downlead_url, "X-Requested-With": "XMLHttpRequest",
                         "Accept": "application/json", "Content-Type": "application/json",
                         "Origin": self.MQ_BASE_URL.rstrip("/")},
                json={"captcha": None, "_token": token}, timeout=15)

            if api_resp.status_code != 200: 
                logger.error(f"    [!] API error {api_resp.status_code}: {api_resp.text[:100]}")
                return None

            data = api_resp.json()
            quals = data.get("qualities", [])
            if data.get("success") and quals:
                # Quality link might need this metadata for download
                # Use the base domain as referer as seen in successful browser requests
                self.metadata = {
                    'cookies': self.session.cookies.get_dict(),
                    'referer': downlead_url,
                    'user_agent': self.UA
                }
                
                result = []
                for q in quals:
                    lbl = q.get("label") or q.get("quality")
                    if not lbl or lbl == "N/A":
                        title = self._extract_title(resp.text)
                        lbl = self._quality_from_text(title) or self._quality_from_text(q.get("link","")) or "N/A"
                    
                    link = q.get("link")
                    # Append metadata to each link object for easier use in bot.py
                    result.append({
                        "label": lbl, 
                        "size": q.get("size"), 
                        "link": link,
                        "metadata": self.metadata
                    })
                return result
            else:
                logger.warning(f"    [!] No qualities. Msg: {data.get('message')}")
        except Exception as e:
            logger.error(f"    [!] MQ error: {e}", exc_info=False)
        return None

    def _extract_title(self, html):
        soup = BeautifulSoup(html, "html.parser")
        h = soup.find("div", class_="head__title")
        if h: return h.get_text(" ", strip=True).replace("DOWNLOAD:", "").strip()
        for tag in soup.find_all(["h1", "h2", "title"]):
            t = tag.get_text(strip=True)
            if "download" in t.lower() or "episode" in t.lower():
                return t.split("|")[0].split("-")[0].strip()
        return ""

    def _extract_juicy_data(self, html):
        m = re.search(r"window\.juicyData\s*=\s*(\{)", html)
        if not m: return None
        start, depth, end = m.start(1), 0, -1
        for i in range(start, len(html)):
            if html[i] == "{": depth += 1
            elif html[i] == "}": depth -= 1
            if depth == 0: end = i + 1; break
        if end == -1: return None
        try:
            parsed = json.loads(html[start:end])
            return parsed.get("data", parsed)
        except Exception as e:
            logger.error(f"    [!] juicyData parse error: {e}", exc_info=False)
            return None
