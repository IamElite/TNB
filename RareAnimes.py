import sys, io, os, json, random, re, time, logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET

if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

logger = logging.getLogger("RareAnimes")

EP_REGEX = re.compile(r"(Episode\s*\d+|Ep\s*\d+|S\d+\s*E\d+|Movie|Special|OVA)", re.I)
QUALITY_REGEX = re.compile(r'(\d{3,4}p|SD|HD|FHD|4K|Ultra\s*HD)', re.I)


class RareAnimes:
    ROOT_URL = "https://codedew.com/"
    MQ_BASE_URL = "https://swift.multiquality.click/"
    MAX_STEPS = 10
    STEP_DELAY = 1.0
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

    def __init__(self):
        s = requests.Session()
        s.headers.update({
            "User-Agent": self.UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        self.session = s
        self.initialized = False
        self.last_mq_url = None

    # ─── Session ───
    def init_session(self):
        if self.initialized: return
        logger.info("[*] Initializing session...")
        try:
            self.session.get(self.ROOT_URL, timeout=10)
            if "zipper_client_id" not in self.session.cookies:
                self.session.cookies.set("zipper_client_id",
                    "".join(random.choices("0123456789abcdef", k=16)), domain="codedew.com")
            self.initialized = True
        except Exception:
            pass

    # ─── Main Bypass ───
    def bypass(self, url):
        logger.info(f"[*] ANALYZING: {url}")
        try:
            self.init_session()
            html = self.session.get(url, timeout=20).text

            episodes = self._extract_episodes(html, url)
            info = self.get_rareanime_info(html)
            info['rare_total_on_page'] = len(episodes)
            self._print_anime_info(info)

            unique = self._group_episodes(episodes)
            total = len(unique)
            logger.info(f"Unique Episodes: {total} | Raw Links: {len(episodes)}")

            if not unique:
                logger.warning("[!] No episodes found."); return

            logger.info(f"[+] Found {total} unique episodes to process.")
            for i, (label, mirrors) in enumerate(unique.items(), 1):
                logger.info(f"\n[*] Processing Episode {i}/{total}: {label}")
                links = self._try_mirrors(mirrors)
                if links:
                    self._print_qualities(links)
                else:
                    logger.warning(f"Failed to fetch qualities for {label}")

            logger.info("\n" + "═"*65 + "\n  ✅  BYPASS COMPLETED\n" + "═"*65)
        except Exception as e:
            logger.error(f"[!] Error: {e}", exc_info=True)

    def get_links(self, url, ep_start=None, ep_end=None, verbose=True):
        if verbose: logger.info(f"[*] ANALYZING: {url}")
        try:
            self.init_session()
            html = self.session.get(url, timeout=20).text
            raw_eps = self._extract_episodes(html, url)
            info = self.get_rareanime_info(html)
            
            if not raw_eps:
                if verbose: logger.warning("[!] No episodes found on page.")
                return {"error": "No episodes found", "info": info, "episodes": []}

            # Professional Grouping: Group mirrors by unique episode labels
            grouped_eps = self._group_episodes(raw_eps)
            unique_labels = list(grouped_eps.keys())
            total_available = len(unique_labels)
            
            info['rare_total_on_page'] = total_available
            if verbose: self._print_anime_info(info)

            # Apply Selection to unique episodes
            s_idx, e_idx = 0, total_available
            if ep_start is not None:
                s_idx = max(0, ep_start - 1)
                e_idx = (s_idx + 1) if ep_end is None else min(total_available, ep_end)
            
            selected_labels = unique_labels[s_idx:e_idx]
            total_selected = len(selected_labels)

            if verbose:
                logger.info(f"[SYSTEM] Sources Scanned   : {len(raw_eps)} links")
                logger.info(f"[SYSTEM] Unique Episodes   : {total_available}")
                logger.info(f"[SYSTEM] Selected Episodes : {total_selected}")

            results = []
            for idx, label in enumerate(selected_labels, 1):
                mirrors = grouped_eps[label]
                if verbose: logger.info(f"\n[*] Processing Item {idx}/{total_selected}: {label}")
                
                # Try mirrors until one works
                links = self._try_mirrors(mirrors)
                
                entry = {"episode": label, "url": mirrors[0]["url"]}
                if links and isinstance(links, list):
                    entry["downloads"] = links
                    if verbose: self._print_qualities(links)
                else:
                    entry["downloads"], entry["error"] = [], "Failed to bypass"
                    if verbose: logger.warning(f"    [!] Failed to extract links for: {label}")
                results.append(entry)

            if verbose: logger.info("\n" + "═"*65 + "\n  ✅  SESSIONS COMPLETED SUCCESSFULLY\n" + "═"*65)
            return {"info": info, "episodes": results}
        except Exception as e:
            if verbose: logger.error(f"[!] System Error: {e}", exc_info=True)
            return {"error": str(e), "info": {}, "episodes": []}

    # ─── Helpers ───
    def _group_episodes(self, episodes):
        groups = {}
        for ep in episodes:
            groups.setdefault(ep['label'], []).append(ep)
        return groups

    def _try_mirrors(self, mirrors):
        for m in mirrors:
            links = self.process_zipper(m["url"], m["referer"])
            if links: return links
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

    # ─── Anime Info ───
    def get_rareanime_info(self, html):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text()
        patterns = {
            "rarefullname": r"(?:Full Name|Name|Anime):\s*(.*)",
            "rareseason": r"Season(?: No| Number)?:\s*(.*)",
            "rareepisodes": r"Episode[s]?:\s*(.*)",
            "rareyear": r"(?:Release Year|Year):\s*(.*)",
            "rareruntime": r"(?:RunTime|Run Time|Duration):\s*(.*)",
            "raregenre": r"Genre[s]?:\s*(.*)",
            "rarelanguage": r"(?:Language|Audio):\s*(.*)",
            "rarequality": r"Quality:\s*(.*)",
            "rarenetwork": r"(?:Network|TV Channel.*?):\s*(.*)",
            "raresynopsis": r"Synopsis:\s*(.*)"
        }
        info = {}
        for key, pat in patterns.items():
            m = re.search(pat, text, re.I)
            if m:
                val = re.sub(r'^[^\w\s\(\)\[\]]+', '', m.group(1).split('\n')[0].strip()).strip()
                info[key] = val or "N/A"
            else:
                info[key] = "N/A"

        if info["raresynopsis"] == "N/A":
            h = soup.find(re.compile(r'h[234]'), string=re.compile(r'Synopsis', re.I))
            if h:
                p = h.find_next('p')
                if p: info["raresynopsis"] = p.get_text(strip=True)
                
        # Parse quality count from rarequality string (e.g., "3 Qualities (480p, 720p, 1080p)")
        q_text = info.get("rarequality", "")
        q_count = 0
        if "quality" in q_text.lower():
            count_match = re.search(r'(\d+)\s*qualit', q_text, re.I)
            if count_match: q_count = int(count_match.group(1))
        if q_count == 0: # Fallback: Count comma-separated resolutions
            q_count = len([x for x in q_text.split(',') if x.strip()])
        info["quality_count_target"] = q_count if q_count > 0 else 3 # Default to 3 if unknown
        
        return info

    def _print_anime_info(self, info):
        logger.info("\n" + "─"*65)
        logger.info(f" 📺  {info.get('rarefullname', 'Anime Info')}")
        logger.info(" ─" * 15)
        for icon, key, name in [
            ("📅","rareyear","Year"), ("🍂","rareseason","Season"),
            ("🎞","rareepisodes","Episodes"), ("⌛","rareruntime","Runtime"),
            ("🎭","raregenre","Genre"), ("🔊","rarelanguage","Language"),
            ("🎬","rarequality","Quality"), ("🌐","rarenetwork","Network")
        ]:
            if key in info:
                logger.info(f"  {icon} {name:10}: {info[key]}")
        if "rare_total_on_page" in info:
            logger.info(f"  🎞 On Page   : {info['rare_total_on_page']}")
        syn = info.get('raresynopsis', '')
        if len(syn) > 150: syn = syn[:147] + "..."
        logger.info(f"  📝 Synopsis  : {syn}")
        logger.info("─"*65 + "\n")

    # ─── Episode Extraction ───
    def _extract_episodes(self, html, referer):
        soup = BeautifulSoup(html, "html.parser")
        episodes, seen = [], set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            text = tag.get_text(strip=True).lower()
            is_zipper = "codedew.com/zipper/" in href
            is_hub = "store.animetoonhindi.com" in href or "/multiquality/" in href
            if not (is_zipper or is_hub): continue
            if not is_zipper and not any(kw in text for kw in ["watchmultquality","watchmultiquality","multiquality","download"]): continue
            if href in seen: continue
            seen.add(href)
            ep = {"url": href, "referer": referer, "label": self._get_episode_label(tag), "is_hub": is_hub}
            if is_hub:
                hub_eps = self._extract_links_from_hub(href, referer)
                episodes.extend(hub_eps if hub_eps else [ep])
            else:
                episodes.append(ep)
        return episodes

    def _extract_links_from_hub(self, hub_url, referer):
        try:
            r = self.session.get(hub_url, headers={"Referer": referer}, timeout=15)
            if r.status_code != 200: return []
            soup = BeautifulSoup(r.text, "html.parser")
            seen, eps = set(), []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "codedew.com/zipper/" in href and href not in seen:
                    seen.add(href)
                    label = re.sub(r'\[|\]', '', a.get_text(strip=True)).strip()
                    eps.append({"url": href, "referer": hub_url, "label": label, "is_hub": False})
            return eps
        except Exception as e:
            logger.warning(f"Hub error: {e}"); return []

    def _get_episode_label(self, tag):
        text = tag.get_text(strip=True)
        if len(text) > 3 and any(kw in text.lower() for kw in ["episode","ep ","mov","spec"]):
            m = EP_REGEX.search(text)
            if m: return m.group(1)

        curr = tag.parent
        for _ in range(5):
            if not curr: break
            p = curr.get_text(" ", strip=True)
            m = EP_REGEX.search(p)
            if m and len(p) < 200: return m.group(1)
            curr = curr.parent

        curr = tag.parent
        while curr:
            prev = curr.find_previous_sibling()
            while prev:
                m = EP_REGEX.search(prev.get_text(strip=True))
                if m: return m.group(1)
                prev = prev.find_previous_sibling()
            curr = curr.parent
            if curr and curr.name == "body": break
        return "Episode/Download"

    # ─── Zipper Bypass ───
    def process_zipper(self, url, referer):
        logger.info(f"[*] Starting Zipper: {url[:80]}")
        try:
            cur_url, cur_ref = url, referer
            for step in range(1, self.MAX_STEPS + 1):
                headers = {"Referer": cur_ref, "Sec-Fetch-Site": "cross-site" if step == 1 else "same-origin"}
                resp = self.session.get(cur_url, headers=headers, timeout=15)
                if "Invalid request" in resp.text:
                    resp = self.session.get(cur_url, timeout=15)

                token = re.search(r'name="rtiwatch"\s+value="([^"]+)"', resp.text)
                if token and token.group(1) != "notranslate":
                    logger.info(f"    [+] Token found (Step {step})")
                    return self.process_multiquality(f"{self.MQ_BASE_URL}downlead/{token.group(1)}/")

                soup = BeautifulSoup(resp.text, "html.parser")
                nxt = self._find_next_step(soup, cur_url)
                if not nxt:
                    logger.warning(f"    [!] Dead end at step {step}"); return None
                logger.debug(f"    [*] Step {step} done...")
                cur_ref, cur_url = cur_url, nxt
                time.sleep(self.STEP_DELAY)
            logger.warning(f"    [!] Exceeded {self.MAX_STEPS} steps.")
        except Exception as e:
            logger.error(f"    [!] Zipper error: {e}")
        return None

    def _find_next_step(self, soup, current_url):
        # Professional Step Detection
        # 1. Look for specific buttons by ID
        for btn_id in ["goBtn", "mainActionBtn", "btn-main"]:
            btn = soup.find("a", id=btn_id)
            if btn and btn.has_attr("href"):
                u = urljoin(self.ROOT_URL, btn["href"])
                if u != current_url: return u
        
        # 2. Look for "ad_step=" patterns (common in zipper)
        for a in soup.find_all("a", href=True):
            if "ad_step=" in a["href"]:
                u = urljoin(self.ROOT_URL, a["href"])
                if u != current_url: return u
        
        # 3. Fallback: Any link that looks like a continuation
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(kw in txt for kw in ["continue", "next", "get link", "skip"]):
                u = urljoin(self.ROOT_URL, a["href"])
                if "/zipper/" in u and u != current_url: return u

        return None

    # ─── MultiQuality Bypass ───
    def process_multiquality(self, downlead_url):
        logger.info(f"    [*] Fetching qualities: {downlead_url[:50]}...")
        try:
            hdrs = {"User-Agent": self.UA, "Referer": self.ROOT_URL,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
            try: self.session.get(self.MQ_BASE_URL, headers={"User-Agent": self.UA}, timeout=5)
            except: pass

            resp = self.session.get(downlead_url, headers=hdrs, timeout=15)
            self.last_mq_url = downlead_url

            jd = self._extract_juicy_data(resp.text)
            if not jd:
                logger.error("    [!] juicyData not found")
                return None

            token = jd.get("token")
            links_route = jd.get("routes", {}).get("links")
            ping_route = jd.get("routes", {}).get("ping")
            if not token or not links_route:
                logger.error("    [!] Token/route missing"); return None

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
                result = []
                for q in quals:
                    lbl = q.get("label") or q.get("quality")
                    if not lbl or lbl == "N/A":
                        title = self._extract_title(resp.text)
                        lbl = self._quality_from_text(title) or self._quality_from_text(q.get("link","")) or "N/A"
                        logger.info(f"    [+] Quality fallback: {lbl}")
                    result.append({"label": lbl, "size": q.get("size"), "link": q.get("link")})
                return result

            logger.warning(f"    [!] No qualities. Msg: {data.get('message')}")
        except Exception as e:
            logger.error(f"    [!] MQ error: {e}")
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
            logger.error(f"    [!] juicyData parse error: {e}")
            return None

    # ─── Updates & Tracker ───
    def get_latest_updates(self):
        logger.info("Fetching latest updates...")
        ist = timezone(timedelta(hours=5, minutes=30))
        try:
            r = self.session.get("https://rareanimes.app/feed/", timeout=15)
            items = ET.fromstring(r.text).findall(".//item")
        except Exception as e:
            print(f"[!] Feed error: {e}"); return []

        feed = []
        for item in items:
            title = (item.find("title").text or "") if item.find("title") is not None else ""
            link = (item.find("link").text or "") if item.find("link") is not None else ""
            pub = (item.find("pubDate").text or "") if item.find("pubDate") is not None else ""

            t = "Just Now"
            if pub:
                try: t = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %z").astimezone(ist).strftime("%I:%M %p")
                except: pass

            if any(kw in title.lower() for kw in ["hindi", "hi"]) or "hindi" in link.lower():
                feed.append({"title": title, "url": link, "time": t, "language": "HINDI"})

        updates = []
        for item in feed:
            print(f"    [>] Scanning: {item['title'][:40]}...")
            try:
                res = self.session.get(item["url"], timeout=15)
                soup = BeautifulSoup(res.text, "html.parser")
                content = soup.find('div', class_='entry-content') or soup.find('article') or soup

                max_dub, max_sub, ctx = 0, 0, "dub"
                for tag in content.find_all(['h2','h3','h4','span','strong','p']):
                    txt = tag.get_text(strip=True)
                    if re.search(r'hindi\s*sub', txt, re.I): ctx = "sub"
                    elif re.search(r'hindi\s*dub', txt, re.I): ctx = "dub"
                    em = re.search(r'(?:Episode|Ep|E)\s*(\d+)', txt, re.I)
                    if em:
                        n = int(em.group(1))
                        if re.search(r'hindi\s*sub', txt, re.I) or ctx == "sub": max_sub = max(max_sub, n)
                        else: max_dub = max(max_dub, n)

                if max_dub > 0 and max_sub > 0:
                    ep_str, lang = f"{max_dub}(D), {max_sub}(S)", "HINDI DUB/SUB"
                elif max_sub > 0:
                    ep_str, lang = str(max_sub), "HINDI-SUB"
                elif max_dub > 0:
                    ep_str, lang = str(max_dub), "HINDI"
                else:
                    eps = self._extract_episodes(res.text, item["url"])
                    ep_str = "1" if eps else "0"
                    lang = "HINDI-SUB" if any(k in item["title"].lower() for k in ["subbed","hindi sub"]) else "HINDI"

                time_tag = soup.find("time", class_="updated")
                if time_tag: item["time"] = time_tag.text.strip()
                item["episode"], item["language"] = ep_str, lang
                updates.append(item)
            except Exception as e:
                print(f"        [!] Error: {e}")
        return updates

    def load_tracker_db(self, f="hindi_tracker_db.json"):
        if os.path.exists(f):
            try:
                with open(f) as fh: return json.load(fh)
            except: return {}
        return {}

    def save_tracker_db(self, data, f="hindi_tracker_db.json"):
        with open(f, "w") as fh: json.dump(data, fh, indent=4)

    def track_quality_updates(self):
        print("[*] Checking quality updates...")
        db_file = "hindi_tracker_db.json"
        db = self.load_tracker_db(db_file)
        feed = self.get_latest_updates()
        if not feed: print("[*] No updates."); return []

        notifs = []
        mq_cache = {} # Cache MultiQuality results for this run

        for u in feed:
            url, title = u.get("url"), u.get("title")
            if not url or "store.animetoonhindi.com" in url: continue
            
            if url not in db: db[url] = {"title": title, "episodes": {}}
            sdb = db[url]["episodes"]

            try:
                page_html = self.session.get(url, timeout=15).text
                info = self.get_rareanime_info(page_html)
                target_q = info.get("quality_count_target", 3)
                episodes = self._extract_episodes(page_html, url)
            except Exception as e:
                print(f"    [!] Error scanning {url}: {e}"); continue

            for ep in episodes:
                lbl, link = ep.get("label","Unknown"), ep.get("url","")
                
                # Check current state
                saved = sdb.get(lbl, {})
                known_qs = set(saved.get("qualities", []))
                
                # If we've already reached the target, skip expensive checks
                if len(known_qs) >= target_q:
                    continue

                print(f"    [*] Checking: {lbl} for {title[:25]}...")
                
                # Use cache or fetch
                if link in mq_cache:
                    qd = mq_cache[link]
                else:
                    self.last_mq_url = None
                    qd = self.process_zipper(link, url)
                    mq_cache[link] = qd
                
                if not qd: continue
                
                curr_qs = set(str(q.get('quality') or q.get('label')) for q in qd if q.get('quality') or q.get('label'))
                new_qs = curr_qs - known_qs
                
                if new_qs:
                    is_new_ep = (lbl not in sdb)
                    type_str = "NEW EP" if is_new_ep else "QUALITY"
                    print(f"    [+] {type_str}: {lbl} -> {new_qs}")
                    
                    sdb[lbl] = {
                        "mq_url": getattr(self, 'last_mq_url', saved.get("mq_url")),
                        "qualities": list(curr_qs)
                    }
                    
                    time_str = datetime.now().strftime('%I:%M %p')
                    msg = f"[{type_str}] {title[:30]}... {lbl} | {u.get('language','HINDI')} | Added: {', '.join(new_qs)} ({time_str})"
                    notifs.append(msg)

        self.save_tracker_db(db, db_file)
        return notifs


