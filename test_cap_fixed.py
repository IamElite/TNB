
import os
import re
import sys

# Mocking bot functions since I can't easily import from bot.py in a simple way while keeping paths clean
def clean_unwanted_tags(text):
    if not text: return ""
    text = re.sub(r'@[\w_]+', '', text)
    text = re.sub(r'\[.*?\]', '', text)
    text = re.sub(r'\(.*?\)', '', text)
    text = re.sub(r'www\.\S+', '', text)
    noise = r'Dubbed|Hindi|Dual|Audio|Multi|UNCUT|Eng|Sub|Soft|Hard|ESub|HSub|SD|HD|FHD|4K|Ultra|Web-DL|BluRay|x264|x265|HEVC|Episodes?|Downloads?|Full|Series|Zon-E'
    text = re.sub(noise, '', text, flags=re.I)
    text = re.sub(r'[\_\.\-]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_filename(basename):
    data = {"name": "Unknown", "season": None, "episode": None, "year": None, "is_movie": False}
    yr_match = re.search(r'\(?(19|20)\d{2}\)?', basename)
    if yr_match:
        data["year"] = yr_match.group(0).strip('()')
    if any(x in basename.lower() for x in ["movie", "film"]):
        data["is_movie"] = True
    s_match = re.search(r'Season\s*(\d+)', basename, re.I) or \
              re.search(r'S(\d+)', basename, re.I) or \
              re.search(r'(\d+)(?:st|nd|rd|th)\s*Season', basename, re.I)
    if s_match:
        data["season"] = s_match.group(1).zfill(2)
    ep_match = re.search(r'E(\d+)', basename, re.I) or \
               re.search(r'Episode\s*(\d+)', basename, re.I) or \
               re.search(r'Ep[\.\s]*(\d+)', basename, re.I)
    if ep_match:
        data["episode"] = ep_match.group(1).zfill(2)
    else:
        # Standalone number check: "Solo Leveling 01" or "SL_01" -> 01
        nums = re.findall(r'(?:[\s\_\.\-]|^)(\d{1,4})(?:\b|(?=[^\dp]))', basename)
        for n in nums:
            if n != data["year"] and int(n) < 2000:
                data["episode"] = n.zfill(2)
                break
    name_clean = basename
    name_clean = re.sub(r'\.(mp4|mkv|avi|webm)$', '', name_clean, flags=re.I)
    patterns_to_remove = [
        r'S\d+|Season\s*\d+|(\d+)(?:st|nd|rd|th)\s*Season',
        r'E\d+|Episode\s*\d+|Ep[\.\s]*\d+',
        r'\d{3,4}p',
        r'\(?(19|20)\d{2}\)?'
    ]
    if data["episode"]:
        ep_no = data["episode"].lstrip('0') or "0"
        patterns_to_remove.append(rf'(?:\s|^){ep_no}(?:\b|(?=[^\dp]))')
        patterns_to_remove.append(rf'(?:\s|^){data["episode"]}(?:\b|(?=[^\dp]))')
    for p in patterns_to_remove:
        name_clean = re.sub(p, '', name_clean, flags=re.I)
    name_clean = clean_unwanted_tags(name_clean)
    if name_clean:
        name_clean = re.sub(r'^[ \.\_\-]+|[ \.\_\-]+$', '', name_clean)
        name_clean = re.sub(r'[\.\-\_]{2,}', '-', name_clean)
        name_clean = re.sub(r'\s+', ' ', name_clean)
        data["name"] = name_clean.strip()
    return data

def make_caption(filepath, series_info=None):
    basename = os.path.basename(filepath)
    info = parse_filename(basename)
    series_name = None
    if series_info:
        # Avoid "Attack on Titan Episodes Download" -> "Attack on Titan"
        series_name_dirty = series_info.split("|")[0].split("-")[0].strip()
        series_info_parsed = parse_filename(series_name_dirty)
        series_name = clean_unwanted_tags(series_info_parsed['name'])
    
    file_name_parsed = clean_unwanted_tags(info['name'])
    
    # Logic: Prefer series_name from page for the "Title" if file name is generic or missing
    # But always keep Season and Episode from filename if found.
    if series_name and (len(series_name) > len(file_name_parsed) or file_name_parsed in ["Unknown", "Download"]):
        display_name = series_name
    elif file_name_parsed and file_name_parsed != "Unknown":
        display_name = file_name_parsed
    else:
        display_name = series_name or "Unknown Anime"
    
    # Final check: if DISPLAY NAME is just a number or too short, fallback to series_info
    if len(display_name) < 4 or display_name.isdigit():
        display_name = series_name or display_name
    
    if series_info:
        info_alt = parse_filename(series_info)
        if not info['season']: info['season'] = info_alt['season']
        if not info['year']: info['year'] = info_alt['year']
        if not info['is_movie']: info['is_movie'] = info_alt['is_movie']
    
    display_name = clean_unwanted_tags(display_name)
    
    cap = f"🎬 **{display_name}**"
    if info['year']: cap += f" ({info['year']})"
    cap += f"\n╭━━━━━━━━━━━━━━━━━━━╮"
    if info['season']:
        cap += f"\n│ ✨ **season :** {info['season']}"
    cap += f"\n│ 📺 **Episode:** {info['episode'] if info['episode'] else 'N/A'}"
    cap += "\n╰━━━━━━━━━━━━━━━━━━━╯"
    return cap

if __name__ == "__main__":
    # The user's specific scenario:
    # Full Name scraped from page: "Attack on Titan"
    # Fallback filename: "download_128579_1_360P_SD.mkv"
    
    s_info = "Attack on Titan"
    f_name = "download_128579_1_360P_SD.mkv"
    
    print("--- USER SCENARIO: Full Name + Fallback Filename ---")
    print(make_caption(f_name, series_info=s_info))
    
    # Scenario: Page title is messy but Full Name is clean
    s_info_messy = "Attack on Titan Season 1 Hindi Dubbed Episodes Download HD"
    print("\n--- MESSY PAGE TITLE FALLBACK ---")
    print(make_caption(f_name, series_info=s_info_messy))
