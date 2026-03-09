import os
import io
import sys
import time
import math
import asyncio
import aiohttp
from contextlib import redirect_stdout
from pyrogram import Client, filters
from pyrogram.types import Message

# Import your custom modules
from HindiAnimeZone import HindiAnimeZone
from RareAnimes import RareAnimes

API_ID = int(os.environ.get("API_ID", "1234567"))
API_HASH = os.environ.get("API_HASH", "your_hash")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "your_token")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
AUTH_CHAT = int(os.environ.get("AUTH_CHAT", "0"))

app = Client(
    "anime_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

def is_authorized(message: Message) -> bool:
    user_id = message.from_user.id if message.from_user else 0
    chat_id = message.chat.id if message.chat else 0
    
    if user_id == OWNER_ID:
        return True
    if chat_id == AUTH_CHAT:
        return True
    return False

def humanbytes(size):
    if not size: return ""
    power = 2**10
    n = 0
    Dic_powerN = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return str(round(size, 2)) + " " + Dic_powerN[n] + 'B'

def time_formatter(milliseconds: int) -> str:
    seconds, milliseconds = divmod(int(milliseconds), 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    tmp = ((str(days) + "d, ") if days else "") + \
          ((str(hours) + "h, ") if hours else "") + \
          ((str(minutes) + "m, ") if minutes else "") + \
          ((str(seconds) + "s, ") if seconds else "")
    return tmp[:-2] if tmp else "0s"

async def progress_for_pyrogram(current, total, ud_type, message, start):
    now = time.time()
    diff = now - start
    if round(diff % 5.00) == 0 or current == total:
        percentage = current * 100 / total if total else 0
        speed = current / diff if diff > 0 else 0
        elapsed_time = round(diff) * 1000
        time_to_completion = round((total - current) / speed) * 1000 if speed > 0 else 0
        estimated_total_time = elapsed_time + time_to_completion

        elapsed_time = time_formatter(elapsed_time)
        estimated_total_time = time_formatter(estimated_total_time)

        progress = "[{0}{1}] \n**Progress**: {2}%\n".format(
            ''.join(["█" for i in range(math.floor(percentage / 10))]),
            ''.join(["░" for i in range(10 - math.floor(percentage / 10))]),
            round(percentage, 2))

        tmp = progress + "{0} of {1}\n**Speed:** {2}/s\n**ETA:** {3}\n".format(
            humanbytes(current),
            humanbytes(total),
            humanbytes(speed),
            estimated_total_time if estimated_total_time != '' else "0 s"
        )
        try:
            await message.edit(f"{ud_type}\n{tmp}")
        except Exception:
            pass

async def download_file(url, file_name, status_msg):
    start_t = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                last_update = time.time()
                
                with open(file_name, 'wb') as f:
                    async for chunk in response.content.iter_chunked(1024 * 1024):
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        now = time.time()
                        if now - last_update > 5:
                            last_update = now
                            percentage = downloaded * 100 / total_size if total_size else 0
                            try:
                                await status_msg.edit(
                                    f"📥 **Downloading...** {round(percentage, 2)}%\n"
                                    f"{humanbytes(downloaded)} / {humanbytes(total_size)}"
                                )
                            except Exception: pass
        return True
    except Exception as e:
        await status_msg.edit(f"❌ Download failed: {str(e)}")
        return False

@app.on_message(filters.command("start") & filters.incoming)
async def start_cmd(client, message):
    if not is_authorized(message):
        return await message.reply("⛔ Aap authorized nahi hain is bot ko use karne ke liye.")
    await message.reply("✅ Bot is alive and authorized!\n\n**Commands:**\n`/hindianime <url>`\n`/rareanime <url>`")

@app.on_message(filters.command("hindianime") & filters.incoming)
async def hindianime_cmd(client, message):
    if not is_authorized(message):
        return await message.reply("⛔ Unauthorized")
        
    if len(message.command) < 2:
        return await message.reply("Usage: `/hindianime <url>`")
        
    url = message.command[1]
    status_msg = await message.reply("🔄 Analyzing HindiAnimeZone link...")
    
    try:
        def bypass_func():
            f = io.StringIO()
            with redirect_stdout(f):
                bypasser = HindiAnimeZone()
                bypasser.pro_main_bypass(url)
            return f.getvalue()
            
        output = await asyncio.to_thread(bypass_func)
        
        links = []
        for line in output.split('\\n'):
            if '[PRO] FINAL DL: ' in line:
                links.append(line.split('[PRO] FINAL DL: ')[1].strip())
            
        if not links:
            return await status_msg.edit("❌ Koi download links nahi mile. API fail ho gayi ya structure change ho gaya.")
            
        await status_msg.edit(f"✅ Found {len(links)} links. Downloading first one...")
        
        dl_url = links[0]
        file_name = f"hindianime_{message.id}.mkv"
        
        success = await download_file(dl_url, file_name, status_msg)
        if not success: return
        
        await status_msg.edit("📤 **Uploading to Telegram...**")
        start_time = time.time()
        
        await client.send_document(
            chat_id=message.chat.id,
            document=file_name,
            caption="Downloaded via @HindiAnimeZone",
            progress=progress_for_pyrogram,
            progress_args=("📤 **Uploading to Telegram...**", status_msg, start_time)
        )
        
        await status_msg.delete()
        os.remove(file_name)
            
    except Exception as e:
        await status_msg.edit(f"❌ Error occurred: {str(e)}")
        file_path = f"hindianime_{message.id}.mkv"
        if os.path.exists(file_path):
            os.remove(file_path)

@app.on_message(filters.command("rareanime") & filters.incoming)
async def rareanime_cmd(client, message):
    if not is_authorized(message):
        return await message.reply("⛔ Unauthorized")
        
    if len(message.command) < 2:
        return await message.reply("Usage: `/rareanime <url>`")
        
    url = message.command[1]
    status_msg = await message.reply("🔄 Analyzing RareAnimes link...")
    
    try:
        def bypass_func():
            bypasser = RareAnimes()
            return bypasser.get_links(url, verbose=False)
            
        res = await asyncio.to_thread(bypass_func)
        
        if "error" in res and not res.get("episodes"):
            return await status_msg.edit(f"❌ API fail ho gaya. Error: {res.get('error')}")
            
        episodes = res.get("episodes", [])
        if not episodes:
            return await status_msg.edit("❌ Koi episodes nahi mile.")
            
        first_ep = episodes[0]
        downloads = first_ep.get("downloads", [])
        
        if not downloads:
            return await status_msg.edit("❌ Koi download link generate nahi hua.")
            
        dl_url = downloads[0]
        if isinstance(dl_url, str) and ': http' in dl_url:
            dl_url = dl_url.split(': http')[1].strip()
            dl_url = 'http' + dl_url
        elif isinstance(dl_url, dict) and 'url' in dl_url:
            dl_url = dl_url['url']
            
        await status_msg.edit(f"✅ Found episode link. Downloading...")
        
        file_name = f"rareanime_{message.id}.mkv"
        
        success = await download_file(dl_url, file_name, status_msg)
        if not success: return
        
        await status_msg.edit("📤 **Uploading to Telegram...**")
        start_time = time.time()
        
        await client.send_document(
            chat_id=message.chat.id,
            document=file_name,
            caption="Downloaded via RareAnimes",
            progress=progress_for_pyrogram,
            progress_args=("📤 **Uploading to Telegram...**", status_msg, start_time)
        )
        
        await status_msg.delete()
        os.remove(file_name)
            
    except Exception as e:
        await status_msg.edit(f"❌ Error occurred: {str(e)}")
        file_path = f"rareanime_{message.id}.mkv"
        if os.path.exists(file_path):
            os.remove(file_path)

if __name__ == "__main__":
    print("Starting bot...")
    app.run()
