# bot.py - 413 LARGE FILE FIX
# Bot API limit 50MB - Large files userbot se send karo
# ============================================

import os
import re
import asyncio
import logging
import time
import signal
import sys
from datetime import datetime
from typing import Optional, Tuple, List

from pyrogram import Client as PyroClient
from pyrogram.types import Message as PyroMessage
from pyrogram.errors import (
    FloodWait, ChannelPrivate,
    PeerIdInvalid, UserNotParticipant,
)
from pyrogram.enums import MessageMediaType

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    CallbackQueryHandler
)
from telegram.constants import ParseMode

# ============================================
# ⚙️ CONFIG
# ============================================

API_ID       = 31110304
API_HASH     = "8945c9b99de5dcc82fa8520c077f5303"
SESSION_STRING = "BQHatKAAovmdkLvCXvIQn81VNKw6tYJvSIrGUV2PdxalekOdDYBc68DWBVVwvZJdn6HHDvFengpN0qr9gQZvCvlQ0PhmPWN8YRthYtaWJIFPuMUPBf-nBZnVxfoe0xVBoRVlVspQVcCBQlFGrXIGjl8VTCk1RHGkulQxM4getei-bOvKih_UyOXTVu5H3aL8A0cORKZLjVwNWxoTKWTD15vQZxa-6w1vIXErevA3t4AGDpeLd4BmrOMBLuQ19m5TTnXJs9ErUhoUPNqmwLAhxXdu_-DK8ibcoqd53aJB7RMrNxr5bAmUcLI4tuicnYEjkkkDP5ASKZONTsjxCXZVn4t2Mxz-uQAAAAF84uDeAA"
BOT_TOKEN    = "8731944731:AAGHNn-jvpHNK40V65PMqJp6aMu6A65amQE"
OWNER_ID     = 6390210782
AUTHORIZED_USERS = {OWNER_ID}

# Bot API file size limit = 50MB
# Files bigger than this → userbot sends directly
BOT_API_LIMIT = 50 * 1024 * 1024   # 50 MB in bytes

# ============================================
# LOGGER
# ============================================

logging.basicConfig(
    format   = "%(asctime)s | %(levelname)s | %(message)s",
    level    = logging.INFO,
    handlers = [logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SaveBot")

# ============================================
# PYROGRAM CLIENT
# ============================================

pyro: Optional[PyroClient] = None
pyro_started: bool = False
peer_cache: dict = {}


def make_client() -> PyroClient:
    return PyroClient(
        name           = "userbot",
        api_id         = API_ID,
        api_hash       = API_HASH,
        session_string = SESSION_STRING,
        no_updates     = True,
        in_memory      = True,
    )


async def start_pyro() -> bool:
    global pyro, pyro_started
    if pyro_started and pyro and pyro.is_connected:
        return True
    try:
        if pyro is None:
            pyro = make_client()
        await pyro.start()
        pyro_started = True
        me = await pyro.get_me()
        logger.info(
            f"✅ Userbot → {me.first_name} "
            f"(@{me.username}) [ID:{me.id}]"
        )
        return True
    except Exception as e:
        logger.error(f"❌ Start failed: {e}")
        pyro_started = False
        return False


async def stop_pyro():
    global pyro, pyro_started
    if not pyro_started or pyro is None:
        return
    try:
        if pyro.is_connected:
            await pyro.stop()
    except Exception as e:
        logger.warning(f"stop_pyro: {e}")
    finally:
        pyro_started = False


def is_ready() -> bool:
    return pyro_started and pyro is not None and pyro.is_connected

# ============================================
# DATABASE
# ============================================

class DB:
    def __init__(self):
        self.users:            dict = {}
        self.forward_channels: dict = {}
        self.stats = {
            "total_saved":   0,
            "total_batches": 0,
            "start_time":    datetime.now(),
        }

    def add_user(self, uid: int, uname: str = None):
        if uid not in self.users:
            self.users[uid] = {
                "username": uname,
                "joined":   datetime.now(),
                "saved":    0,
            }

    def set_forward(self, uid: int, cid: int):
        self.forward_channels[uid] = cid

    def get_forward(self, uid: int) -> Optional[int]:
        return self.forward_channels.get(uid)

    def inc(self, uid: int, n: int = 1):
        if uid in self.users:
            self.users[uid]["saved"] += n
        self.stats["total_saved"] += n


db = DB()

# ============================================
# PEER RESOLVER
# ============================================

async def get_input_peer(bare_id: str):
    bare = str(bare_id).strip().lstrip("-")

    if bare in peer_cache:
        return peer_cache[bare]

    candidates = [
        int(f"-100{bare}"),
        int(bare),
        -int(bare),
    ]

    # Try resolve_peer first (uses session internal db)
    for cid in candidates:
        try:
            await pyro.resolve_peer(cid)
            peer_cache[bare] = cid
            logger.info(f"resolve_peer ok: {cid}")
            return cid
        except Exception:
            pass

    # Try get_chat
    for cid in candidates:
        try:
            chat = await pyro.get_chat(cid)
            real = chat.id
            peer_cache[bare] = real
            logger.info(f"get_chat ok: {cid} → {real} [{chat.title}]")
            return real
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return await get_input_peer(bare_id)
        except Exception as e:
            logger.debug(f"get_chat({cid}): {e}")

    # Search dialogs
    try:
        target = int(f"-100{bare}")
        async for dialog in pyro.get_dialogs():
            if dialog.chat and dialog.chat.id == target:
                peer_cache[bare] = target
                logger.info(f"Found in dialogs: {target}")
                return target
    except Exception as e:
        logger.warning(f"Dialog search: {e}")

    fallback = int(f"-100{bare}")
    peer_cache[bare] = fallback
    return fallback


async def fetch_message_proper(peer, msg_id: int) -> Optional[PyroMessage]:
    try:
        msg = await pyro.get_messages(peer, msg_id)
        if msg and not msg.empty:
            return msg
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await fetch_message_proper(peer, msg_id)
    except Exception as e:
        logger.warning(f"get_messages(single): {e}")

    try:
        msgs = await pyro.get_messages(peer, [msg_id])
        if isinstance(msgs, list) and msgs:
            m = msgs[0]
            if m and not m.empty:
                return m
    except Exception as e:
        logger.warning(f"get_messages(list): {e}")

    try:
        async for m in pyro.get_chat_history(
            peer, limit=1, offset_id=msg_id + 1
        ):
            if m.id == msg_id:
                return m
    except Exception as e:
        logger.warning(f"get_chat_history: {e}")

    return None


async def fetch_messages_batch_proper(
    peer, msg_ids: List[int]
) -> List[PyroMessage]:
    try:
        result = await pyro.get_messages(peer, msg_ids)
        if isinstance(result, list):
            return [m for m in result if m and not m.empty]
        if result and not result.empty:
            return [result]
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await fetch_messages_batch_proper(peer, msg_ids)
    except Exception as e:
        logger.error(f"fetch_batch: {e}")
    return []

# ============================================
# LINK PARSER
# ============================================

def parse_link(link: str) -> Optional[Tuple]:
    link = link.strip()
    PATTERNS = [
        (r"https?://(?:www\.)?t\.me/c/(\d+)/(\d+)",                            True),
        (r"https?://(?:www\.)?t\.me/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)",         False),
        (r"https?://(?:www\.)?telegram\.me/c/(\d+)/(\d+)",                      True),
        (r"https?://(?:www\.)?telegram\.me/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)",  False),
        (r"https?://(?:www\.)?telegram\.dog/c/(\d+)/(\d+)",                     True),
        (r"https?://(?:www\.)?telegram\.dog/([a-zA-Z][a-zA-Z0-9_]{2,})/(\d+)", False),
    ]
    for pat, private in PATTERNS:
        m = re.match(pat, link)
        if m:
            raw    = m.group(1)
            msg_id = int(m.group(2))
            return raw, msg_id, private
    return None


def validate_batch(sl: str, el: str) -> Optional[Tuple]:
    s = parse_link(sl)
    e = parse_link(el)
    if not s or not e:
        return None
    s_raw, s_id, s_priv = s
    e_raw, e_id, e_priv = e
    if s_raw != e_raw or s_priv != e_priv:
        return None
    if s_id > e_id:
        s_id, e_id = e_id, s_id
    return s_raw, s_priv, s_id, e_id


def parse_channel_link(link: str) -> Optional[any]:
    link = link.strip()
    if re.match(r"^-?\d+$", link):
        s = link
        if s.startswith("-100"): return int(s)
        if s.startswith("-"):    return int(f"-100{s.lstrip('-')}")
        return int(f"-100{s}")
    if link.startswith("@"):
        return link[1:]
    PATTERNS = [
        (r"https?://(?:www\.)?t\.me/c/(\d+)(?:/\d+)?",                           True),
        (r"https?://(?:www\.)?t\.me/([a-zA-Z][a-zA-Z0-9_]{2,})(?:/\d+)?",        False),
        (r"https?://(?:www\.)?telegram\.me/c/(\d+)(?:/\d+)?",                     True),
        (r"https?://(?:www\.)?telegram\.me/([a-zA-Z][a-zA-Z0-9_]{2,})(?:/\d+)?", False),
    ]
    for pat, private in PATTERNS:
        m = re.match(pat, link)
        if m:
            raw = m.group(1)
            return int(f"-100{raw}") if private else raw
    return None

# ============================================
# FILE SIZE CHECKER
# ============================================

def get_file_size(msg: PyroMessage) -> int:
    """Message se file size nikalo."""
    mt = msg.media
    try:
        if mt == MessageMediaType.PHOTO:
            # Photo ka last (largest) size
            if msg.photo and msg.photo.file_size:
                return msg.photo.file_size
            return 0
        elif mt == MessageMediaType.VIDEO:
            return getattr(msg.video, "file_size", 0) or 0
        elif mt == MessageMediaType.DOCUMENT:
            return getattr(msg.document, "file_size", 0) or 0
        elif mt == MessageMediaType.AUDIO:
            return getattr(msg.audio, "file_size", 0) or 0
        elif mt == MessageMediaType.VOICE:
            return getattr(msg.voice, "file_size", 0) or 0
        elif mt == MessageMediaType.VIDEO_NOTE:
            return getattr(msg.video_note, "file_size", 0) or 0
        elif mt == MessageMediaType.STICKER:
            return getattr(msg.sticker, "file_size", 0) or 0
        elif mt == MessageMediaType.ANIMATION:
            return getattr(msg.animation, "file_size", 0) or 0
    except Exception:
        pass
    return 0


def format_size(size_bytes: int) -> str:
    """Human readable size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.1f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.1f} GB"

# ============================================
# SEND ENGINE - THE 413 FIX
# ============================================

_SEM = asyncio.Semaphore(2)


async def process_and_send(
    msg:         PyroMessage,
    bot,
    bot_chat_id: int,
    fwd_channel: int = None,
) -> dict:
    res = {
        "success": False,
        "type":    None,
        "error":   None,
        "caption": None,
        "msg_id":  getattr(msg, "id", None),
        "size":    "—",
    }

    if not msg:
        res["error"] = "Empty message"
        return res

    async with _SEM:
        try:
            caption = msg.caption or msg.text or ""

            # ── Pure text ─────────────────────────────────────────
            if not msg.media and msg.text:
                await bot.send_message(
                    chat_id    = bot_chat_id,
                    text       = msg.text,
                    parse_mode = None,
                )
                if fwd_channel:
                    try:
                        await pyro.send_message(fwd_channel, msg.text)
                    except Exception as ex:
                        logger.warning(f"fwd text: {ex}")
                res.update(success=True, type="text", caption=msg.text[:200])
                return res

            # ── Media ─────────────────────────────────────────────
            if msg.media:
                res = await _handle_media(
                    msg, bot, bot_chat_id, fwd_channel, caption, res
                )
                return res

            res["error"] = "Unsupported type"

        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            return await process_and_send(msg, bot, bot_chat_id, fwd_channel)
        except Exception as e:
            res["error"] = str(e)
            logger.error(f"process_and_send: {e}")

    return res


async def _handle_media(msg, bot, bot_chat_id, fwd_channel, caption, res):
    os.makedirs("downloads", exist_ok=True)
    fp = tp = None

    try:
        # File size check BEFORE downloading
        declared_size = get_file_size(msg)
        is_large      = declared_size > BOT_API_LIMIT
        mt            = msg.media

        logger.info(
            f"Msg {msg.id} | type={mt} | "
            f"size={format_size(declared_size)} | "
            f"large={is_large}"
        )

        # ── Download via userbot ──────────────────────────────────
        fp = await pyro.download_media(
            msg,
            file_name = f"downloads/{msg.id}_"
        )
        if not fp or not os.path.exists(fp):
            res["error"] = "Download failed"
            return res

        # Actual file size after download
        actual_size = os.path.getsize(fp)
        is_large    = actual_size > BOT_API_LIMIT

        logger.info(
            f"Downloaded: {os.path.basename(fp)} "
            f"({format_size(actual_size)}) "
            f"large={is_large}"
        )
        res["size"] = format_size(actual_size)

        # Thumbnail
        if mt == MessageMediaType.VIDEO:
            if msg.video and getattr(msg.video, "thumbs", None):
                try:
                    tp = await pyro.download_media(
                        msg.video.thumbs[0].file_id,
                        file_name = f"downloads/th_{msg.id}_"
                    )
                except Exception:
                    tp = None

        if is_large:
            # ── LARGE FILE: Userbot se directly send karo ─────────
            logger.info(
                f"Large file ({format_size(actual_size)}) → "
                f"sending via userbot directly"
            )

            # Send to bot_chat via userbot
            await _send_via_userbot(
                bot_chat_id, fp, tp, mt, msg, caption
            )

            # Send to fwd_channel via userbot
            if fwd_channel:
                try:
                    await _send_via_userbot(
                        fwd_channel, fp, tp, mt, msg, caption
                    )
                except Exception as ex:
                    logger.warning(f"Userbot fwd large: {ex}")

        else:
            # ── SMALL FILE: Bot API se send karo ──────────────────
            logger.info(
                f"Small file ({format_size(actual_size)}) → "
                f"sending via bot API"
            )

            await _send_via_bot_api(
                bot, bot_chat_id, fp, tp, mt, msg, caption
            )

            if fwd_channel:
                try:
                    await _send_via_userbot(
                        fwd_channel, fp, tp, mt, msg, caption
                    )
                except Exception as ex:
                    logger.warning(f"Userbot fwd: {ex}")
                    try:
                        await _send_via_bot_api(
                            bot, fwd_channel, fp, tp, mt, msg, caption
                        )
                    except Exception as ex2:
                        logger.warning(f"Bot API fwd: {ex2}")

        mt_name = mt.name.lower() if mt else "unknown"
        res.update(
            success = True,
            type    = mt_name,
            caption = caption[:200] if caption else "—",
        )

    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        raise
    except Exception as e:
        res["error"] = str(e)
        logger.error(f"_handle_media: {e}")
    finally:
        for p in (fp, tp):
            if p and os.path.exists(p):
                try:
                    os.remove(p)
                except Exception:
                    pass

    return res


async def _send_via_userbot(chat_id, fp, tp, mt, msg, caption):
    """
    Userbot se send = no 'Forwarded from' + no 50MB limit
    Pyrogram MTProto supports up to 2GB files
    """
    cap = caption if caption else None
    kw  = dict(
        chat_id          = chat_id,
        caption          = cap,
        caption_entities = getattr(msg, "caption_entities", None),
    )

    if mt == MessageMediaType.PHOTO:
        await pyro.send_photo(photo=fp, **kw)

    elif mt == MessageMediaType.VIDEO:
        await pyro.send_video(
            video              = fp,
            thumb              = tp,
            duration           = getattr(msg.video,  "duration", 0) if msg.video else 0,
            width              = getattr(msg.video,  "width",    0) if msg.video else 0,
            height             = getattr(msg.video,  "height",   0) if msg.video else 0,
            supports_streaming = True,
            **kw,
        )

    elif mt == MessageMediaType.DOCUMENT:
        await pyro.send_document(
            document  = fp,
            file_name = getattr(msg.document, "file_name", None) if msg.document else None,
            **kw,
        )

    elif mt == MessageMediaType.AUDIO:
        await pyro.send_audio(
            audio     = fp,
            duration  = getattr(msg.audio, "duration",  0)   if msg.audio else 0,
            performer = getattr(msg.audio, "performer", None) if msg.audio else None,
            title     = getattr(msg.audio, "title",     None) if msg.audio else None,
            **kw,
        )

    elif mt == MessageMediaType.VOICE:
        await pyro.send_voice(
            voice    = fp,
            duration = getattr(msg.voice, "duration", 0) if msg.voice else 0,
            **kw,
        )

    elif mt == MessageMediaType.VIDEO_NOTE:
        await pyro.send_video_note(
            chat_id    = chat_id,
            video_note = fp,
            duration   = getattr(msg.video_note, "duration", 0) if msg.video_note else 0,
            length     = getattr(msg.video_note, "length",   0) if msg.video_note else 0,
        )

    elif mt == MessageMediaType.STICKER:
        await pyro.send_sticker(chat_id=chat_id, sticker=fp)

    elif mt == MessageMediaType.ANIMATION:
        await pyro.send_animation(animation=fp, **kw)

    else:
        await pyro.send_document(document=fp, **kw)


async def _send_via_bot_api(bot, chat_id, fp, tp, mt, msg, caption):
    """Bot API se send (only for files < 50MB)."""
    cap = caption if caption else None

    with open(fp, "rb") as f:
        data = f.read()

    if mt == MessageMediaType.PHOTO:
        await bot.send_photo(
            chat_id = chat_id,
            photo   = data,
            caption = cap,
        )

    elif mt == MessageMediaType.VIDEO:
        thumb_data = None
        if tp and os.path.exists(tp):
            with open(tp, "rb") as tf:
                thumb_data = tf.read()
        await bot.send_video(
            chat_id            = chat_id,
            video              = data,
            caption            = cap,
            duration           = getattr(msg.video, "duration", 0) if msg.video else 0,
            width              = getattr(msg.video, "width",    0) if msg.video else 0,
            height             = getattr(msg.video, "height",   0) if msg.video else 0,
            thumbnail          = thumb_data,
            supports_streaming = True,
        )

    elif mt == MessageMediaType.DOCUMENT:
        await bot.send_document(
            chat_id  = chat_id,
            document = data,
            caption  = cap,
            filename = getattr(msg.document, "file_name", "file") if msg.document else "file",
        )

    elif mt == MessageMediaType.AUDIO:
        await bot.send_audio(
            chat_id   = chat_id,
            audio     = data,
            caption   = cap,
            duration  = getattr(msg.audio, "duration",  0)   if msg.audio else 0,
            performer = getattr(msg.audio, "performer", None) if msg.audio else None,
            title     = getattr(msg.audio, "title",     None) if msg.audio else None,
        )

    elif mt == MessageMediaType.VOICE:
        await bot.send_voice(
            chat_id  = chat_id,
            voice    = data,
            caption  = cap,
            duration = getattr(msg.voice, "duration", 0) if msg.voice else 0,
        )

    elif mt == MessageMediaType.VIDEO_NOTE:
        await bot.send_video_note(
            chat_id    = chat_id,
            video_note = data,
            duration   = getattr(msg.video_note, "duration", 0) if msg.video_note else 0,
            length     = getattr(msg.video_note, "length",   0) if msg.video_note else 0,
        )

    elif mt == MessageMediaType.STICKER:
        await bot.send_sticker(
            chat_id = chat_id,
            sticker = data,
        )

    elif mt == MessageMediaType.ANIMATION:
        await bot.send_animation(
            chat_id   = chat_id,
            animation = data,
            caption   = cap,
        )

    else:
        await bot.send_document(
            chat_id  = chat_id,
            document = data,
            caption  = cap,
        )

# ============================================
# HELPERS
# ============================================

def auth(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id
        if uid not in AUTHORIZED_USERS:
            await update.message.reply_text(
                f"⛔ Unauthorized\nID: `{uid}`",
                parse_mode = ParseMode.MARKDOWN,
            )
            return
        return await func(update, context)
    return wrapper


def pbar(cur: int, tot: int, w: int = 20) -> str:
    if tot == 0:
        return "░" * w
    f = int(w * cur / tot)
    return f"[{'█'*f}{'░'*(w-f)}] {cur/tot*100:.1f}%"


async def sedit(msg, text: str):
    try:
        await msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        if "not modified" not in str(e).lower():
            logger.warning(f"sedit: {e}")


async def not_ready_reply(update: Update):
    await update.message.reply_text(
        "❌ Userbot connected nahi.\n/reconnect karo.",
        parse_mode = ParseMode.MARKDOWN,
    )

# ============================================
# /start
# ============================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db.add_user(user.id, user.username)
    ub  = "🟢 Online" if is_ready() else "🔴 Offline"
    fwd = db.get_forward(user.id)
    kb  = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📖 Help",     callback_data="help"),
            InlineKeyboardButton("📊 Stats",    callback_data="stats"),
        ],
        [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
    ])
    await update.message.reply_text(
        f"🔥 *Restricted Content Saver*\n\n"
        f"Userbot : {ub}\n"
        f"Forward : {f'`{fwd}`' if fwd else 'Set nahi'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📎 `/test <link>`\n"
        f"📦 `/batch <start> <end>`\n"
        f"📢 `/forward <channel>`\n"
        f"🔄 `/reconnect`\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ Large files (>50MB) supported\n"
        f"✅ No 'Forwarded from' tag\n"
        f"✅ All media types supported\n\n"
        f"🆔 ID: `{user.id}`",
        parse_mode   = ParseMode.MARKDOWN,
        reply_markup = kb,
    )

# ============================================
# /help
# ============================================

@auth
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Help*\n\n"
        "📎 `/test <link>` – Single message\n"
        "📦 `/batch <start> <end>` – Range save\n"
        "📢 `/forward <channel>` – Forward channel\n"
        "🔄 `/reconnect` – Userbot restart\n"
        "📊 `/stats` – Statistics\n\n"
        "*File size handling:*\n"
        "• `< 50MB` → Bot API se DM mein\n"
        "• `> 50MB` → Userbot se directly\n"
        "• Max size: ~2GB (Telegram limit)\n\n"
        "*Private link:*\n"
        "`https://t.me/c/CHATID/MSGID`\n\n"
        "*Public link:*\n"
        "`https://t.me/username/MSGID`",
        parse_mode = ParseMode.MARKDOWN,
    )

# ============================================
# /stats
# ============================================

@auth
async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    up   = datetime.now() - db.stats["start_time"]
    h, r = divmod(int(up.total_seconds()), 3600)
    m, s = divmod(r, 60)
    ub   = "🟢 Online" if is_ready() else "🔴 Offline"
    await update.message.reply_text(
        f"📊 *Stats*\n\n"
        f"Userbot      : {ub}\n"
        f"Cached peers : `{len(peer_cache)}`\n"
        f"👥 Users     : `{len(db.users)}`\n"
        f"💾 Saved     : `{db.stats['total_saved']}`\n"
        f"📦 Batches   : `{db.stats['total_batches']}`\n"
        f"⏱ Uptime    : `{h}h {m}m {s}s`",
        parse_mode = ParseMode.MARKDOWN,
    )

# ============================================
# /reconnect
# ============================================

@auth
async def cmd_reconnect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global pyro, peer_cache
    msg = await update.message.reply_text("🔄 Reconnect ho raha hai …")
    await stop_pyro()
    peer_cache = {}
    pyro       = make_client()
    ok         = await start_pyro()
    if ok:
        await sedit(msg, "✅ *Userbot reconnect ho gaya!*")
    else:
        await sedit(msg, "❌ *Reconnect fail.*\nSESSION\\_STRING check karo.")

# ============================================
# /forward
# ============================================

@auth
async def cmd_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not context.args:
        cur = db.get_forward(uid)
        await update.message.reply_text(
            f"📢 *Forward Channel Set Karo*\n\n"
            f"Bot ko us channel mein admin chahiye.\n\n"
            f"Usage:\n"
            f"• `/forward @username`\n"
            f"• `/forward -1001234567890`\n"
            f"• `/forward https://t.me/channel`\n"
            f"• `/forward off`\n\n"
            f"Abhi: {f'`{cur}`' if cur else 'Set nahi'}",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    arg = context.args[0].strip()

    if arg.lower() in ("off", "remove", "none", "clear", "0"):
        db.forward_channels.pop(uid, None)
        await update.message.reply_text("✅ Forward channel hata diya.")
        return

    peer = parse_channel_link(arg)
    if peer is None and arg.startswith("@"):
        peer = arg[1:]
    if peer is None:
        await update.message.reply_text("❌ Invalid channel.")
        return

    st = await update.message.reply_text("🔄 Verify ho raha hai …")
    try:
        chat = await context.bot.get_chat(peer)
        test = await context.bot.send_message(
            chat.id, "✅ Bot verified! Delete ho raha hai …"
        )
        await context.bot.delete_message(chat.id, test.message_id)
        db.set_forward(uid, chat.id)
        await sedit(
            st,
            f"✅ *Forward channel set!*\n\n"
            f"📢 {chat.title}\n"
            f"🆔 `{chat.id}`",
        )
    except Exception as e:
        err = str(e)
        if "not found" in err.lower():
            await sedit(st, "❌ Channel nahi mila. Bot ko admin banao.")
        elif "rights" in err.lower() or "admin" in err.lower():
            await sedit(st, "❌ Bot ko admin chahiye (Post Messages).")
        else:
            await sedit(st, f"❌ `{e}`")

# ============================================
# /test
# ============================================

@auth
async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.effective_user.id
    bot_chat = update.effective_chat.id

    if not is_ready():
        await not_ready_reply(update)
        return

    if not context.args:
        await update.message.reply_text(
            "📎 *Single Message Save*\n\n"
            "Usage: `/test <link>`\n\n"
            "`/test https://t.me/c/3124399297/8`",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    parsed = parse_link(context.args[0])
    if not parsed:
        await update.message.reply_text(
            "❌ Invalid link.\n"
            "Format: `https://t.me/c/CHATID/MSGID`",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    raw, msg_id, is_private = parsed
    fwd_channel = db.get_forward(uid)

    st = await update.message.reply_text(
        f"🔄 *Processing …*\n\n"
        f"Raw  : `{raw}`\n"
        f"MsgID: `{msg_id}`",
        parse_mode = ParseMode.MARKDOWN,
    )

    try:
        # Peer resolve
        if is_private:
            peer = await get_input_peer(raw)
        else:
            peer = raw

        await sedit(
            st,
            f"🔄 *Peer* → `{peer}`\n"
            f"Message fetch ho raha hai …"
        )

        # Fetch
        msg = await fetch_message_proper(peer, msg_id)
        if not msg:
            await sedit(
                st,
                "❌ *Message nahi mila.*\n\n"
                "Session account channel ka member hona chahiye.",
            )
            return

        # Size check
        declared = get_file_size(msg)
        mt       = msg.media.name if msg.media else "text"
        size_txt = format_size(declared) if declared else "unknown"
        method   = "Userbot (large)" if declared > BOT_API_LIMIT else "Bot API"

        await sedit(
            st,
            f"⬇️ *Downloading …*\n\n"
            f"Type   : `{mt}`\n"
            f"Size   : `{size_txt}`\n"
            f"Method : `{method}`"
        )

        # Download + Send
        res = await process_and_send(
            msg, context.bot, bot_chat, fwd_channel
        )

        if res["success"]:
            db.inc(uid)
            fwd_txt = (
                "\n📢 Forward channel mein bhi bheja!"
                if fwd_channel else ""
            )
            await sedit(
                st,
                f"✅ *Saved!*\n\n"
                f"Type    : `{res['type']}`\n"
                f"Size    : `{res['size']}`\n"
                f"Msg ID  : `{msg_id}`\n"
                f"Caption : {res['caption'] or '—'}"
                f"{fwd_txt}",
            )
        else:
            await sedit(st, f"❌ *Fail*\n\n`{res['error']}`")

    except PeerIdInvalid:
        await sedit(
            st,
            "❌ *Peer ID Invalid*\n\n"
            "Session account se channel open karo\n"
            "phir /reconnect karo.",
        )
    except UserNotParticipant:
        await sedit(st, "❌ Session account channel mein join karo.")
    except ChannelPrivate:
        await sedit(st, "❌ Session account se channel join karo.")
    except Exception as e:
        await sedit(st, f"❌ `{e}`")

# ============================================
# /batch
# ============================================

@auth
async def cmd_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    if not is_ready():
        await not_ready_reply(update)
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "📦 *Batch Save*\n\n"
            "Usage: `/batch <start> <end>`\n\n"
            "`/batch https://t.me/c/123/1 https://t.me/c/123/50`\n\n"
            "⚠️ Max 1000 msgs | Same channel",
            parse_mode = ParseMode.MARKDOWN,
        )
        return

    info = validate_batch(context.args[0], context.args[1])
    if not info:
        await update.message.reply_text(
            "❌ Invalid links ya different channels."
        )
        return

    raw, is_private, s_id, e_id = info
    total       = e_id - s_id + 1
    fwd_channel = db.get_forward(uid)

    if total > 1000:
        await update.message.reply_text(f"❌ {total} msgs – max 1000.")
        return

    bot_chat = update.effective_chat.id
    token    = f"{uid}_{int(time.time())}"
    context.bot_data[token] = {
        "raw":        raw,
        "is_private": is_private,
        "s_id":       s_id,
        "e_id":       e_id,
        "bot_chat":   bot_chat,
        "fwd":        fwd_channel,
    }

    fwd_txt = f"\n📢 Forward: `{fwd_channel}`" if fwd_channel else ""
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Start", callback_data=f"BATCH:{token}"),
        InlineKeyboardButton("❌ Cancel", callback_data="CANCEL"),
    ]])

    await update.message.reply_text(
        f"📦 *Batch Confirm*\n\n"
        f"Channel : `{raw}`\n"
        f"Range   : `{s_id}` → `{e_id}`\n"
        f"Total   : `{total}`{fwd_txt}\n"
        f"Est.    : ~{total * 2}s\n\n"
        f"✅ Large files automatically handled",
        parse_mode   = ParseMode.MARKDOWN,
        reply_markup = kb,
    )

# ============================================
# BATCH RUNNER
# ============================================

async def run_batch(
    q, context, uid,
    raw, is_private, s_id, e_id,
    bot_chat, fwd
):
    if not is_ready():
        await q.edit_message_text("❌ Userbot offline. /reconnect karo.")
        return

    total = e_id - s_id + 1

    await sedit(q.message, "🔄 *Peer resolve ho raha hai …*")

    if is_private:
        peer = await get_input_peer(raw)
    else:
        peer = raw

    await q.edit_message_text(
        f"🚀 *Batch Start!*\n\n"
        f"Peer  : `{peer}`\n"
        f"Total : `{total}`\n"
        f"{pbar(0, total)}\n"
        f"✅ 0  ❌ 0  ⏭ 0",
        parse_mode = ParseMode.MARKDOWN,
    )

    saved    = 0
    failed   = 0
    skipped  = 0
    t0       = time.time()
    last_edit= 0.0
    CHUNK    = 10

    for cs in range(s_id, e_id + 1, CHUNK):
        ce  = min(cs + CHUNK - 1, e_id)
        ids = list(range(cs, ce + 1))

        try:
            msgs = await fetch_messages_batch_proper(peer, ids)
        except Exception as e:
            logger.error(f"Chunk {cs}-{ce}: {e}")
            failed += len(ids)
            continue

        fetched  = {m.id for m in msgs}
        skipped += sum(1 for i in ids if i not in fetched)

        for msg in msgs:
            try:
                r = await process_and_send(
                    msg, context.bot, bot_chat, fwd
                )
                if r["success"]:
                    saved += 1
                else:
                    failed += 1
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
                try:
                    r = await process_and_send(
                        msg, context.bot, bot_chat, fwd
                    )
                    saved  += 1 if r["success"] else 0
                    failed += 0 if r["success"] else 1
                except Exception:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Msg {msg.id}: {e}")

            await asyncio.sleep(1.5)

        now = time.time()
        if now - last_edit >= 5:
            last_edit = now
            done    = saved + failed + skipped
            elapsed = now - t0
            speed   = done / elapsed if elapsed else 0
            eta     = (total - done) / speed if speed else 0
            try:
                await q.edit_message_text(
                    f"🔄 *Processing …*\n\n"
                    f"{pbar(done, total)}\n\n"
                    f"✅ {saved}  ❌ {failed}  ⏭ {skipped}\n\n"
                    f"⚡ {speed:.1f} msg/s  ⏱ ETA {int(eta)}s",
                    parse_mode = ParseMode.MARKDOWN,
                )
            except Exception:
                pass

    elapsed = max(time.time() - t0, 0.1)
    db.inc(uid, saved)
    db.stats["total_batches"] += 1
    fwd_txt = "\n📢 Forward ho gaya!" if fwd else ""

    try:
        await q.edit_message_text(
            f"🎉 *Batch Done!*\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ Saved   : *{saved}*\n"
            f"❌ Failed  : *{failed}*\n"
            f"⏭ Skipped : *{skipped}*\n"
            f"📦 Total   : *{total}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ {int(elapsed)}s  "
            f"⚡ {total/elapsed:.1f} msg/s"
            f"{fwd_txt}",
            parse_mode = ParseMode.MARKDOWN,
        )
    except Exception:
        pass

# ============================================
# CALLBACK HANDLER
# ============================================

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    data = q.data
    await q.answer()

    BACK = [[InlineKeyboardButton("🔙 Back", callback_data="menu_back")]]

    if data == "CANCEL":
        await q.edit_message_text("❌ Cancel ho gaya.")
        return

    if data.startswith("BATCH:"):
        token  = data[6:]
        params = context.bot_data.pop(token, None)
        if not params:
            await q.edit_message_text(
                "❌ Batch expire ho gaya.\n/batch dobara karo."
            )
            return
        await run_batch(
            q, context,
            q.from_user.id,
            params["raw"],
            params["is_private"],
            params["s_id"],
            params["e_id"],
            params["bot_chat"],
            params["fwd"],
        )
        return

    if data == "help":
        await q.edit_message_text(
            "📖 *Help*\n\n"
            "📎 `/test <link>`\n"
            "📦 `/batch <s> <e>`\n"
            "📢 `/forward <ch>`\n"
            "🔄 `/reconnect`\n"
            "📊 `/stats`\n\n"
            "*File handling:*\n"
            "< 50MB → Bot API\n"
            "> 50MB → Userbot MTProto\n"
            "Max → 2GB",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    elif data == "stats":
        up   = datetime.now() - db.stats["start_time"]
        h, r = divmod(int(up.total_seconds()), 3600)
        m, s = divmod(r, 60)
        ub   = "🟢 Online" if is_ready() else "🔴 Offline"
        await q.edit_message_text(
            f"📊 *Stats*\n\n"
            f"Userbot      : {ub}\n"
            f"Cached peers : `{len(peer_cache)}`\n"
            f"👥 Users     : `{len(db.users)}`\n"
            f"💾 Saved     : `{db.stats['total_saved']}`\n"
            f"📦 Batches   : `{db.stats['total_batches']}`\n"
            f"⏱ Uptime    : `{h}h {m}m {s}s`",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    elif data == "settings":
        uid = q.from_user.id
        fwd = db.get_forward(uid)
        await q.edit_message_text(
            f"⚙️ *Settings*\n\n"
            f"📢 Forward : {f'`{fwd}`' if fwd else 'Set nahi'}\n"
            f"🆔 ID      : `{uid}`\n\n"
            f"`/forward <ch>` – set\n"
            f"`/forward off` – hatao",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = InlineKeyboardMarkup(BACK),
        )

    elif data == "menu_back":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("📖 Help",     callback_data="help"),
                InlineKeyboardButton("📊 Stats",    callback_data="stats"),
            ],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        ])
        await q.edit_message_text(
            "🔥 *Restricted Content Saver*",
            parse_mode   = ParseMode.MARKDOWN,
            reply_markup = kb,
        )

# ============================================
# MAIN
# ============================================

async def run():
    os.makedirs("downloads", exist_ok=True)

    logger.info("Userbot start ho raha hai …")
    ok = await start_pyro()
    if not ok:
        logger.warning("⚠️ Userbot fail – /reconnect try karo")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",     cmd_start))
    app.add_handler(CommandHandler("help",      cmd_help))
    app.add_handler(CommandHandler("stats",     cmd_stats))
    app.add_handler(CommandHandler("reconnect", cmd_reconnect))
    app.add_handler(CommandHandler("forward",   cmd_forward))
    app.add_handler(CommandHandler("test",      cmd_test))
    app.add_handler(CommandHandler("batch",     cmd_batch))
    app.add_handler(CallbackQueryHandler(on_callback))

    await app.initialize()
    await app.start()
    await app.updater.start_polling(
        allowed_updates      = Update.ALL_TYPES,
        drop_pending_updates = True,
    )
    logger.info("🚀 Bot chal raha hai!")

    stop_event = asyncio.Event()

    def _sig(*_):
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig)
        except (NotImplementedError, RuntimeError):
            pass

    await stop_event.wait()

    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    await stop_pyro()
    logger.info("✅ Band ho gaya.")


if __name__ == "__main__":
    asyncio.run(run())