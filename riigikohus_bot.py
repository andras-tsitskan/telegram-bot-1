#!/usr/bin/env python3
"""
Riigikohus Criminal Chamber Judgment Monitor
=============================================
Designed to run as a GitHub Actions scheduled job (once daily).
On each run it:
  1. Reads seen_judgments.json from the repository (state persisted via git).
  2. Fetches the Criminal Chamber RSS feed from riigikohus.ee.
  3. For each new judgment, scrapes the annotation from rikos.rik.ee.
     - If the annotation is not yet available it is added to a pending retry
       queue and attempted again on the next run(s).
  4. Sends Telegram messages to the configured group.
  5. Writes the updated state back to seen_judgments.json — the GitHub Actions
     workflow then commits this file back to the repository.

Environment variables (set as GitHub Actions secrets):
    TELEGRAM_BOT_TOKEN   — bot token from @BotFather
    TELEGRAM_CHAT_ID     — target group chat ID (negative number)

Optional:
    ANNOTATION_MAX_RETRIES  — how many daily runs to retry a missing annotation (default 3)
    STATE_FILE              — path to state file (default seen_judgments.json)
"""

import os
import re
import sys
import json
import time
import logging
import hashlib
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID",   "")
STATE_FILE = Path(os.getenv("STATE_FILE", "seen_judgments.json"))

# How many consecutive daily runs to keep retrying a missing annotation
# before sending a "still unavailable" notice and giving up.
ANNOTATION_MAX_RETRIES = int(os.getenv("ANNOTATION_MAX_RETRIES", "3"))

RSS_URL      = "https://www.riigikohus.ee/kuriteo-ja-vaarteoasjad/rss.xml"
JUDGMENT_URL = "https://www.riigikohus.ee/et/lahendid?asjaNr={full_number}"
ANNOTATION_URL = "https://rikos.rik.ee/?asjaNr={case_number}&kuvadaVaartus=Annotatsioonid"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; RiigikohusBot/1.0; "
        "+https://github.com/your-org/riigikohus-bot)"
    ),
    "Accept-Language": "et,en;q=0.8",
}

REQUEST_TIMEOUT = 30

# Telegram hard limit is 4096 chars per message.
# HEADER_RESERVE accounts for the header block in the first message.
HEADER_RESERVE = 300
CHUNK_SIZE     = 4096 - HEADER_RESERVE   # first chunk budget (~3796)
CONT_CHUNK     = 4000                    # continuation messages have no header
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State  (seen_judgments.json)
# ---------------------------------------------------------------------------
# Schema:
# {
#   "seen": ["uid1", ...],          — UIDs already notified
#   "pending": {                    — judgments awaiting annotation
#     "<uid>": {
#       "judgment": {...},
#       "retries_done": 0
#     }
#   },
#   "updated": "2024-09-04T07:01:00"
# }

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, KeyError) as exc:
            log.warning("Could not read state file (%s); starting fresh.", exc)
    return {"seen": [], "pending": {}}


def save_state(state: dict) -> None:
    state["updated"] = datetime.utcnow().isoformat()
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    log.info("State saved to %s.", STATE_FILE)


# ---------------------------------------------------------------------------
# RSS parsing
# ---------------------------------------------------------------------------

def fetch_rss() -> list:
    """Fetch the Criminal Chamber RSS feed and return a list of judgment dicts."""
    try:
        resp = requests.get(RSS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Failed to fetch RSS feed: %s", exc)
        return []

    soup  = BeautifulSoup(resp.content, "lxml-xml")
    items = soup.find_all("item") or soup.find_all("entry")

    judgments = []
    for item in items:
        title    = _tag_text(item, ["title"])
        rss_link = _tag_text(item, ["link", "guid"])
        date_raw = _tag_text(item, ["pubDate", "published", "updated", "dc:date"])

        if not rss_link:
            continue

        full_number = _extract_full_number(title + " " + rss_link)
        case_number = _bare_case_number(full_number)
        uid         = _make_uid(rss_link or full_number or title)

        judgments.append({
            "title":       title or "(no title)",
            "rss_url":     rss_link,
            "full_number": full_number,
            "case_number": case_number,
            "date":        _format_date(date_raw),
            "uid":         uid,
        })

    log.info("RSS feed: %d entries found.", len(judgments))
    return judgments


def _tag_text(tag, names: list) -> str:
    for name in names:
        child = tag.find(name)
        if child:
            return child.get_text(strip=True)
    return ""


def _extract_full_number(text: str) -> str:
    """
    Extract judgment number e.g. 1-22-6710/83 or 1-24-1715.
    The /NN suffix is not a word character so \b cannot follow it;
    we use a lookahead for common delimiters and end-of-string instead.
    """
    m = re.search(r"\b(1-\d{2}-\d+/\d+)(?=[\s?&\"'<>]|$)", text)
    if m:
        return m.group(1)
    m = re.search(r"\b(1-\d{2}-\d+)\b", text)
    return m.group(1) if m else ""


def _bare_case_number(full_number: str) -> str:
    """'1-22-6710/83'  →  '1-22-6710'"""
    return full_number.split("/")[0] if full_number else ""


def _format_date(raw: str) -> str:
    """Return DD.MM.YYYY from any RSS date string."""
    if not raw:
        return ""
    # ISO: YYYY-MM-DD
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(3)}.{m.group(2)}.{m.group(1)}"
    # Already DD.MM.YYYY
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return m.group(0)
    # RFC 2822: "Wed, 04 Sep 2024 00:00:00 +0000"
    MONTHS = {
        "Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06",
        "Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12",
    }
    m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})", raw)
    if m:
        month = MONTHS.get(m.group(2)[:3].capitalize(), "??")
        return f"{m.group(1).zfill(2)}.{month}.{m.group(3)}"
    return raw[:20]


def _make_uid(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# Annotation scraping
# ---------------------------------------------------------------------------

_SUPERSCRIPT_MAP = str.maketrans(
    "0123456789+-=()",
    "⁰¹²³⁴⁵⁶⁷⁸⁹⁺⁻⁼⁽⁾",
)


def _replace_sup_tags(node) -> None:
    """
    Replace every <sup> tag in-place with its Unicode superscript equivalent
    so that get_text() keeps it inline rather than adding newline separators.
    e.g. <sup>1</sup>  →  '¹'
    """
    for sup in node.find_all("sup"):
        sup.replace_with(NavigableString(sup.get_text().translate(_SUPERSCRIPT_MAP)))


def fetch_annotation(case_number: str) -> str:
    """
    Scrape the annotation from rikos.rik.ee for the given case number.
    Returns Telegram-safe HTML, or '' if not yet available.
    """
    if not case_number:
        return ""

    url = ANNOTATION_URL.format(case_number=case_number)
    log.info("Fetching annotation for %s", case_number)

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Annotation fetch failed for %s: %s", case_number, exc)
        return ""

    annotation = _parse_annotation(BeautifulSoup(resp.text, "html.parser"))

    if annotation:
        log.info("Annotation fetched: %d characters.", len(annotation))
    else:
        log.info("No annotation content found for %s.", case_number)

    return annotation


def _parse_annotation(soup: BeautifulSoup) -> str:
    """
    Extract annotations from a rikos.rik.ee page using the known HTML structure:

      <div class="lahendi-otsing-annotatsioonid">
        <div class="annotatsioon">
          <strong class="annotatsiooni-marksona">Bold keywords...<br>...</strong>
          <div class="annotatsioon-sisu">Body text...</div>
        </div>
        <hr class="annotatsioon-eraldaja">
        <div class="annotatsioon">...</div>
        ...
      </div>

    Each annotation block is rendered as:
        <b>keyword line 1
        keyword line 2</b>

        body text

    Multiple annotation blocks are separated by a blank line.
    Returns Telegram-safe HTML, or '' if no annotations found.
    """
    container = soup.find("div", class_="lahendi-otsing-annotatsioonid")
    if not container:
        return ""

    annotation_divs = container.find_all("div", class_="annotatsioon")
    if not annotation_divs:
        return ""

    parts = []
    for ann in annotation_divs:

        # --- Bold keywords block ---
        marksona = ann.find("strong", class_="annotatsiooni-marksona")
        if marksona:
            _replace_sup_tags(marksona)
            lines = []
            for child in marksona.children:
                if isinstance(child, NavigableString):
                    text = child.strip()
                    if text:
                        lines.append(_esc(text))
                # <br> acts as a line separator — already handled by joining
            # Fallback if the NavigableString walk yielded nothing
            if not lines:
                lines = [
                    _esc(l)
                    for l in marksona.get_text(separator="\n", strip=True).splitlines()
                    if l.strip()
                ]
            if lines:
                parts.append("<b>" + "\n".join(lines) + "</b>")

        # --- Body text ---
        sisu = ann.find("div", class_="annotatsioon-sisu")
        if sisu:
            _replace_sup_tags(sisu)
            body = sisu.get_text(separator="\n", strip=True)
            body = re.sub(r"\n{3,}", "\n\n", body).strip()
            if body:
                parts.append(_esc(body))

    if not parts:
        return ""

    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_message(text: str) -> bool:
    """Send one Telegram message, respecting rate limits. Returns True on success."""
    api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id":                  CHAT_ID,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(3):
        try:
            resp = requests.post(api_url, json=payload, timeout=15)
            if resp.status_code == 429:
                wait = resp.json().get("parameters", {}).get("retry_after", 10)
                log.warning("Telegram rate limit — waiting %ds.", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            log.error("Telegram send failed (attempt %d): %s", attempt + 1, exc)
            time.sleep(3)
    return False


def send_judgment(judgment: dict, annotation: str, pending: bool = False) -> bool:
    """
    Send one or more Telegram messages for a judgment.

    Message 1:  header + first annotation chunk
    Message 2…N: continuation chunks (if annotation is long)

    Returns True if the header message was delivered successfully.
    Continuation failures are logged but do not affect the return value —
    the judgment is still considered notified (the link is in message 1).
    """
    # --- Header ---
    if judgment.get("full_number"):
        j_url    = JUDGMENT_URL.format(full_number=judgment["full_number"])
        num_line = f'\n📋 <a href="{j_url}"><b>{_esc(judgment["full_number"])}</b></a>'
    else:
        num_line = f'\n📋 <a href="{judgment["rss_url"]}"><b>{_esc(judgment["title"])}</b></a>'

    ann_link = ""
    if judgment.get("case_number"):
        a_url    = ANNOTATION_URL.format(case_number=judgment["case_number"])
        ann_link = f'\n🔎 <a href="{a_url}">Annotatsioon</a>'

    date_str     = f"\n📅 {judgment['date']}" if judgment.get("date") else ""
    pending_note = "\n<i>(annotatsioon lisati hiljem)</i>" if pending else ""

    header = (
        f"⚖️ <b>Uus kriminaalkolleegiumi lahend</b>"
        f"{date_str}"
        f"{num_line}"
        f"{ann_link}"
        f"{pending_note}"
    )

    # --- No annotation yet ---
    if not annotation:
        ok = send_message(header + "\n\n<i>(Annotatsioon pole veel kättesaadav)</i>")
        time.sleep(1)
        return ok

    # --- Split annotation and send ---
    # First chunk has less budget because it shares a message with the header.
    chunks = _split(annotation, first_max=CHUNK_SIZE, rest_max=CONT_CHUNK)
    total  = len(chunks)

    part_suffix = f"\n\n<i>(1/{total})</i>" if total > 1 else ""
    first_msg   = header + f"\n\n📝 <b>Annotatsioonid:</b>\n{chunks[0]}" + part_suffix

    if not send_message(first_msg):
        return False
    time.sleep(1)

    for i, chunk in enumerate(chunks[1:], start=2):
        if not send_message(f"📝 <b>Annotatsioonid ({i}/{total}):</b>\n{chunk}"):
            log.warning("Failed to send annotation part %d/%d — continuing.", i, total)
        time.sleep(1)

    return True


def _split(text: str, first_max: int, rest_max: int) -> list:
    """
    Split text into chunks. The first chunk is limited to first_max chars
    (to leave room for the header), subsequent chunks to rest_max chars.
    Prefers splitting at paragraph then line then word boundaries.
    Never cuts inside a <b>...</b> tag.
    """
    if len(text) <= first_max:
        return [text]

    chunks = []
    max_chars = first_max

    while text:
        if len(text) <= max_chars:
            chunks.append(text)
            break

        cut = text.rfind("\n\n", 0, max_chars)
        if cut == -1:
            cut = text.rfind("\n", 0, max_chars)
        if cut == -1:
            cut = text.rfind(" ", 0, max_chars)
        if cut == -1:
            cut = max_chars

        candidate = text[:cut].strip()

        # Don't split inside an unclosed <b> tag
        if candidate.count("<b>") > candidate.count("</b>"):
            safe = candidate.rfind("<b>")
            if safe > 0:
                candidate = text[:safe].strip()

        chunks.append(candidate)
        text = text[len(candidate):].strip()
        max_chars = rest_max  # subsequent chunks use the larger budget

    return [c for c in chunks if c]


def _esc(text: str) -> str:
    """Escape plain text for use in Telegram HTML messages."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ---------------------------------------------------------------------------
# Annotation retry queue
# ---------------------------------------------------------------------------

def process_pending(state: dict) -> dict:
    """
    Work through judgments that previously had no annotation.
    Called once per daily run before checking for new judgments.

    - Annotation found        → send follow-up message, remove from pending.
    - Still missing, retries remain   → increment counter, keep in pending.
    - Still missing, retries exhausted → send a notice, remove from pending.
    """
    pending = state.get("pending", {})
    if not pending:
        return state

    log.info("Processing %d pending annotation(s)…", len(pending))
    to_remove = []

    for uid, entry in list(pending.items()):
        judgment     = entry["judgment"]
        retries_done = entry["retries_done"] + 1
        case_number  = judgment.get("case_number", "")

        log.info(
            "Annotation retry %d/%d for %s",
            retries_done, ANNOTATION_MAX_RETRIES,
            judgment.get("full_number") or uid,
        )

        time.sleep(2)
        annotation = fetch_annotation(case_number)

        if annotation:
            log.info("Annotation now available for %s — sending follow-up.", case_number)
            send_judgment(judgment, annotation, pending=True)
            to_remove.append(uid)

        elif retries_done >= ANNOTATION_MAX_RETRIES:
            log.warning(
                "Annotation still missing after %d retries for %s — giving up.",
                ANNOTATION_MAX_RETRIES, case_number,
            )
            if judgment.get("full_number"):
                j_url  = JUDGMENT_URL.format(full_number=judgment["full_number"])
                j_link = f'<a href="{j_url}">{_esc(judgment["full_number"])}</a>'
            else:
                j_link = _esc(judgment.get("title", "?"))
            a_url = ANNOTATION_URL.format(case_number=case_number)
            send_message(
                f"ℹ️ Annotatsioon lahendile {j_link} pole {ANNOTATION_MAX_RETRIES} "
                f'päeva jooksul ilmunud. <a href="{a_url}">Kontrolli käsitsi.</a>'
            )
            to_remove.append(uid)

        else:
            entry["retries_done"] = retries_done
            log.info("Will retry annotation for %s on next run.", case_number)

    for uid in to_remove:
        pending.pop(uid, None)

    state["pending"] = pending
    return state


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("=== Riigikohus bot run started at %s UTC ===", datetime.utcnow().isoformat())

    if not BOT_TOKEN or not CHAT_ID:
        log.error(
            "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set. "
            "Add them as GitHub Actions secrets."
        )
        sys.exit(1)

    state = load_state()

    # 1. Retry any pending annotations first
    state = process_pending(state)

    # 2. Fetch RSS and find new judgments
    seen      = set(state.get("seen", []))
    judgments = fetch_rss()
    new       = [j for j in judgments if j["uid"] not in seen]

    if not new:
        log.info("No new judgments found.")
    else:
        log.info("%d new judgment(s) to process.", len(new))

    for judgment in new:
        log.info(
            "Processing: %s [%s]",
            judgment["title"],
            judgment.get("full_number") or judgment.get("case_number") or "?",
        )

        time.sleep(2)
        annotation = fetch_annotation(judgment["case_number"])

        if send_judgment(judgment, annotation):
            seen.add(judgment["uid"])
            if not annotation and judgment.get("case_number"):
                state.setdefault("pending", {})[judgment["uid"]] = {
                    "judgment":     judgment,
                    "retries_done": 0,
                }
                log.info(
                    "Queued %s for annotation retry on next run.",
                    judgment.get("full_number") or judgment["uid"],
                )
        else:
            log.warning(
                "Failed to send notification for %s — will retry next run.",
                judgment["rss_url"],
            )

    state["seen"] = sorted(seen)
    save_state(state)
    log.info("=== Run complete ===")


if __name__ == "__main__":
    main()
