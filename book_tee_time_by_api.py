#!/usr/bin/env python3.12
"""
API-Based Tee Time Booking for Lochmere Golf Club (EZLinks)

Hybrid approach:
- Browser ONLY for passing Cloudflare challenge (one-time)
- Extract cookies, then use direct HTTP API calls via curl_cffi
- Chrome TLS fingerprint impersonation to satisfy Cloudflare

Target: Complete booking in <2 seconds (after Cloudflare).
"""

import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from curl_cffi import requests as http_requests
from dotenv import load_dotenv

from browser_use import Agent
from browser_use.browser.profile import BrowserProfile
from browser_use.browser.session import BrowserSession
from browser_use.llm.anthropic.chat import ChatAnthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")

EZLINKS_USERNAME = os.environ["EZLINKS_USERNAME"]
EZLINKS_PASSWORD = os.environ["EZLINKS_PASSWORD"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]

TARGET_TIME = os.getenv("TARGET_TIME", "08:28")
NUM_PLAYERS = int(os.getenv("NUM_PLAYERS", "4"))
BOOKING_URL = os.getenv("BOOKING_URL", "https://lochmeregm.ezlinksgolf.com")
DAYS_OUT = int(os.getenv("DAYS_OUT", "14"))
BOOKING_HOUR = int(os.getenv("BOOKING_HOUR", "7"))  # Hour when new times drop
BOOKING_MINUTE = int(os.getenv("BOOKING_MINUTE", "0"))  # Minute when new times drop
POLL_LEAD_SECS = int(os.getenv("POLL_LEAD_SECS", "15"))  # Start polling before drop

# Preferred rate type (SponsorID from HAR)
PREFERRED_SPONSOR_ID = int(os.getenv("PREFERRED_SPONSOR_ID", "19191"))  # Member Walk 18H
MASTER_SPONSOR_ID = int(os.getenv("MASTER_SPONSOR_ID", "18718"))
COURSE_ID = int(os.getenv("COURSE_ID", "11301"))
GROUP_ID = int(os.getenv("GROUP_ID", "27848"))

DRY_RUN = "--dry-run" in sys.argv
DEBUG = "--debug" in sys.argv

# Stale-result recovery: re-search after this many consecutive "no longer available" failures
RE_SEARCH_THRESHOLD = int(os.getenv("RE_SEARCH_THRESHOLD", "3"))
# Accept API-suggested alternative times if before this minute-of-day (720 = noon)
MAX_ALT_TIME_MINS = int(os.getenv("MAX_ALT_TIME_MINS", "720"))
# Number of time slots to try in parallel on the first booking wave
PARALLEL_SLOTS = int(os.getenv("PARALLEL_SLOTS", "3"))

# Set up logging — logs/ directory with daily rotation
# Reconfigure stdout to UTF-8 so non-cp1252 chars in scraped page text
# (e.g. ﻿ BOM, emoji) don't crash the StreamHandler on Windows.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
_log_dir = Path(__file__).parent / "logs"
_log_dir.mkdir(exist_ok=True)
_log_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_log_stdout = logging.StreamHandler(sys.stdout)
_log_stdout.setFormatter(_log_fmt)
from logging.handlers import TimedRotatingFileHandler
_log_file = TimedRotatingFileHandler(
    _log_dir / "booking_api.log",
    when="midnight",
    backupCount=30,
    encoding="utf-8",
)
_log_file.suffix = "%Y-%m-%d.log"      # rolled files: booking_api.log.2026-04-05.log
_log_file.setFormatter(_log_fmt)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
log.addHandler(_log_stdout)
log.addHandler(_log_file)
log.propagate = False

# ---------------------------------------------------------------------------
# Known EZLinks API field mappings (from HAR analysis)
# ---------------------------------------------------------------------------
# Search request: p01=courseIDs, p02=date, p03=startTime, p04=endTime,
#                 p05=holes(0=any), p06=players, p07=?
# Search response: r01=csrfToken, r02=?, r03=sessionID, r04=?,
#                  r05=rateTypes, r06=teeTimeSlots
# Tee time slot: r01=uuid, r06=sponsorID, r07=courseID, r08=price,
#                r10=rateIcons, r11=maxPlayers, r12=feeID,
#                r13=reservationTypeID, r14=availablePlayers,
#                r15=dateTime, r16=courseName, r24=displayTime
# Reservation response r02[]: r01=rateName, r03=rateIcons, r06=sponsorID,
#                              r07=feeID, r08=price, r09=rateDescription
# Cart/add: r01=teeTimeUUID, r02=rateInfo, r03=numPlayers, r05=contactID,
#           r07=sessionID, r08=masterSponsorID, r09=csrfToken


def time_to_minutes(time_str: str) -> int:
    """Convert time string like '08:28' or '8:28 AM' to minutes since midnight."""
    time_str = time_str.strip().upper()
    if "AM" in time_str or "PM" in time_str:
        is_pm = "PM" in time_str
        time_str = time_str.replace("AM", "").replace("PM", "").strip()
        parts = time_str.split(":")
        h, m = int(parts[0]), int(parts[1])
        if is_pm and h != 12:
            h += 12
        if not is_pm and h == 12:
            h = 0
        return h * 60 + m
    parts = time_str.split(":")
    return int(parts[0]) * 60 + int(parts[1])


def parse_alternative_time(error_msg):
    """Extract alternative time from cart error like 'We found a <b>Apr 26 2026  9:24AM</b>'.

    Returns the time string (e.g. '9:24AM') or None if not found.
    """
    match = re.search(r'We found a\s*<b>[^<]*?\s+(\d{1,2}:\d{2}\s*[AP]M)</b>', error_msg, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def minutes_to_time(mins: int) -> str:
    """Convert minutes since midnight to readable time."""
    h = mins // 60
    m = mins % 60
    period = "AM" if h < 12 else "PM"
    if h == 0:
        h = 12
    elif h > 12:
        h -= 12
    return f"{h}:{m:02d} {period}"


# ---------------------------------------------------------------------------
# Phase 1: Browser — pass Cloudflare challenge
# ---------------------------------------------------------------------------

async def pass_cloudflare():
    """Use browser to pass Cloudflare, return browser_session."""
    log.info("Phase 1: Passing Cloudflare with browser...")

    llm = ChatAnthropic(model="claude-sonnet-4-6", api_key=ANTHROPIC_API_KEY)
    browser_profile = BrowserProfile(
        headless=False,
        disable_security=False,
        window_size={"width": 1024, "height": 768},
        allowed_domains=[
            "lochmeregm.ezlinksgolf.com",
            "ezlinksgolf.com",
            "challenges.cloudflare.com",
        ],
    )
    browser_session = BrowserSession(browser_profile=browser_profile, keep_alive=True)

    await browser_session.start()
    page = await browser_session.get_current_page()
    await page.goto(BOOKING_URL)
    await asyncio.sleep(10)

    cloudflare_task = """
    You are on a page with a Cloudflare "Verify you are human" checkbox.
    On your VERY FIRST action, do BOTH of these together:
      1. Click the LABEL element (checkbox-state attribute, text "Verify you are human").
         Do NOT click the div with role=alert — only the label works.
      2. Call done("CLOUDFLARE_PASSED").
    If the booking page is already loaded (date picker, tee times, Sign In), call done immediately.
    Do NOT wait. Do NOT use JavaScript. Do NOT take extra steps.
    """

    MAX_CF_RETRIES = 3
    CF_POLL_TIMEOUT = 20
    CF_POLL_INTERVAL = 2

    for cf_attempt in range(1, MAX_CF_RETRIES + 1):
        log.info(f"Cloudflare agent attempt {cf_attempt}/{MAX_CF_RETRIES}...")

        agent = Agent(
            task=cloudflare_task, llm=llm, use_vision=True,
            browser_session=browser_session, max_failures=2,
            max_actions_per_step=2, flash_mode=True, use_judge=False,
        )
        await agent.run()

        page_check = await browser_session.get_current_page()
        if not page_check:
            log.error("Could not get page for Cloudflare verification")
            sys.exit(1)

        poll_elapsed = 0.0
        while poll_elapsed < CF_POLL_TIMEOUT:
            cf_check = await page_check.evaluate("""() => {
                var body = document.body ? document.body.innerText : '';
                var url = window.location.href;
                var hasChallenge = body.includes('Verify you are human') ||
                                  body.includes('Just a moment') ||
                                  body.includes('security verification') ||
                                  body.includes('security service') ||
                                  body.includes('malicious bots') ||
                                  body.includes('not a bot') ||
                                  url.includes('challenges.cloudflare.com') ||
                                  !!document.querySelector('iframe[src*="challenges.cloudflare.com"]');
                var isTransitioning = (body.includes('Verifying') && !body.includes('security verification')) ||
                                     body.trim().length < 50;
                var hasBookingContent = body.includes('Sign In') ||
                                       body.includes('Tee Times') ||
                                       body.includes('Book') ||
                                       body.includes('Player');
                return JSON.stringify({
                    hasChallenge: hasChallenge,
                    isTransitioning: isTransitioning,
                    hasBookingContent: hasBookingContent,
                    url: url,
                    bodyPreview: body.substring(0, 300)
                });
            }""")
            log.info(f"  Verify poll +{poll_elapsed:.0f}s: {cf_check}")
            cf_status = json.loads(cf_check) if isinstance(cf_check, str) else cf_check

            if not cf_status.get("hasChallenge") and cf_status.get("hasBookingContent"):
                log.info("Cloudflare passed — booking page confirmed.")
                return browser_session

            if cf_status.get("hasChallenge") and not cf_status.get("isTransitioning"):
                log.info("Challenge still active — will retry agent.")
                break

            log.info("  Page transitioning...")
            await asyncio.sleep(CF_POLL_INTERVAL)
            poll_elapsed += CF_POLL_INTERVAL

    log.error(f"Cloudflare not passed after {MAX_CF_RETRIES} attempts. Aborting.")
    sys.exit(1)


async def extract_cookies(browser_session):
    """Extract all cookies from the browser session."""
    log.info("Extracting cookies from browser...")
    cookies = await browser_session._cdp_get_cookies()
    log.info(f"Got {len(cookies)} cookies")
    for c in cookies:
        name = c.get("name", "?")
        domain = c.get("domain", "?")
        log.info(f"  {name} ({domain})")
    return cookies


def build_http_session(cookies):
    """Build a curl_cffi Session with Chrome TLS fingerprint + browser cookies."""
    session = http_requests.Session(impersonate="chrome")
    session.headers.update({
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json; charset=UTF-8",
        "origin": BOOKING_URL,
        "referer": f"{BOOKING_URL}/index.html",
    })
    for c in cookies:
        session.cookies.set(
            c["name"], c["value"],
            domain=c.get("domain", ""),
            path=c.get("path", "/"),
        )
    return session


# ---------------------------------------------------------------------------
# Phase 2: Direct API booking flow
# ---------------------------------------------------------------------------

def api_get(session, endpoint, desc=""):
    """GET an API endpoint, return parsed JSON or exit on failure."""
    t0 = time.time()
    r = session.get(f"{BOOKING_URL}{endpoint}")
    ms = (time.time() - t0) * 1000
    log.info(f"  GET {endpoint} -> {r.status_code} ({ms:.0f}ms) {desc}")
    if r.status_code != 200:
        log.error(f"  Failed: {r.text[:300]}")
        return None
    return r.json()


def api_post(session, endpoint, body, desc=""):
    """POST to an API endpoint, return parsed JSON or None on failure."""
    t0 = time.time()
    r = session.post(f"{BOOKING_URL}{endpoint}", json=body)
    ms = (time.time() - t0) * 1000
    log.info(f"  POST {endpoint} -> {r.status_code} ({ms:.0f}ms) {desc}")
    if r.status_code != 200:
        log.error(f"  Failed: {r.text[:300]}")
        return None
    return r.json()


def group_tee_times(tee_times):
    """Group tee time entries by datetime (r15).

    The search API returns one entry per rate type per time slot.
    E.g., 12:12 PM has 4 entries: Member Ride 18H, Member Ride 9H,
    Member Walk 18H, Member Walk 9H — all with the same r15 datetime.

    Returns a list of dicts: {datetime, display_time, entries: [...], available_players}
    """
    from collections import OrderedDict
    groups = OrderedDict()
    for tt in tee_times:
        dt = tt.get("r15", "")
        if dt not in groups:
            groups[dt] = {
                "datetime": dt,
                "display_time": tt.get("r24", ""),
                "entries": [],
                "available_players": tt.get("r14", 0),
            }
        groups[dt]["entries"].append(tt)
    return list(groups.values())


def find_best_time_slot(time_slots, target_time, num_players, tried_datetimes=None):
    """Find the time slot closest to target with enough player slots."""
    if tried_datetimes is None:
        tried_datetimes = set()
    target_mins = time_to_minutes(target_time)

    eligible = []
    for slot in time_slots:
        if slot["datetime"] in tried_datetimes:
            continue
        if slot["available_players"] < num_players:
            continue
        display_time = slot["display_time"]
        if not display_time:
            continue
        tt_mins = time_to_minutes(display_time)
        diff = abs(tt_mins - target_mins)
        if tt_mins <= target_mins:
            diff -= 0.5  # Small bonus for being before target
        eligible.append((diff, slot))

    if not eligible:
        return None

    eligible.sort(key=lambda x: x[0])
    return eligible[0][1]


def get_preferred_entry(slot, sponsor_id):
    """Get the tee time entry for the preferred rate type from a time slot."""
    for entry in slot["entries"]:
        if entry.get("r06") == sponsor_id:
            return entry
    return slot["entries"][0] if slot["entries"] else None


def login(session):
    """Authenticate and return (session_id, csrf_token, contact_id)."""
    log.info("Step 1: Getting session...")
    login_init = api_get(session, "/api/login/login", "get session")
    if not login_init:
        log.error("Failed to get initial session")
        sys.exit(1)
    session_id = login_init["SessionID"]
    csrf_token = login_init["CsrfToken"]
    log.info(f"  SessionID: {session_id}")

    log.info("Step 2: Authenticating...")
    login_resp = api_post(session, "/api/login/login", {
        "Login": EZLINKS_USERNAME,
        "Password": EZLINKS_PASSWORD,
        "SessionID": "",
        "MasterSponsorID": str(MASTER_SPONSOR_ID),
    }, "authenticate")
    if not login_resp or not login_resp.get("IsSuccessful"):
        log.error(f"Login failed: {login_resp}")
        sys.exit(1)

    contact_id = login_resp["ContactID"]
    session_id = login_resp["SessionID"]
    csrf_token = login_resp["CsrfToken"]
    log.info(f"  Logged in as {login_resp['ContactFirstName']} {login_resp['ContactLastName']}")
    log.info(f"  ContactID: {contact_id}")

    return session_id, csrf_token, contact_id


def search_tee_times(session, target_date_str, num_players):
    """Search for tee times on a given date. Returns (all_times, rate_types)."""
    log.info(f"Step 3: Searching tee times for {target_date_str}, {num_players} players...")
    search_resp = api_post(session, "/api/search/search", {
        "p01": [COURSE_ID],
        "p02": target_date_str,
        "p03": "5:00 AM",
        "p04": "7:00 PM",
        "p05": 0,       # Any holes
        "p06": num_players,
        "p07": False,
    }, "search tee times")

    if not search_resp:
        return [], []

    tee_times = search_resp.get("r06", [])
    rate_types = search_resp.get("r05", [])
    return tee_times, rate_types


def get_reservation_details(session, time_slot, session_id):
    """Get reservation/rate details for a time slot. Returns rate info list.

    The reservation request needs:
    - p01: UUID of any entry in the time slot (first one works)
    - p02: array of rate entries built from ALL entries at this time slot
    - p03: sessionID
    """
    # Build p02 from all entries at this time slot
    rate_entries = []
    for entry in time_slot["entries"]:
        rate_entries.append({
            "r01": entry["r06"],       # sponsorID
            "r02": entry["r10"],       # rateIcons
            "r03": entry["r13"],       # reservationTypeID
            "r04": entry["r12"],       # feeID
            "r05": 0,
            "r06": -1,
            "r07": str(entry["r10"]),  # rateIcons as string
        })

    # Use the first entry's UUID as p01
    first_uuid = time_slot["entries"][0]["r01"]

    log.info("Step 4: Getting reservation details...")
    res_resp = api_post(session, "/api/search/reservation", {
        "p01": first_uuid,
        "p02": rate_entries,
        "p03": session_id,
    }, "reservation details")

    if not res_resp:
        return None
    return res_resp.get("r02", [])


def add_to_cart(session, preferred_entry, rate_info, num_players, contact_id, session_id, csrf_token):
    """Add selected tee time to cart.

    Returns (cart_response, error_message).
    On success: (response_dict, None)
    On failure: (None, status_message_string_or_None)
    """
    # Find the matching rate info for our preferred sponsor
    selected_rate = None
    for ri in rate_info:
        if ri.get("r06") == PREFERRED_SPONSOR_ID:
            selected_rate = ri
            break
    if not selected_rate:
        log.warning(f"Preferred sponsor {PREFERRED_SPONSOR_ID} not found, using first rate")
        selected_rate = rate_info[0] if rate_info else None
    if not selected_rate:
        log.error("No rate info available")
        return None, None

    log.info(f"Step 5: Adding to cart ({selected_rate.get('r01', '?')})...")
    cart_resp = api_post(session, "/api/cart/add", {
        "r01": preferred_entry["r01"],  # tee time UUID for preferred rate
        "r02": selected_rate,            # full rate info object from reservation
        "r03": num_players,
        "r04": False,
        "r05": contact_id,
        "r06": False,
        "r07": session_id,
        "r08": MASTER_SPONSOR_ID,
        "r09": csrf_token,
    }, "add to cart")

    if not cart_resp:
        return None, None

    log.info(f"  cart/add response body: {json.dumps(cart_resp)}")

    if not cart_resp.get("IsSuccessful"):
        error_msg = cart_resp.get('StatusMessage', 'unknown error')
        log.error(f"Cart add failed: {error_msg}")
        return None, error_msg

    return cart_resp, None


def try_book_slot(session, slot, session_id, csrf_token, contact_id, num_players, success_event):
    """Try to book a single slot: reservation details → cart/add.

    Checks success_event before calling cart/add — if another thread already
    won, this thread skips the cart/add to avoid holding extra tee times.

    Returns (slot, cart_resp, error_msg).
    """
    display_time = slot["display_time"]
    preferred_entry = get_preferred_entry(slot, PREFERRED_SPONSOR_ID)
    if not preferred_entry:
        return slot, None, "no preferred entry"

    log.info(f"  [stagger] {display_time}: getting reservation details...")

    rate_info = get_reservation_details(session, slot, session_id)
    if not rate_info:
        return slot, None, "reservation details failed"

    # Check if another thread already succeeded — skip cart/add if so
    if success_event.is_set():
        log.info(f"  [stagger] {display_time}: another slot already won, skipping cart/add")
        return slot, None, "skipped — another slot won"

    log.info(f"  [stagger] {display_time}: adding to cart...")
    cart_resp, error_msg = add_to_cart(
        session, preferred_entry, rate_info, num_players,
        contact_id, session_id, csrf_token,
    )
    if cart_resp:
        success_event.set()  # signal other threads to skip
    return slot, cart_resp, error_msg


# Delay between staggered slot launches (seconds)
STAGGER_DELAY = float(os.getenv("STAGGER_DELAY", "0.5"))


def staggered_book_slots(session, slots, session_id, csrf_token, contact_id, num_players):
    """Fire reservation + cart/add for slots with staggered starts.

    Launches each slot STAGGER_DELAY apart. A shared Event prevents later
    slots from calling cart/add once an earlier slot succeeds, so at most
    one tee time is held under normal conditions.

    Returns (winning_slot, cart_resp, failures).
    """
    success_event = threading.Event()
    failures = []

    with ThreadPoolExecutor(max_workers=len(slots)) as pool:
        futures = {}
        for i, slot in enumerate(slots):
            if success_event.is_set():
                log.info(f"  [stagger] Skipping {slot['display_time']} — already have a winner")
                break
            futures[pool.submit(
                try_book_slot, session, slot,
                session_id, csrf_token, contact_id, num_players,
                success_event,
            )] = slot
            # Stagger: wait before launching next slot (except after the last one)
            if i < len(slots) - 1:
                time.sleep(STAGGER_DELAY)

        for future in as_completed(futures):
            slot, cart_resp, error_msg = future.result()
            if cart_resp:
                log.info(f"  [stagger] Winner: {slot['display_time']}")
                return slot, cart_resp, failures
            else:
                failures.append((slot, error_msg))
                log.info(f"  [stagger] Failed: {slot['display_time']} — {error_msg or 'unknown'}")

    return None, None, failures


def hold_reservation(session, contact_id, session_id):
    """Hold the reservation in the cart."""
    log.info("Step 6: Holding reservation...")
    hold_resp = api_post(session, "/api/cart/holdreservation", {
        "PriceWindowIDs": None,
        "SponsorID": str(PREFERRED_SPONSOR_ID),
        "ContactID": contact_id,
        "SessionID": session_id,
        "MasterSponsorID": str(MASTER_SPONSOR_ID),
    }, "hold reservation")
    return hold_resp


def check_conflicts(session, preferred_entry, contact_id):
    """Check for tee time conflicts."""
    log.info("Step 7: Checking conflicts...")
    conflict_resp = api_post(session, "/api/cart/checkteetimeconflicts", {
        "CourseID": str(COURSE_ID),
        "ContactID": str(contact_id),
        "SponsorID": str(PREFERRED_SPONSOR_ID),
        "TeeTime": preferred_entry["r15"],  # datetime string
    }, "check conflicts")
    return conflict_resp


def finish_booking(session, contact_id, session_id):
    """Finalize the booking. Returns confirmation response."""
    log.info("Step 8: Finishing booking...")
    finish_resp = api_post(session, "/api/cart/finish", {
        "ContinueOnPartnerTeeTimeConflict": True,
        "Email1": None,
        "Email2": None,
        "Email3": None,
        "SponsorID": str(PREFERRED_SPONSOR_ID),
        "CourseID": str(COURSE_ID),
        "ReservationTypeID": "52365",
        "SessionID": session_id,
        "ContactID": str(contact_id),
        "MasterSponsorID": str(MASTER_SPONSOR_ID),
        "GroupID": str(GROUP_ID),
        "SensibleWeatherQuoteId": None,
        "DeclineSensibleWeatherQuotation": False,
    }, "finish booking")
    return finish_resp


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

async def main():
    target_date = datetime.now() + timedelta(days=DAYS_OUT)
    target_date_str = target_date.strftime("%m/%d/%Y")
    target_day_str = target_date.strftime("%A, %B %d, %Y")

    log.info("=" * 60)
    log.info("Tee Time Booking Bot (API MODE)")
    log.info("=" * 60)
    log.info(f"Target date:    {target_day_str} ({target_date_str})")
    log.info(f"Target time:    {TARGET_TIME}")
    log.info(f"Players:        {NUM_PLAYERS}")
    log.info(f"Booking URL:    {BOOKING_URL}")
    log.info(f"Dry run:        {DRY_RUN}")
    log.info("=" * 60)

    overall_start = time.time()

    # ---------------------------------------------------------------
    # Phase 1: Pass Cloudflare (browser)
    # ---------------------------------------------------------------
    browser_session = await pass_cloudflare()
    cookies = await extract_cookies(browser_session)

    # Close browser — we don't need it anymore
    # Suppress noisy browser-use cleanup warnings
    logging.getLogger("BrowserSession").setLevel(logging.ERROR)
    logging.getLogger("browser_use.browser.session").setLevel(logging.ERROR)
    try:
        await browser_session.stop()
    except Exception:
        pass

    cf_elapsed = time.time() - overall_start
    log.info(f"Cloudflare phase complete in {cf_elapsed:.1f}s")

    # Build HTTP session with browser cookies + Chrome TLS fingerprint
    session = build_http_session(cookies)

    # ---------------------------------------------------------------
    # Phase 2: API-based login
    # ---------------------------------------------------------------
    log.info("Phase 2: API login + search...")
    api_start = time.time()

    session_id, csrf_token, contact_id = login(session)

    # ---------------------------------------------------------------
    # Wait for booking window (only on 14-day-out drop runs)
    # ---------------------------------------------------------------
    now = datetime.now()
    drop_time = now.replace(hour=BOOKING_HOUR, minute=BOOKING_MINUTE, second=0, microsecond=0)
    poll_start = drop_time - timedelta(seconds=POLL_LEAD_SECS)
    is_drop_run = DAYS_OUT == 14 and now < drop_time

    if is_drop_run and now < poll_start:
        wait_secs = (poll_start - now).total_seconds()
        log.info(f"Pre-positioned! Waiting {wait_secs:.0f}s until {poll_start.strftime('%H:%M:%S')} to start polling...")
        log.info(f"Drop time: {drop_time.strftime('%H:%M:%S')}, polling starts {POLL_LEAD_SECS}s early")
        while True:
            now = datetime.now()
            remaining = (poll_start - now).total_seconds()
            if remaining <= 0:
                break
            if remaining <= 10:
                log.info(f"  {remaining:.1f}s until polling starts...")
            elif remaining <= 60:
                if int(remaining) % 10 == 0:
                    log.info(f"  {remaining:.0f}s until polling starts...")
            else:
                if int(remaining) % 30 == 0:
                    log.info(f"  {remaining:.0f}s until polling starts...")
            await asyncio.sleep(min(1.0, remaining))

        log.info("Starting rapid API polling for new tee times!")

    # ---------------------------------------------------------------
    # Phase 3: Search + Book via API
    # ---------------------------------------------------------------
    if is_drop_run:
        # Poll for morning times to appear
        POLL_INTERVAL = 5        # seconds between searches
        POLL_TIMEOUT_MIN = 15    # give up after this many minutes past drop
        poll_deadline = drop_time + timedelta(minutes=POLL_TIMEOUT_MIN)
        poll_attempt = 0
        tee_times = []
        found_morning = False

        while datetime.now() < poll_deadline:
            poll_attempt += 1
            now = datetime.now()
            log.info(f"Poll #{poll_attempt} at {now.strftime('%H:%M:%S')} "
                     f"(drop {'in ' + str(int((drop_time - now).total_seconds())) + 's' if now < drop_time else '+' + str(int((now - drop_time).total_seconds())) + 's'})")

            tee_times, rate_types = search_tee_times(session, target_date_str, NUM_PLAYERS)

            # Check if morning times exist
            morning_times = [
                tt for tt in tee_times
                if tt.get("r24") and time_to_minutes(tt["r24"]) < 720  # before noon
            ]
            if morning_times:
                log.info(f"Morning times detected! {len(morning_times)} entries before noon")
                found_morning = True
                break

            await asyncio.sleep(POLL_INTERVAL)

        if not found_morning:
            log.warning(f"No morning times appeared after {poll_attempt} polls ({POLL_TIMEOUT_MIN} min timeout) — using best available")
    else:
        if DAYS_OUT != 14:
            log.info(f"Not a drop run (DAYS_OUT={DAYS_OUT}), searching immediately...")
        else:
            log.info("Booking window already open, searching immediately...")
        tee_times, rate_types = search_tee_times(session, target_date_str, NUM_PLAYERS)

    if not tee_times:
        log.error("No tee times found!")
        sys.exit(1)

    # Group tee times by datetime (each time slot has multiple rate entries)
    time_slots = group_tee_times(tee_times)
    log.info(f"Found {len(tee_times)} total entries across {len(time_slots)} time slots")

    # Log available time slots
    for slot in time_slots[:10]:
        preferred = get_preferred_entry(slot, PREFERRED_SPONSOR_ID)
        price = preferred["r08"] if preferred else "?"
        log.info(f"  {slot['display_time']} - {slot['available_players']} players - ${price}")
    if len(time_slots) > 10:
        log.info(f"  ... and {len(time_slots) - 10} more")

    search_elapsed = time.time() - api_start
    log.info(f"Login + search completed in {search_elapsed*1000:.0f}ms")

    # ---------------------------------------------------------------
    # Booking: parallel race then sequential fallback with re-search
    # ---------------------------------------------------------------
    tried_datetimes = set()
    MAX_ATTEMPTS = 15
    booking_start = time.time()
    booked = False

    # --- Wave 1: Race top PARALLEL_SLOTS slots simultaneously ---
    race_candidates = []
    for _ in range(PARALLEL_SLOTS):
        slot = find_best_time_slot(time_slots, TARGET_TIME, NUM_PLAYERS, tried_datetimes)
        if not slot:
            break
        race_candidates.append(slot)
        tried_datetimes.add(slot["datetime"])

    best_slot = None
    cart_resp = None
    attempts_used = 0

    if race_candidates:
        log.info("=" * 60)
        log.info(f"STAGGERED RACE: {len(race_candidates)} slots, {STAGGER_DELAY:.1f}s apart")
        for rc in race_candidates:
            pe = get_preferred_entry(rc, PREFERRED_SPONSOR_ID)
            log.info(f"  {rc['display_time']} — {rc['available_players']}p — ${pe['r08'] if pe else '?'}")
        log.info("=" * 60)

        winning_slot, cart_resp, failures = staggered_book_slots(
            session, race_candidates, session_id, csrf_token, contact_id, NUM_PLAYERS,
        )
        attempts_used = len(race_candidates)

        if cart_resp:
            best_slot = winning_slot
        else:
            log.warning(f"Parallel race: all {len(race_candidates)} slots failed")
            # Check failures for alternative time hints
            for failed_slot, error_msg in failures:
                if error_msg:
                    alt_time_str = parse_alternative_time(error_msg)
                    if alt_time_str:
                        alt_mins = time_to_minutes(alt_time_str)
                        if alt_mins < MAX_ALT_TIME_MINS:
                            log.info(f"API suggests {alt_time_str} — re-searching for fresh results")
                            tee_times, rate_types = search_tee_times(session, target_date_str, NUM_PLAYERS)
                            if tee_times:
                                time_slots = group_tee_times(tee_times)
                                tried_datetimes.clear()
                                log.info(f"Re-search found {len(time_slots)} time slots")
                            break
                        else:
                            log.info(f"API suggests {alt_time_str} — too late (after {minutes_to_time(MAX_ALT_TIME_MINS)})")

    # --- Wave 2: Sequential fallback with re-search recovery ---
    if not cart_resp:
        consecutive_stale = 0
        for attempt in range(attempts_used, MAX_ATTEMPTS):
            if consecutive_stale >= RE_SEARCH_THRESHOLD:
                log.info(f"Re-searching after {consecutive_stale} consecutive stale failures...")
                tee_times, rate_types = search_tee_times(session, target_date_str, NUM_PLAYERS)
                if tee_times:
                    time_slots = group_tee_times(tee_times)
                    log.info(f"Re-search found {len(time_slots)} time slots")
                    tried_datetimes.clear()
                else:
                    log.warning("Re-search returned no results, continuing with existing data")
                consecutive_stale = 0

            slot = find_best_time_slot(time_slots, TARGET_TIME, NUM_PLAYERS, tried_datetimes)
            if not slot:
                log.error("No more eligible time slots to try!")
                break

            display_time = slot["display_time"]
            preferred_entry = get_preferred_entry(slot, PREFERRED_SPONSOR_ID)
            log.info("=" * 60)
            log.info(f"BOOKING ATTEMPT {attempt + 1}/{MAX_ATTEMPTS}: {display_time}")
            log.info(f"  DateTime: {slot['datetime']}")
            log.info(f"  Available: {slot['available_players']} players")
            log.info(f"  Preferred entry UUID: {preferred_entry['r01'] if preferred_entry else '?'}")
            log.info(f"  Price: ${preferred_entry['r08'] if preferred_entry else '?'}")
            log.info("=" * 60)

            rate_info = get_reservation_details(session, slot, session_id)
            if not rate_info:
                log.warning("Failed to get reservation details — trying next time")
                tried_datetimes.add(slot["datetime"])
                continue

            resp, error_msg = add_to_cart(
                session, preferred_entry, rate_info, NUM_PLAYERS,
                contact_id, session_id, csrf_token,
            )
            if not resp:
                tried_datetimes.add(slot["datetime"])

                if error_msg:
                    alt_time_str = parse_alternative_time(error_msg)
                    if alt_time_str:
                        alt_mins = time_to_minutes(alt_time_str)
                        if alt_mins < MAX_ALT_TIME_MINS:
                            log.info(f"API suggests {alt_time_str} — re-searching for fresh results")
                            tee_times, rate_types = search_tee_times(session, target_date_str, NUM_PLAYERS)
                            if tee_times:
                                time_slots = group_tee_times(tee_times)
                                tried_datetimes.clear()
                                consecutive_stale = 0
                                log.info(f"Re-search found {len(time_slots)} time slots")
                            continue
                        else:
                            log.info(f"API suggests {alt_time_str} — too late (after {minutes_to_time(MAX_ALT_TIME_MINS)})")

                consecutive_stale += 1
                log.warning(f"Failed to add to cart — trying next (stale streak: {consecutive_stale})")
                continue

            best_slot = slot
            cart_resp = resp
            consecutive_stale = 0
            break
        else:
            log.error(f"All {MAX_ATTEMPTS} booking attempts failed!")
            sys.exit(1)

    if not cart_resp or not best_slot:
        log.error("No booking succeeded!")
        sys.exit(1)

    # --- Finalize the winning slot ---
    display_time = best_slot["display_time"]
    preferred_entry = get_preferred_entry(best_slot, PREFERRED_SPONSOR_ID)

    # Hold reservation
    hold_resp = hold_reservation(session, contact_id, session_id)
    if not hold_resp:
        log.error("Failed to hold reservation after successful cart add!")
        sys.exit(1)

    # Check conflicts
    conflict_resp = check_conflicts(session, preferred_entry, contact_id)
    if conflict_resp and conflict_resp.get("CaptainTeeTimeConflictsFound"):
        log.warning("Tee time conflict detected — you may already have a booking")

    if DRY_RUN:
        booking_elapsed = time.time() - booking_start
        total_elapsed = time.time() - overall_start
        log.info("=" * 60)
        log.info(f"DRY RUN — stopping before finish")
        log.info(f"Would book: {display_time} on {target_day_str}")
        log.info(f"Players: {NUM_PLAYERS}")
        log.info("-" * 60)
        log.info(f"  Cloudflare:       {cf_elapsed:.1f}s")
        log.info(f"  Login + Search:   {search_elapsed:.1f}s")
        log.info(f"  Booking:          {booking_elapsed:.1f}s")
        log.info(f"  TOTAL:            {total_elapsed:.1f}s")
        log.info("=" * 60)
    else:
        finish_resp = finish_booking(session, contact_id, session_id)
        if not finish_resp or not finish_resp.get("IsSuccessful"):
            log.error(f"Finish call failed: {finish_resp.get('StatusText', 'unknown') if finish_resp else 'no response'}")
            sys.exit(1)

        booking_elapsed = time.time() - booking_start
        total_elapsed = time.time() - overall_start
        log.info("=" * 60)
        log.info("BOOKING SUCCESSFUL!")
        log.info(f"  Time: {display_time} on {target_day_str}")
        log.info(f"  Confirmation: {finish_resp.get('ConfirmationNumber', '?')}")
        log.info(f"  Location: {finish_resp.get('Location', '?')}")
        log.info(f"  Players: {finish_resp.get('NumberOfPlayers', '?')}")
        log.info(f"  Total Price: ${finish_resp.get('TotalPrice', '?')}")
        log.info(f"  Cancel By: {finish_resp.get('CancellationDeadline', '?')}")
        log.info("-" * 60)
        log.info(f"  Cloudflare:       {cf_elapsed:.1f}s")
        log.info(f"  Login + Search:   {search_elapsed:.1f}s")
        log.info(f"  Booking:          {booking_elapsed:.1f}s")
        log.info(f"  TOTAL:            {total_elapsed:.1f}s")
        log.info("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed transport")
    warnings.filterwarnings("ignore", category=RuntimeWarning, message="coroutine.*was never awaited")
    # Suppress Windows asyncio pipe cleanup noise
    if sys.platform == "win32":
        _original_del = getattr(asyncio.proactor_events._ProactorBasePipeTransport, "__del__", None)
        if _original_del:
            def _silent_del(self):
                try:
                    _original_del(self)
                except (ValueError, OSError):
                    pass
            asyncio.proactor_events._ProactorBasePipeTransport.__del__ = _silent_del
    # Suppress browser-use cleanup chatter
    logging.getLogger("BrowserSession").setLevel(logging.CRITICAL)
    logging.getLogger("browser_use.browser.session").setLevel(logging.CRITICAL)
    try:
        asyncio.run(main())
    except SystemExit:
        pass
