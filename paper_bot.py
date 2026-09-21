"""
Paper trading bot for a memecoin alert channel - cloud deployment version.

Reads launch alerts from your Telegram channel (read-only), opens virtual
positions in two parallel portfolios ("fixed" target vs "scaled + trailing"),
tracks real prices via DexScreener's free public API, and simulates fees.
No real money moves.

Designed to run on Render's free web service tier:
  - Persists to Postgres (via DATABASE_URL) so data survives Render's
    "restart at any time" behavior on the free tier. Falls back to a local
    SQLite file if DATABASE_URL isn't set, for running on your own computer.
  - Runs a tiny Flask server on $PORT so Render sees it as a live web
    service and UptimeRobot has an HTTP endpoint to ping (which prevents
    the free-tier 15-minute idle spin-down).
  - Logs into Telegram using a pre-generated STRING session (see
    generate_session.py) since a cloud server can't answer an interactive
    "enter the code we texted you" prompt the way your own terminal can.

Secrets (never hardcoded, never committed to GitHub):
  TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_SESSION, TELEGRAM_CHANNEL,
  DATABASE_URL (set automatically by Render when you attach a Postgres db)
"""

import asyncio
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import requests
from flask import Flask, request, redirect
from telethon import TelegramClient, events
from telethon.sessions import StringSession

HERE = Path(__file__).parent
CONFIG_PATH = HERE / "config.json"
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{}"

# ---------------------------- secrets / env ---------------------------------

TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH")
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "")
TELEGRAM_CHANNEL = os.environ.get("TELEGRAM_CHANNEL")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 8080))
ADMIN_KEY = os.environ.get("ADMIN_KEY", "")

USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
else:
    import sqlite3
    SQLITE_PATH = HERE / "paperbot.db"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ------------------------ database abstraction ------------------------------
# Every call opens its own short-lived connection. This is deliberate: the
# Telegram listener runs on an asyncio loop and Flask runs on its own thread,
# so sharing one long-lived connection across both would be unsafe. At this
# app's traffic volume (roughly one price check per open position every 15s)
# the overhead of opening a fresh connection each time is irrelevant.

def _raw_conn():
    if USE_PG:
        return psycopg2.connect(DATABASE_URL)
    return sqlite3.connect(SQLITE_PATH)


def ph(sql):
    """Translate '?' placeholders (sqlite style) to '%s' (psycopg2 style)."""
    return sql.replace("?", "%s") if USE_PG else sql


@contextmanager
def db():
    con = _raw_conn()
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db():
    with db() as con:
        cur = con.cursor()
        if USE_PG:
            cur.execute("""CREATE TABLE IF NOT EXISTS portfolios (
                name TEXT PRIMARY KEY, balance REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS open_positions (
                id SERIAL PRIMARY KEY, portfolio TEXT, contract TEXT, name TEXT,
                entry_time TEXT, entry_mcap REAL, bet_size REAL,
                remaining_pct REAL, realized_pnl REAL, peak_ratio REAL,
                tier1_done INTEGER, tier2_done INTEGER)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY, portfolio TEXT, contract TEXT, name TEXT,
                entry_time TEXT, exit_time TEXT, entry_mcap REAL, exit_mcap REAL,
                exit_reason TEXT, pnl REAL, balance_after REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS balance_history (
                portfolio TEXT, ts TEXT, balance REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS skipped (
                portfolio TEXT, ts TEXT, contract TEXT, name TEXT, reason TEXT)""")
        else:
            cur.execute("""CREATE TABLE IF NOT EXISTS portfolios (
                name TEXT PRIMARY KEY, balance REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS open_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, portfolio TEXT, contract TEXT,
                name TEXT, entry_time TEXT, entry_mcap REAL, bet_size REAL,
                remaining_pct REAL, realized_pnl REAL, peak_ratio REAL,
                tier1_done INTEGER, tier2_done INTEGER)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, portfolio TEXT, contract TEXT,
                name TEXT, entry_time TEXT, exit_time TEXT, entry_mcap REAL,
                exit_mcap REAL, exit_reason TEXT, pnl REAL, balance_after REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS balance_history (
                portfolio TEXT, ts TEXT, balance REAL)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS skipped (
                portfolio TEXT, ts TEXT, contract TEXT, name TEXT, reason TEXT)""")

        # Migration: older deployments' trades table predates peak_ratio.
        # peak_ratio is the highest ratio (current_mcap / entry_mcap) the
        # token ever reached while the position was open, regardless of
        # what target/stop actually closed it - this is what lets us later
        # ask "would this have hit 1.5x?" for a target the bot wasn't even
        # using at the time.
        if USE_PG:
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS peak_ratio REAL")
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS bet_size REAL")
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_type TEXT")
            cur.execute("ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS entry_type TEXT")
            cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS alert_mcap REAL")
            cur.execute("ALTER TABLE open_positions ADD COLUMN IF NOT EXISTS alert_mcap REAL")
        else:
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN peak_ratio REAL")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN bet_size REAL")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN entry_type TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            try:
                cur.execute("ALTER TABLE open_positions ADD COLUMN entry_type TEXT")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            try:
                cur.execute("ALTER TABLE trades ADD COLUMN alert_mcap REAL")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
            try:
                cur.execute("ALTER TABLE open_positions ADD COLUMN alert_mcap REAL")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise


def ensure_portfolios(starting_balance):
    with db() as con:
        cur = con.cursor()
        for name in ("fixed", "scaled"):
            cur.execute(ph("SELECT 1 FROM portfolios WHERE name=?"), (name,))
            if not cur.fetchone():
                cur.execute(ph("INSERT INTO portfolios (name, balance) VALUES (?, ?)"),
                            (name, starting_balance))
                cur.execute(ph("INSERT INTO balance_history (portfolio, ts, balance) VALUES (?, ?, ?)"),
                            (name, now_iso(), starting_balance))


def get_balance(portfolio):
    with db() as con:
        cur = con.cursor()
        cur.execute(ph("SELECT balance FROM portfolios WHERE name=?"), (portfolio,))
        return cur.fetchone()[0]


def set_balance(portfolio, new_balance):
    with db() as con:
        cur = con.cursor()
        cur.execute(ph("UPDATE portfolios SET balance=? WHERE name=?"), (new_balance, portfolio))
        cur.execute(ph("INSERT INTO balance_history (portfolio, ts, balance) VALUES (?, ?, ?)"),
                    (portfolio, now_iso(), new_balance))


def manual_set_balance(portfolio, new_balance):
    """Used by the /admin panel: force a portfolio's balance to a chosen
    value, and clear any open positions (they were sized against whatever
    the balance was before this change, so they no longer make sense)."""
    with db() as con:
        cur = con.cursor()
        cur.execute(ph("UPDATE portfolios SET balance=? WHERE name=?"), (new_balance, portfolio))
        cur.execute(ph("INSERT INTO balance_history (portfolio, ts, balance) VALUES (?, ?, ?)"),
                    (portfolio, now_iso(), new_balance))
        cur.execute(ph("DELETE FROM open_positions WHERE portfolio=?"), (portfolio,))


def deployed_amount(portfolio):
    with db() as con:
        cur = con.cursor()
        cur.execute(ph("""SELECT COALESCE(SUM(bet_size * remaining_pct), 0)
                          FROM open_positions WHERE portfolio=?"""), (portfolio,))
        return cur.fetchone()[0]


# ------------------------------ alert parsing --------------------------------

CONTRACT_RE = re.compile(r"📋\s*([A-Za-z0-9]{30,50})")
# Market cap can show up as "$45,231", "45.2K", "1.2M", "2.1B", or occasionally
# "N/A" / "Pending" when a token is too fresh for the alert bot to have real
# data yet. The old version of this regex only matched the first form, so
# every other form silently failed to parse - the alert vanished with no log
# line, no DB row, nothing. That's disproportionately likely to hit very new,
# very thin-liquidity launches, which are also disproportionately where big
# runners come from. This version handles all the numeric forms; "N/A"/
# "Pending" still can't be traded (there's no entry mcap to compare against)
# but now gets logged instead of vanishing.
MCAP_RE = re.compile(r"Market Cap:\s*\$?([\d,]+\.?\d*)\s*([KkMmBb]?)")
MCAP_UNKNOWN_RE = re.compile(r"Market Cap:\s*\$?\s*(N/?A|[Pp]ending|--?)")
LAUNCH_HEADERS = ("GMGN NEW LAUNCH", "NEW LAUNCH ALERT")
_MCAP_MULT = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}


def log_skip(portfolio, contract, name, reason):
    """Record a reason an alert never became a trade, so 'why didn't this
    get traded' has an answer somewhere other than silence. portfolio is
    'parse' for failures that happen before either portfolio is even
    considered (bad contract/mcap format)."""
    with db() as con:
        con.cursor().execute(
            ph("INSERT INTO skipped (portfolio, ts, contract, name, reason) VALUES (?,?,?,?,?)"),
            (portfolio, now_iso(), contract or "", name or "", reason))
    print(f"[skip] {reason}: {name or contract or '(unknown)'}")


def parse_launch_alert(raw_text):
    # This alert channel wraps several fields (the contract address, the
    # market cap, the ticker) in backtick characters for Telegram's
    # monospace/tap-to-copy formatting - e.g. `4VvSY...pump` instead of
    # plain 4VvSY...pump. The regexes below only ever expected plain
    # characters, so any field wrapped in backticks failed to match at all,
    # and the whole alert got thrown away as "unparsed" - even for coins
    # that went on to be big winners. Stripping backticks/asterisks up
    # front fixes every regex below in one place, and is a no-op for any
    # message format that never used them.
    text = raw_text.replace("`", "").replace("*", "")

    if not any(h in text for h in LAUNCH_HEADERS):
        return None

    lines = text.split("\n")
    cmatch = CONTRACT_RE.search(text)
    contract = cmatch.group(1) if cmatch else None

    # The name usually sits on the line right before the clipboard (📋)
    # marker that precedes the contract address. Anchoring to that marker
    # is more robust than a fixed line index, since the number of divider
    # lines above it can vary between alert formats/updates.
    name = None
    clip_idx = next((i for i, l in enumerate(lines) if "📋" in l), None)
    if clip_idx is not None and clip_idx > 0 and lines[clip_idx - 1].strip():
        name = lines[clip_idx - 1].strip()
    elif len(lines) > 2 and lines[2].strip():
        name = lines[2].strip()
    if not name:
        name = contract[:8] if contract else "unknown"

    if not cmatch:
        log_skip("parse", None, name, "unparsed: no contract address matched")
        return None

    mmatch = MCAP_RE.search(text)
    if not mmatch or not mmatch.group(1):
        if MCAP_UNKNOWN_RE.search(text):
            log_skip("parse", contract, name, "unparsed: market cap not yet available (N/A/Pending)")
        else:
            log_skip("parse", contract, name, "unparsed: market cap format not recognized")
        return None

    number_part, suffix = mmatch.group(1), mmatch.group(2).upper()
    mcap = float(number_part.replace(",", "")) * _MCAP_MULT.get(suffix, 1)
    return {"contract": contract, "mcap": mcap, "name": name}


def fetch_mcap(contract):
    try:
        resp = requests.get(DEXSCREENER_URL.format(contract), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd", 0) or 0)
        mcap = best.get("marketCap") or best.get("fdv")
        return float(mcap) if mcap else None
    except Exception as e:
        print(f"[price fetch error] {contract}: {e}")
        return None


# ------------------------------ trading engine -------------------------------

class PaperEngine:
    def __init__(self, cfg):
        self.cfg = cfg

    def try_open(self, portfolio, contract, name, entry_mcap, entry_type="instant", alert_mcap=None):
        cfg = self.cfg
        balance = get_balance(portfolio)
        bet_size = balance * (cfg["position_size_pct"] / 100)
        deployed = deployed_amount(portfolio)
        cap = balance * (cfg["max_exposure_pct"] / 100)
        if deployed + bet_size > cap:
            log_skip(portfolio, contract, name,
                      f"exposure cap reached (${deployed:,.2f} deployed of ${cap:,.2f} cap)")
            return None
        if alert_mcap is None:
            alert_mcap = entry_mcap
        with db() as con:
            cur = con.cursor()
            cur.execute(ph("""
                INSERT INTO open_positions
                (portfolio, contract, name, entry_time, entry_mcap, bet_size,
                 remaining_pct, realized_pnl, peak_ratio, tier1_done, tier2_done, entry_type, alert_mcap)
                VALUES (?,?,?,?,?,?,1.0,0.0,1.0,0,0,?,?)"""),
                (portfolio, contract, name, now_iso(), entry_mcap, bet_size, entry_type, alert_mcap))
        drift = f" (alert was ${alert_mcap:,.0f})" if abs(alert_mcap - entry_mcap) > 0.01 else ""
        print(f"[{portfolio}] OPEN  {name} ({contract[:6]}...) bet=${bet_size:.2f} "
              f"entry_mcap=${entry_mcap:,.0f}{drift} entry_type={entry_type}")
        return bet_size

    async def watch_for_dip(self, contract, name, alert_mcap):
        """Watches a freshly-alerted token BEFORE any money commits, waiting
        for a qualifying pullback rather than buying instantly at the alert
        price. Returns the mcap to enter at if a qualifying dip happens, or
        None if the watch was abandoned (too sharp a drop, no price data
        ever, or it never dipped enough within the watch window) - in every
        None case, the reason is already logged via log_skip.

        Peak tracking starts from the alert price itself (a token can pump
        further before ever dipping), and the dip is always measured off
        the highest point seen so far - so a fresh new high resets what
        counts as a "dip" going forward.
        """
        cfg = self.cfg
        if not cfg.get("dip_entry_enabled", False):
            return alert_mcap  # feature off: behave exactly like instant-buy

        buy_min = cfg["dip_buy_min_pct"]
        buy_max = cfg["dip_buy_max_pct"]
        skip_pct = cfg["dip_skip_pct"]
        min_bounce = cfg.get("dip_min_bounce_pct", 0)
        max_wait = cfg["dip_watch_max_minutes"]
        no_data_wait = cfg.get("max_wait_for_price_minutes", max_wait)

        started = time.time()
        peak = alert_mcap
        trough = alert_mcap  # lowest point seen since the most recent peak
        has_price_data = False
        last_price_time = time.time()

        while True:
            await asyncio.sleep(cfg["price_poll_seconds"])
            elapsed_min = (time.time() - started) / 60
            mcap = fetch_mcap(contract)

            if mcap is None:
                if not has_price_data and elapsed_min >= no_data_wait:
                    log_skip("dip-watch", contract, name,
                              f"voided: no price data obtained within {no_data_wait}min "
                              f"while waiting for a dip entry")
                    return None
                if has_price_data and (time.time() - last_price_time) / 60 >= no_data_wait:
                    log_skip("dip-watch", contract, name,
                              f"abandoned: feed went dark while waiting for a dip entry "
                              f"(last real price was {(time.time()-last_price_time)/60:.0f}min ago)")
                    return None
                if elapsed_min >= max_wait:
                    log_skip("dip-watch", contract, name,
                              f"no price data within the {max_wait}min watch window - abandoned")
                    return None
                continue

            has_price_data = True
            last_price_time = time.time()
            if mcap >= peak:
                peak = mcap
                trough = mcap  # a fresh high resets what counts as "the low of this pullback"
            else:
                trough = min(trough, mcap)
            dip_pct = (peak - mcap) / peak * 100 if peak else 0
            bounce_pct = (mcap - trough) / trough * 100 if trough else 0

            if dip_pct >= skip_pct:
                log_skip("dip-watch", contract, name,
                          f"dip too sharp ({dip_pct:.0f}% down from peak ${peak:,.0f}) - never entered")
                return None

            if buy_min <= dip_pct <= buy_max and bounce_pct >= min_bounce:
                print(f">>> dip confirmed: {name} is {dip_pct:.0f}% down from peak ${peak:,.0f}, "
                      f"bounced {bounce_pct:.0f}% off low ${trough:,.0f} -> entering at ${mcap:,.0f}")
                return mcap

            if elapsed_min >= max_wait:
                log_skip("dip-watch", contract, name,
                          f"never reached a qualifying {buy_min}-{buy_max}% dip with {min_bounce}%+ bounce "
                          f"within {max_wait}min (last seen {dip_pct:.0f}% off peak, {bounce_pct:.0f}% bounce) - abandoned")
                return None

    def close_position(self, pos_id, portfolio, contract, name, entry_time, entry_mcap,
                        bet_size, remaining_pct, realized_pnl, exit_mcap, reason, peak_ratio=1.0,
                        entry_type="instant", alert_mcap=None):
        cfg = self.cfg
        if alert_mcap is None:
            alert_mcap = entry_mcap
        ratio = exit_mcap / entry_mcap if entry_mcap else 1.0
        peak_ratio = max(peak_ratio, ratio)  # in case the closing tick itself is the peak
        remaining_value = bet_size * remaining_pct
        fee = remaining_value * (cfg["fee_pct"] / 100)
        final_pnl = realized_pnl + remaining_value * (ratio - 1.0) - fee
        balance = get_balance(portfolio)
        new_balance = max(balance + final_pnl, 0)
        set_balance(portfolio, new_balance)
        with db() as con:
            cur = con.cursor()
            cur.execute(ph("""
                INSERT INTO trades (portfolio, contract, name, entry_time, exit_time,
                                     entry_mcap, exit_mcap, exit_reason, pnl, balance_after,
                                     peak_ratio, bet_size, entry_type, alert_mcap)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""),
                (portfolio, contract, name, entry_time, now_iso(), entry_mcap, exit_mcap,
                 reason, final_pnl, new_balance, peak_ratio, bet_size, entry_type, alert_mcap))
            cur.execute(ph("DELETE FROM open_positions WHERE id=?"), (pos_id,))
        tag = "WIN " if final_pnl > 0 else "LOSS"
        drift = f" (alert was ${alert_mcap:,.0f})" if abs(alert_mcap - entry_mcap) > 0.01 else ""
        print(f"[{portfolio}] {tag} {name} exit={reason} pnl=${final_pnl:+.2f} "
              f"balance=${new_balance:,.2f} peak={peak_ratio:.2f}x entry_type={entry_type}{drift}")

    async def monitor_position(self, pos_id, portfolio, contract, name, entry_time, entry_mcap, bet_size,
                                remaining_pct=1.0, realized_pnl=0.0, peak_ratio=1.0,
                                tier1_done=False, tier2_done=False, entry_type="instant", alert_mcap=None):
        cfg = self.cfg
        started = time.time()
        has_price_data = peak_ratio > 1.0  # a resumed position with a real peak already proves data existed
        last_known_mcap = entry_mcap
        last_price_time = time.time()
        max_wait = cfg.get("max_wait_for_price_minutes", cfg["timeout_minutes"])

        def persist_state():
            # Keeps peak_ratio (and the rest of the scaled portfolio's
            # partial-exit state) durable across restarts, not just held in
            # this coroutine's local variables.
            with db() as con:
                con.cursor().execute(ph("""
                    UPDATE open_positions
                    SET remaining_pct=?, realized_pnl=?, peak_ratio=?, tier1_done=?, tier2_done=?
                    WHERE id=?"""),
                    (remaining_pct, realized_pnl, peak_ratio, int(tier1_done), int(tier2_done), pos_id))

        def void_position(why):
            # DexScreener never returned a single real price for this
            # contract - most often because the pool hasn't been indexed
            # yet, sometimes because it's an instant rug with no real pool
            # ever forming. Either way, we never actually observed a price,
            # so charging a fee against a fabricated 1.00x "exit" was
            # dishonest bookkeeping - it made every data gap look like a
            # small loss and quietly dragged the balance down over many
            # trades ("slow death" from fees on trades that never really
            # happened, not from real losing trades). Void it instead: no
            # balance change, no trades-table row, logged separately so it's
            # visible but doesn't pollute win rate or the 1.5x stats.
            with db() as con:
                con.cursor().execute(ph("DELETE FROM open_positions WHERE id=?"), (pos_id,))
            log_skip(portfolio, contract, name, why)
            print(f"[{portfolio}] VOID {name} - {why}")

        while True:
            await asyncio.sleep(cfg["price_poll_seconds"])
            elapsed_min = (time.time() - started) / 60
            mcap = fetch_mcap(contract)

            if mcap is None:
                if not has_price_data:
                    if elapsed_min >= max_wait:
                        void_position(f"voided: no price data obtained within {max_wait}min "
                                       f"of opening (token not indexed yet, or instant rug with no real pool)")
                        return
                    continue
                # We DO have a real price history for this token; a single
                # missed poll doesn't erase it. But if the feed has been
                # dark for a long stretch since the last real reading - e.g.
                # the token got one snapshot near entry and then the pool
                # got drained or DexScreener dropped it entirely - don't sit
                # on it for the full timeout_minutes tying up exposure and
                # eventually closing on a stale number that LOOKS like a
                # boring "1.00-1.04x" close but is actually "we lost the
                # feed almost immediately." Close it now, using the last
                # real price, and say clearly how stale it is.
                stale_min = (time.time() - last_price_time) / 60
                if stale_min >= max_wait:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, last_known_mcap,
                                         f"feed went dark ({stale_min:.0f}min since last real price, "
                                         f"closed at last known {last_known_mcap/entry_mcap:.2f}x)", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, last_known_mcap,
                                         f"timeout (last real price {stale_min:.0f}min old)", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                continue

            has_price_data = True
            last_known_mcap = mcap
            last_price_time = time.time()
            ratio = mcap / entry_mcap
            peak_ratio = max(peak_ratio, ratio)

            if portfolio == "fixed":
                target = cfg["fixed_target_multiple"]
                stop = 1 - cfg["fixed_stop_loss_pct"] / 100
                if ratio >= target:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, f"target {target}x", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                if ratio <= stop:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, "stop-loss", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, "timeout", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                persist_state()
            else:
                stop = 1 - cfg["scaled_stop_loss_pct"] / 100
                trail = 1 - cfg["scaled_trailing_pct"] / 100

                if not tier1_done and ratio >= 2.0:
                    realized_pnl += bet_size * 0.5 * (2.0 - 1.0)
                    remaining_pct -= 0.5
                    tier1_done = True
                    print(f"[scaled] {name} hit 2x -> sold 50%, remaining={remaining_pct:.0%}")

                if not tier2_done and ratio >= 3.0:
                    realized_pnl += bet_size * 0.25 * (3.0 - 1.0)
                    remaining_pct -= 0.25
                    tier2_done = True
                    print(f"[scaled] {name} hit 3x -> sold 25%, remaining={remaining_pct:.0%}")

                if tier1_done and ratio <= peak_ratio * trail:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, mcap, "trailing stop", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                if not tier1_done and ratio <= stop:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, mcap, "stop-loss", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, mcap, "timeout", peak_ratio, entry_type=entry_type, alert_mcap=alert_mcap)
                    return
                persist_state()


# ------------------------------- dashboard HTML -------------------------------

def render_dashboard():
    cfg = load_config()
    portfolios = {}
    with db() as con:
        cur = con.cursor()
        for name in ("fixed", "scaled"):
            bal = get_balance(name)
            cur.execute(ph("SELECT balance FROM balance_history WHERE portfolio=? ORDER BY ts"), (name,))
            curve = [r[0] for r in cur.fetchall()]
            cur.execute(ph("SELECT COUNT(*), SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END) FROM trades WHERE portfolio=?"), (name,))
            total, wins = cur.fetchone()
            total = total or 0
            wins = wins or 0
            cur.execute(ph("SELECT COUNT(*) FROM open_positions WHERE portfolio=?"), (name,))
            open_count = cur.fetchone()[0]
            cur.execute(ph("""SELECT name, exit_reason, pnl, balance_after, peak_ratio, entry_mcap, alert_mcap
                              FROM trades WHERE portfolio=? ORDER BY id DESC LIMIT 15"""), (name,))
            recent = cur.fetchall()
            cur.execute(ph("""SELECT COUNT(*), SUM(CASE WHEN peak_ratio>=1.5 THEN 1 ELSE 0 END)
                              FROM trades WHERE portfolio=?"""), (name,))
            hit_total, hit_1_5 = cur.fetchone()
            hit_total = hit_total or 0
            hit_1_5 = hit_1_5 or 0

            # entry_type breakdown: NULL means the trade predates this
            # column and was necessarily an instant-buy, since the dip-entry
            # feature didn't exist yet.
            cur.execute(ph("""SELECT COALESCE(entry_type, 'instant'), COUNT(*),
                                     SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)
                              FROM trades WHERE portfolio=? GROUP BY COALESCE(entry_type, 'instant')"""), (name,))
            by_entry_type = {row[0]: {"total": row[1], "wins": row[2] or 0} for row in cur.fetchall()}

            portfolios[name] = {"balance": bal, "curve": curve, "total": total,
                                 "wins": wins, "open_count": open_count, "recent": recent,
                                 "hit_total": hit_total, "hit_1_5": hit_1_5,
                                 "by_entry_type": by_entry_type}

        # Hypothetical: what would the "fixed" portfolio's closed trades have
        # done under a 1.5x target instead of whatever fixed_target_multiple
        # actually was at the time? If a trade's peak_ratio ever reached
        # 1.5x, it must have passed through 1.5x before any later stop-loss
        # or timeout could trigger (price has to rise through 1.5x, then
        # fall back down through 1.0x, to reach a stop below entry) - so a
        # 1.5x target would have closed it right there, at 1.5x. If it never
        # reached 1.5x, a 1.5x target changes nothing and the real recorded
        # outcome stands.
        cur.execute("""SELECT bet_size, peak_ratio, pnl
                       FROM trades WHERE portfolio='fixed'""")
        fixed_trades_raw = cur.fetchall()

        cur.execute("""SELECT ts, contract, name, reason FROM skipped
                       ORDER BY ts DESC LIMIT 20""")
        recent_skips = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM skipped WHERE reason LIKE 'exposure cap%'")
        skip_cap_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM skipped WHERE reason LIKE 'unparsed%'")
        skip_parse_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM skipped WHERE reason LIKE 'voided%'")
        skip_void_count = cur.fetchone()[0]

    # Hypothetical: what would "fixed" portfolio trades have done under a
    # 1.5x target instead of whatever fixed_target_multiple actually was?
    # If a trade's peak_ratio ever reached 1.5x, it must have passed through
    # 1.5x before any later stop-loss or timeout could trigger (price has to
    # rise through 1.5x, then fall back down through 1.0x, to reach a stop
    # below entry) - so a 1.5x target would have closed it right there, at
    # 1.5x. If it never reached 1.5x, a 1.5x target changes nothing and the
    # real recorded outcome stands. Uses each trade's own actual bet_size,
    # so it's exact even if position_size_pct or fee_pct changed over time.
    hypo_target = 1.5
    fee_pct = cfg["fee_pct"]
    actual_total = sum(pnl for _, _, pnl in fixed_trades_raw)
    hypo_hit_count = sum(1 for _, peak, _ in fixed_trades_raw if peak is not None and peak >= hypo_target)
    hypo_total = 0.0
    for bet_size, peak_ratio, pnl in fixed_trades_raw:
        if bet_size is not None and peak_ratio is not None and peak_ratio >= hypo_target:
            hypo_total += bet_size * ((hypo_target - 1) - fee_pct / 100)
        else:
            hypo_total += pnl  # never reached 1.5x (or pre-migration row missing bet_size): unaffected
    fixed_trade_count = len(fixed_trades_raw)

    def curve_svg(curve, color):
        if len(curve) < 2:
            return "<div style='color:#948c7c;font-size:12px;'>Not enough data yet.</div>"
        pts = curve[-300:]
        max_v, min_v = max(pts), min(pts)
        rng = (max_v - min_v) or 1
        w, h = 560, 160
        path = []
        for i, v in enumerate(pts):
            x = (i / (len(pts) - 1)) * (w - 20) + 10
            y = h - 15 - ((v - min_v) / rng) * (h - 30)
            path.append(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}")
        return f'<svg viewBox="0 0 {w} {h}" style="width:100%;height:160px;"><path d="{" ".join(path)}" fill="none" stroke="{color}" stroke-width="2"/></svg>'

    def rows_html(recent):
        if not recent:
            return '<div class="muted">No closed trades yet.</div>'
        out = ['<table><tr><th>Token</th><th>Exit</th><th>P&L</th><th>Balance</th>'
               '<th>Peak</th><th>1.5x?</th><th>Entry vs alert</th></tr>']
        for name, reason, pnl, bal_after, peak_ratio, entry_mcap, alert_mcap in recent:
            cls = "win" if pnl > 0 else "loss"
            peak_str = f"{peak_ratio:.2f}x" if peak_ratio is not None else "—"
            hit_str = ('<span class="win">yes</span>' if peak_ratio is not None and peak_ratio >= 1.5
                       else ('<span class="loss">no</span>' if peak_ratio is not None else "—"))
            # Shows how much of the move was already "spent" waiting for a
            # dip - e.g. if the alert fired at $9,000 but we didn't buy
            # until $12,850, that gap is exactly why our measured multiple
            # on a token can look smaller than the channel's own headline
            # (which measures from the alert price, not our actual entry).
            if alert_mcap and entry_mcap and abs(alert_mcap - entry_mcap) > 0.01:
                drift_pct = (entry_mcap - alert_mcap) / alert_mcap * 100
                drift_str = f"${alert_mcap:,.0f} → ${entry_mcap:,.0f} ({drift_pct:+.0f}%)"
            else:
                drift_str = "instant buy"
            out.append(f'<tr><td>{name}</td><td>{reason}</td>'
                       f'<td class="{cls}">${pnl:+.2f}</td><td>${bal_after:,.2f}</td>'
                       f'<td>{peak_str}</td><td>{hit_str}</td><td>{drift_str}</td></tr>')
        out.append("</table>")
        return "".join(out)

    def panel(title, p, color):
        wr = (p["wins"] / p["total"] * 100) if p["total"] else 0
        hr = (p["hit_1_5"] / p["hit_total"] * 100) if p["hit_total"] else 0
        bet = p["by_entry_type"]
        entry_split_html = ""
        if bet:
            parts = []
            for etype in ("dip", "instant"):
                if etype in bet and bet[etype]["total"]:
                    t, w = bet[etype]["total"], bet[etype]["wins"]
                    parts.append(f'{etype}: {w}/{t} ({w/t*100:.0f}%)')
            if parts:
                entry_split_html = (f'<div class="muted" style="margin-top:8px;">'
                                     f'Win rate by entry type — {" · ".join(parts)}</div>')
        return f"""
        <div class="panel">
          <h2>{title}</h2>
          <div class="balance">${p['balance']:,.2f}</div>
          <div class="stats">
            <div><span class="muted">Win rate</span><br>{wr:.1f}%</div>
            <div><span class="muted">Trades closed</span><br>{p['total']}</div>
            <div><span class="muted">Open now</span><br>{p['open_count']}</div>
            <div><span class="muted">Ever hit 1.5x</span><br>{p['hit_1_5']}/{p['hit_total']} ({hr:.0f}%)</div>
          </div>
          {entry_split_html}
          {curve_svg(p['curve'], color)}
          <h3>Recent trades</h3>
          {rows_html(p['recent'])}
        </div>"""

    def skips_html():
        if not recent_skips:
            return '<div class="muted">No skipped alerts recorded.</div>'
        out = ['<table><tr><th>Time</th><th>Token</th><th>Reason</th></tr>']
        for ts, contract, name, reason in recent_skips:
            label = name or (contract[:8] + "..." if contract else "(unknown)")
            try:
                t = datetime.fromisoformat(ts).strftime("%m-%d %H:%M")
            except Exception:
                t = ts
            out.append(f'<tr><td>{t}</td><td>{label}</td><td>{reason}</td></tr>')
        out.append("</table>")
        return "".join(out)

    hypo_diff = hypo_total - actual_total
    hypo_diff_cls = "win" if hypo_diff > 0 else ("loss" if hypo_diff < 0 else "")
    hypo_panel = f"""
        <div class="panel" style="max-width:1200px;margin:24px auto 0;">
          <h2>If the fixed target had been 1.5x</h2>
          <div class="muted">Calculated from real history: every closed "fixed" trade's actual
          recorded peak. If a trade's peak reached 1.5x, it's re-priced at exactly 1.5x
          (minus the same fee); if it never reached 1.5x, its real outcome is unchanged.</div>
          <div class="stats" style="margin-top:14px;">
            <div><span class="muted">Fixed trades closed</span><br>{fixed_trade_count}</div>
            <div><span class="muted">Would have hit 1.5x</span><br>{hypo_hit_count}/{fixed_trade_count}</div>
            <div><span class="muted">Actual total P&L</span><br>${actual_total:,.2f}</div>
            <div><span class="muted">Hypothetical 1.5x total P&L</span><br>${hypo_total:,.2f}</div>
            <div><span class="muted">Difference</span><br><span class="{hypo_diff_cls}">${hypo_diff:+,.2f}</span></div>
          </div>
        </div>"""

    skips_panel = f"""
        <div class="panel" style="max-width:1200px;margin:24px auto 0;">
          <h2>Skipped alerts</h2>
          <div class="stats">
            <div><span class="muted">Exposure cap skips</span><br>{skip_cap_count}</div>
            <div><span class="muted">Unparsed alert skips</span><br>{skip_parse_count}</div>
            <div><span class="muted">Voided (no price data)</span><br>{skip_void_count}</div>
          </div>
          <h3>Most recent 20</h3>
          {skips_html()}
        </div>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta http-equiv="refresh" content="20">
<title>Paper bot dashboard</title>
<style>
  body {{ background:#14120f; color:#ece6d9; font-family: -apple-system, sans-serif; padding:30px; }}
  .grid {{ display:grid; grid-template-columns:1fr 1fr; gap:24px; max-width:1200px; margin:0 auto; }}
  .panel {{ background:#1d1a15; border:1px solid #34302a; border-radius:8px; padding:22px; }}
  h1 {{ text-align:center; font-weight:600; }}
  h2 {{ margin:0 0 4px; color:#d6a24c; }}
  h3 {{ font-size:13px; color:#948c7c; margin:18px 0 8px; }}
  .balance {{ font-size:32px; font-weight:600; font-family:monospace; margin:6px 0 14px; }}
  .stats {{ display:flex; gap:24px; margin-bottom:16px; font-family:monospace; }}
  .muted {{ color:#948c7c; font-size:12px; font-family:sans-serif; }}
  table {{ width:100%; border-collapse:collapse; font-size:12.5px; }}
  th, td {{ text-align:left; padding:5px 6px; border-bottom:1px solid #34302a; }}
  .win {{ color:#7fae7f; }}
  .loss {{ color:#c2645a; }}
  .updated {{ text-align:center; color:#948c7c; font-size:12px; margin-top:20px; }}
</style></head>
<body>
  <h1>Paper trading bot</h1>
  <div class="grid">
    {panel("Fixed target", portfolios["fixed"], "#7fae7f")}
    {panel("Scaled + trailing", portfolios["scaled"], "#d6a24c")}
  </div>
  {hypo_panel}
  {skips_panel}
  <div class="updated">Auto-refreshes every 20s · last updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
  &nbsp;·&nbsp;<a href="/admin" style="color:#948c7c;">set balance</a></div>
</body></html>"""


# --------------------------- Flask (health check + dashboard) ----------------

flask_app = Flask(__name__)


@flask_app.route("/")
def health():
    # This is what UptimeRobot should ping - fast, no DB dependency required
    # to answer, so a health check never fails just because the DB is slow.
    return "OK - paper bot is running. See /dashboard for results."


@flask_app.route("/dashboard")
def dashboard():
    try:
        return render_dashboard()
    except Exception as e:
        return f"Dashboard error (bot may still be starting up): {e}", 500


ADMIN_FORM = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>Admin</title>
<style>
  body {{ background:#14120f; color:#ece6d9; font-family:-apple-system,sans-serif;
         padding:40px; max-width:420px; margin:0 auto; }}
  h1 {{ font-size:18px; }}
  label {{ display:block; margin:16px 0 6px; color:#948c7c; font-size:13px; }}
  input {{ width:100%; padding:9px 10px; background:#1d1a15; border:1px solid #34302a;
          color:#ece6d9; border-radius:4px; font-family:monospace; box-sizing:border-box; }}
  button {{ margin-top:20px; width:100%; padding:11px; background:#d6a24c; color:#1a1712;
           border:none; border-radius:4px; font-weight:600; cursor:pointer; }}
  .msg {{ background:#1d1a15; border-left:3px solid #d6a24c; padding:10px 14px;
         border-radius:4px; font-size:13px; margin-bottom:16px; }}
  a {{ color:#d6a24c; }}
</style></head>
<body>
  <h1>Set balance</h1>
  {message}
  <form method="POST" action="/admin/set-balance">
    <label>Admin key</label>
    <input type="password" name="key" required>
    <label>Fixed-target portfolio balance ($) - leave blank to leave unchanged</label>
    <input type="number" step="0.01" name="fixed_balance" placeholder="e.g. 20">
    <label>Scaled+trailing portfolio balance ($) - leave blank to leave unchanged</label>
    <input type="number" step="0.01" name="scaled_balance" placeholder="e.g. 20">
    <button type="submit">Set balance</button>
  </form>
  <p style="margin-top:20px;font-size:12px;color:#948c7c;">
    Setting a balance clears that portfolio's open positions (they were sized
    against the old balance) but keeps its trade history.
    <br><br><a href="/dashboard">&larr; back to dashboard</a>
  </p>
</body></html>"""


@flask_app.route("/admin")
def admin_form():
    if not ADMIN_KEY:
        return ("Admin panel is disabled: set an ADMIN_KEY environment variable "
                "on Render to enable it.", 403)
    return ADMIN_FORM.format(message="")


@flask_app.route("/admin/set-balance", methods=["POST"])
def admin_set_balance():
    if not ADMIN_KEY:
        return ("Admin panel is disabled: set an ADMIN_KEY environment variable "
                "on Render to enable it.", 403)
    if request.form.get("key") != ADMIN_KEY:
        return (ADMIN_FORM.format(
            message='<div class="msg">Wrong admin key - nothing was changed.</div>'), 403)

    changed = []
    for portfolio, field in [("fixed", "fixed_balance"), ("scaled", "scaled_balance")]:
        raw = request.form.get(field, "").strip()
        if raw:
            try:
                amount = float(raw)
                manual_set_balance(portfolio, amount)
                changed.append(f"{portfolio} set to ${amount:,.2f}")
            except ValueError:
                pass
    return redirect("/dashboard")


def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)


# --------------------------------- main ---------------------------------------

async def main():
    if not all([TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_CHANNEL]):
        raise SystemExit(
            "Missing required environment variables. Set TELEGRAM_API_ID, "
            "TELEGRAM_API_HASH, and TELEGRAM_CHANNEL. See SETUP.md."
        )

    cfg = load_config()
    init_db()
    ensure_portfolios(cfg["starting_balance"])
    engine = PaperEngine(cfg)

    # start the Flask health/dashboard server in a background thread
    threading.Thread(target=run_flask, daemon=True).start()
    print(f"Health/dashboard server listening on port {PORT}")

    client = TelegramClient(StringSession(TELEGRAM_SESSION), int(TELEGRAM_API_ID), TELEGRAM_API_HASH)

    # resume monitoring any positions left open from before a restart
    with db() as con:
        cur = con.cursor()
        cur.execute("""SELECT id, portfolio, contract, name, entry_time, entry_mcap, bet_size,
                              remaining_pct, realized_pnl, peak_ratio, tier1_done, tier2_done,
                              entry_type, alert_mcap
                       FROM open_positions""")
        open_rows = cur.fetchall()
    for row in open_rows:
        pos_id, portfolio, contract, name, entry_time, entry_mcap, bet_size, \
            remaining_pct, realized_pnl, peak_ratio, tier1_done, tier2_done, \
            entry_type, alert_mcap = row
        asyncio.create_task(engine.monitor_position(
            pos_id, portfolio, contract, name, entry_time, entry_mcap, bet_size,
            remaining_pct=remaining_pct, realized_pnl=realized_pnl, peak_ratio=peak_ratio,
            tier1_done=bool(tier1_done), tier2_done=bool(tier2_done),
            entry_type=entry_type or "instant", alert_mcap=alert_mcap))
    if open_rows:
        print(f"Resumed monitoring {len(open_rows)} position(s) from before restart "
              f"(peak ratios and partial-exit state restored, not reset).")

    # Never allow an interactive login attempt on a headless server - if the
    # session isn't already valid, client.start() would otherwise sit here
    # silently forever waiting for a phone/code prompt nobody can answer.
    # A hard timeout is also needed: connect() has none by default, so any
    # network trouble reaching Telegram would otherwise hang forever with
    # zero output, indistinguishable from "still working."
    print("Connecting to Telegram...", flush=True)
    try:
        await asyncio.wait_for(client.connect(), timeout=30)
    except asyncio.TimeoutError:
        raise SystemExit(
            "Timed out after 30s trying to reach Telegram's servers. This "
            "points to a network-level problem from Render's side (outbound "
            "connection to Telegram being slow/blocked), not a code or "
            "credentials issue. Try redeploying once; if it keeps timing "
            "out, that's worth flagging to Render support."
        )
    print("Connected. Checking session authorization...", flush=True)

    if not await client.is_user_authorized():
        raise SystemExit(
            "Telegram session is missing or invalid. This means the "
            "TELEGRAM_SESSION environment variable on Render is empty, "
            "wrong, or got mangled when it was pasted in (stray quotes or "
            "line breaks are a common cause). Fix: run generate_session.py "
            "locally again, copy the ENTIRE printed string with no extra "
            "characters, and paste it into Render's TELEGRAM_SESSION "
            "environment variable, then redeploy."
        )
    print("Session is valid. Looking up your channel...", flush=True)

    async def find_channel():
        exact_match = None
        loose_match = None
        count = 0
        async for dialog in client.iter_dialogs():
            count += 1
            if count % 25 == 0:
                print(f"  ...scanned {count} chats so far", flush=True)
            if dialog.name == TELEGRAM_CHANNEL:
                return dialog.entity, dialog.name, count
            if loose_match is None and dialog.name.strip().lower() == TELEGRAM_CHANNEL.strip().lower():
                loose_match = (dialog.entity, dialog.name)
        if loose_match:
            return loose_match[0], loose_match[1], count
        return None, None, count

    try:
        channel_entity, matched_name, scanned = await asyncio.wait_for(find_channel(), timeout=45)
    except asyncio.TimeoutError:
        raise SystemExit(
            "Timed out after 45s scanning your chat list for a match. This "
            "can happen with a very large number of chats, or another "
            "network hiccup talking to Telegram. Try redeploying once; if "
            "it keeps happening, this may need pagination/rate-limit "
            "handling added for accounts with a lot of chats."
        )

    print(f"Finished scanning {scanned} chats.", flush=True)
    if channel_entity is None:
        raise SystemExit(
            f"Could not find a chat titled '{TELEGRAM_CHANNEL}' among your {scanned} "
            f"Telegram chats. This means either: the name doesn't match exactly (check "
            f"for typos, extra spaces, or a trailing emoji in the real channel name), or "
            f"the account this session belongs to isn't a member of that channel. "
            f"No alerts can be received until this resolves."
        )
    if matched_name != TELEGRAM_CHANNEL:
        print(f"Note: matched '{matched_name}' by case-insensitive title "
              f"(TELEGRAM_CHANNEL was set to '{TELEGRAM_CHANNEL}').", flush=True)
    print(f"Connected to channel: {matched_name}", flush=True)

    watching_contracts = set()  # contracts currently in the dip-watch phase, to dedup repeat alerts

    @client.on(events.NewMessage(chats=channel_entity))
    async def handler(event):
        alert = parse_launch_alert(event.raw_text or "")
        if not alert:
            return
        contract, name, alert_mcap = alert["contract"], alert["name"], alert["mcap"]
        print(f"\n>>> alert: {name} ({contract[:6]}...) mcap=${alert_mcap:,.0f}")

        with db() as con:
            cur = con.cursor()
            cur.execute(ph("SELECT COUNT(*) FROM open_positions WHERE contract=?"), (contract,))
            already_open = cur.fetchone()[0]
        if already_open or contract in watching_contracts:
            return
        watching_contracts.add(contract)
        asyncio.create_task(watch_then_open(contract, name, alert_mcap))

    async def watch_then_open(contract, name, alert_mcap):
        entry_type = "dip" if engine.cfg.get("dip_entry_enabled", False) else "instant"
        try:
            entry_mcap = await engine.watch_for_dip(contract, name, alert_mcap)
            if entry_mcap is None:
                return  # abandoned; watch_for_dip already logged why
            for portfolio in ("fixed", "scaled"):
                with db() as con:
                    cur = con.cursor()
                    cur.execute(ph("SELECT COUNT(*) FROM open_positions WHERE contract=? AND portfolio=?"),
                                (contract, portfolio))
                    already_open = cur.fetchone()[0]
                if already_open:
                    continue
                bet_size = engine.try_open(portfolio, contract, name, entry_mcap,
                                            entry_type=entry_type, alert_mcap=alert_mcap)
                if bet_size is None:
                    continue
                with db() as con:
                    cur = con.cursor()
                    cur.execute(ph("""SELECT id, portfolio, contract, name, entry_time, entry_mcap, bet_size
                                      FROM open_positions WHERE contract=? AND portfolio=?
                                      ORDER BY id DESC LIMIT 1"""), (contract, portfolio))
                    row = cur.fetchone()
                if row:
                    asyncio.create_task(engine.monitor_position(*row, entry_type=entry_type, alert_mcap=alert_mcap))
        finally:
            watching_contracts.discard(contract)

    print("Listening for launch alerts...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
