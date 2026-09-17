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
MCAP_RE = re.compile(r"Market Cap:\s*\$?([\d,]+)")
LAUNCH_HEADERS = ("GMGN NEW LAUNCH", "NEW LAUNCH ALERT")


def parse_launch_alert(text):
    if not any(h in text for h in LAUNCH_HEADERS):
        return None
    cmatch = CONTRACT_RE.search(text)
    mmatch = MCAP_RE.search(text)
    if not cmatch or not mmatch:
        return None
    contract = cmatch.group(1)
    mcap = float(mmatch.group(1).replace(",", ""))
    lines = text.split("\n")
    name = lines[2].strip() if len(lines) > 2 and lines[2].strip() else contract[:8]
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

    def try_open(self, portfolio, contract, name, entry_mcap):
        cfg = self.cfg
        balance = get_balance(portfolio)
        bet_size = balance * (cfg["position_size_pct"] / 100)
        deployed = deployed_amount(portfolio)
        cap = balance * (cfg["max_exposure_pct"] / 100)
        if deployed + bet_size > cap:
            with db() as con:
                con.cursor().execute(
                    ph("INSERT INTO skipped (portfolio, ts, contract, name, reason) VALUES (?,?,?,?,?)"),
                    (portfolio, now_iso(), contract, name, "exposure cap reached"))
            print(f"[{portfolio}] skipped {name} - exposure cap reached")
            return None
        with db() as con:
            cur = con.cursor()
            cur.execute(ph("""
                INSERT INTO open_positions
                (portfolio, contract, name, entry_time, entry_mcap, bet_size,
                 remaining_pct, realized_pnl, peak_ratio, tier1_done, tier2_done)
                VALUES (?,?,?,?,?,?,1.0,0.0,1.0,0,0)"""),
                (portfolio, contract, name, now_iso(), entry_mcap, bet_size))
        print(f"[{portfolio}] OPEN  {name} ({contract[:6]}...) bet=${bet_size:.2f} entry_mcap=${entry_mcap:,.0f}")
        return bet_size

    def close_position(self, pos_id, portfolio, contract, name, entry_time, entry_mcap,
                        bet_size, remaining_pct, realized_pnl, exit_mcap, reason):
        cfg = self.cfg
        ratio = exit_mcap / entry_mcap if entry_mcap else 1.0
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
                                     entry_mcap, exit_mcap, exit_reason, pnl, balance_after)
                VALUES (?,?,?,?,?,?,?,?,?,?)"""),
                (portfolio, contract, name, entry_time, now_iso(), entry_mcap, exit_mcap,
                 reason, final_pnl, new_balance))
            cur.execute(ph("DELETE FROM open_positions WHERE id=?"), (pos_id,))
        tag = "WIN " if final_pnl > 0 else "LOSS"
        print(f"[{portfolio}] {tag} {name} exit={reason} pnl=${final_pnl:+.2f} balance=${new_balance:,.2f}")

    async def monitor_position(self, pos_id, portfolio, contract, name, entry_time, entry_mcap, bet_size):
        cfg = self.cfg
        started = time.time()
        remaining_pct = 1.0
        realized_pnl = 0.0
        peak_ratio = 1.0
        tier1_done = False
        tier2_done = False

        while True:
            await asyncio.sleep(cfg["price_poll_seconds"])
            elapsed_min = (time.time() - started) / 60
            mcap = fetch_mcap(contract)

            if mcap is None:
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, entry_mcap, "timeout (no price data)")
                    return
                continue

            ratio = mcap / entry_mcap
            peak_ratio = max(peak_ratio, ratio)

            if portfolio == "fixed":
                target = cfg["fixed_target_multiple"]
                stop = 1 - cfg["fixed_stop_loss_pct"] / 100
                if ratio >= target:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, f"target {target}x")
                    return
                if ratio <= stop:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, "stop-loss")
                    return
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, 1.0, 0.0, mcap, "timeout")
                    return
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
                                         bet_size, remaining_pct, realized_pnl, mcap, "trailing stop")
                    return
                if not tier1_done and ratio <= stop:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, mcap, "stop-loss")
                    return
                if elapsed_min >= cfg["timeout_minutes"]:
                    self.close_position(pos_id, portfolio, contract, name, entry_time, entry_mcap,
                                         bet_size, remaining_pct, realized_pnl, mcap, "timeout")
                    return


# ------------------------------- dashboard HTML -------------------------------

def render_dashboard():
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
            cur.execute(ph("""SELECT name, exit_reason, pnl, balance_after
                              FROM trades WHERE portfolio=? ORDER BY id DESC LIMIT 15"""), (name,))
            recent = cur.fetchall()
            portfolios[name] = {"balance": bal, "curve": curve, "total": total,
                                 "wins": wins, "open_count": open_count, "recent": recent}

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
        out = ['<table><tr><th>Token</th><th>Exit</th><th>P&L</th><th>Balance</th></tr>']
        for name, reason, pnl, bal_after in recent:
            cls = "win" if pnl > 0 else "loss"
            out.append(f'<tr><td>{name}</td><td>{reason}</td>'
                       f'<td class="{cls}">${pnl:+.2f}</td><td>${bal_after:,.2f}</td></tr>')
        out.append("</table>")
        return "".join(out)

    def panel(title, p, color):
        wr = (p["wins"] / p["total"] * 100) if p["total"] else 0
        return f"""
        <div class="panel">
          <h2>{title}</h2>
          <div class="balance">${p['balance']:,.2f}</div>
          <div class="stats">
            <div><span class="muted">Win rate</span><br>{wr:.1f}%</div>
            <div><span class="muted">Trades closed</span><br>{p['total']}</div>
            <div><span class="muted">Open now</span><br>{p['open_count']}</div>
          </div>
          {curve_svg(p['curve'], color)}
          <h3>Recent trades</h3>
          {rows_html(p['recent'])}
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
        cur.execute("SELECT id, portfolio, contract, name, entry_time, entry_mcap, bet_size FROM open_positions")
        open_rows = cur.fetchall()
    for row in open_rows:
        asyncio.create_task(engine.monitor_position(*row))
    if open_rows:
        print(f"Resumed monitoring {len(open_rows)} position(s) from before restart.")

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

    @client.on(events.NewMessage(chats=channel_entity))
    async def handler(event):
        alert = parse_launch_alert(event.raw_text or "")
        if not alert:
            return
        print(f"\n>>> alert: {alert['name']} ({alert['contract'][:6]}...) mcap=${alert['mcap']:,.0f}")
        for portfolio in ("fixed", "scaled"):
            with db() as con:
                cur = con.cursor()
                cur.execute(ph("SELECT COUNT(*) FROM open_positions WHERE contract=? AND portfolio=?"),
                            (alert["contract"], portfolio))
                already_open = cur.fetchone()[0]
            if already_open:
                continue
            bet_size = engine.try_open(portfolio, alert["contract"], alert["name"], alert["mcap"])
            if bet_size is None:
                continue
            with db() as con:
                cur = con.cursor()
                cur.execute(ph("""SELECT id, portfolio, contract, name, entry_time, entry_mcap, bet_size
                                  FROM open_positions WHERE contract=? AND portfolio=?
                                  ORDER BY id DESC LIMIT 1"""), (alert["contract"], portfolio))
                row = cur.fetchone()
            if row:
                asyncio.create_task(engine.monitor_position(*row))

    print("Listening for launch alerts...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
