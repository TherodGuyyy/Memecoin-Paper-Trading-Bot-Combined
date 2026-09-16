# Getting the paper bot live on Render

This walks through everything in order: putting the code on GitHub, deploying
it to Render, connecting it to your real memecoin bot's Telegram channel, and
setting up UptimeRobot so it never goes to sleep. No coding required, just
following steps.

Total time: about 20-30 minutes, mostly waiting for things to load.

---

## Part 1 - Get a Telegram "session string" (5 min, on your own computer)

Render can't answer an interactive "we texted you a code, type it in" login
prompt the way your own computer can. So you log in **once, locally**, and
that produces a piece of text Render can use to stay logged in without ever
seeing a login prompt.

1. Install Python on your computer if you don't have it: https://www.python.org/downloads/
   (Windows: check "Add Python to PATH" during install.)
2. Get free API credentials from Telegram:
   - Go to https://my.telegram.org and log in with your phone number.
   - Click "API development tools."
   - Fill in any app name (e.g. "paperbot") and submit.
   - You'll get an **api_id** (a number) and an **api_hash** (a long string). Save both somewhere.
3. Download this project's files into one folder on your computer.
4. Open a terminal in that folder and run:
   ```
   pip install -r requirements.txt
   python generate_session.py
   ```
5. It'll ask for your api_id and api_hash, then your phone number, then the
   login code Telegram texts you. Type each one in.
6. It prints a long string at the end. **Copy that whole string** somewhere
   safe (a notes app is fine) - you'll paste it into Render in Part 3.
   Treat it like a password: don't share it, don't post it publicly.

---

## Part 2 - Push the code to GitHub

1. If you don't have a GitHub account, make one free at https://github.com.
2. Create a new repository (the "+" icon top right → "New repository").
   It can be private - only you need access.
3. Upload all the project files to it. Easiest way with no command-line git:
   on the repo's page, click "uploading an existing file" and drag in every
   file from the folder (`paper_bot.py`, `config.json`, `requirements.txt`,
   `render.yaml`, `.gitignore`, `generate_session.py`, `SETUP.md`).
4. **Do not upload** any `.session` file or a `paperbot.db` file if you
   happened to create one while testing locally - the `.gitignore` file
   is there to stop that, but double check nothing with your session
   string or api_hash ends up in the repo itself. Those live in Render's
   environment variables instead (next part), never in the code.

---

## Part 3 - Deploy to Render

1. Make a free account at https://render.com and connect your GitHub account.
2. Click "New +" → "Blueprint," and pick the repository you just created.
   Render will read the included `render.yaml` file and automatically set
   up both the web service and a free Postgres database for you.
3. It'll ask you to fill in a few environment variables it couldn't guess
   (marked as secrets in the blueprint):
   - `TELEGRAM_API_ID` - the number from Part 1
   - `TELEGRAM_API_HASH` - the string from Part 1
   - `TELEGRAM_SESSION` - the long string `generate_session.py` printed
   - `TELEGRAM_CHANNEL` - the exact name of your memecoin bot's alert channel
4. Click deploy. Render will install everything and start the bot - this
   takes a few minutes the first time. You can watch it happen in the
   "Logs" tab; when you see `Listening for launch alerts...` it's working.
5. Your service will have a URL like `https://paperbot-xxxx.onrender.com`.
   Visit `https://paperbot-xxxx.onrender.com/dashboard` to see your live
   results - bookmark this, it's your paper trading dashboard from now on,
   viewable from any device, anywhere.

**Important - the free Postgres database expires after 30 days.** That
comfortably covers the "few weeks" test you mentioned, but if you want to
keep going past that, Render will warn you before it expires so you can
upgrade it (a few dollars/month) or export your trade history first.

---

## Part 4 - Keep it awake with UptimeRobot

Render's free tier puts your service to sleep after 15 minutes with no
incoming traffic, and takes about a minute to wake back up. Since your bot
needs to be listening continuously, not waking up on demand, UptimeRobot
pings it regularly so it never gets the chance to fall asleep.

1. Make a free account at https://uptimerobot.com.
2. Add a new monitor:
   - Monitor type: HTTP(s)
   - URL: `https://paperbot-xxxx.onrender.com/` (the plain root URL, **not**
     `/dashboard` - the root is a fast health check that doesn't touch the
     database, so it can't fail just because the DB is briefly busy)
   - Monitoring interval: every 5 minutes (well under Render's 15-minute
     idle window)
3. Save it. That's it - UptimeRobot will now hit your bot every 5 minutes,
   which keeps Render from ever idling it out.

One honest limitation worth knowing: Render says it "may restart a free
service at any time" for its own maintenance reasons, separate from the
idle-sleep issue UptimeRobot solves. That's exactly why this version stores
everything in Postgres instead of a local file - a restart won't wipe your
trade history, it'll just pick back up where it left off (the bot resumes
monitoring any positions that were still open before the restart).

---

## Part 5 - Point it at your real memecoin bot

If `TELEGRAM_CHANNEL` in Part 3 already matches your memecoin bot's alert
channel exactly, you're already connected - nothing else to do. The bot
started listening the moment it deployed.

To double check it's really receiving alerts: watch the Render "Logs" tab
next time your memecoin bot posts a new launch alert. You should see a line
like:

```
>>> alert: SomeToken (7xKq2m...) mcap=$18,375
[fixed] OPEN  SomeToken (7xKq2m...) bet=$50.00 entry_mcap=$18,375
[scaled] OPEN  SomeToken (7xKq2m...) bet=$50.00 entry_mcap=$18,375
```

If you don't see anything after a launch alert posts, the most likely cause
is `TELEGRAM_CHANNEL` not matching the channel's exact name - open it in
Telegram and copy the name precisely (case and spacing matter).

---

## Changing settings later

`config.json` holds the non-secret trading parameters (balance, position
size, targets, stop-loss, etc.) - the same ones from the backtest tool.
To change them: edit the file on GitHub directly (or push an update),
Render will automatically redeploy with the new settings. Note that editing
`starting_balance` only affects a brand-new database - it won't reset your
current balance, since the bot only reads it once when a portfolio doesn't
exist yet. To force a full reset, you'd need to clear the Postgres tables
(ask me if you want a small reset script for this).

## What this still does NOT do

- No real money moves. It only reads Telegram and reads free public
  DexScreener price data - it cannot place real trades.
- It can't retroactively "catch" alerts that happened while it was down for
  any reason (rare Render restarts, etc.) - it only reacts to alerts posted
  while it's actively running.
