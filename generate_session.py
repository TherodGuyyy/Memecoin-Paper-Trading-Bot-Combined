"""
Run this ONCE, on your own computer (not on Render), to log into Telegram
and generate a "string session" - a piece of text that lets the bot stay
logged in on a server without ever typing a login code there.

Usage:
    python generate_session.py

It will ask for your api_id, api_hash (from https://my.telegram.org), your
phone number, and the login code Telegram texts you. At the end it prints a
long string - copy that whole string into Render's environment variables as
TELEGRAM_SESSION (see SETUP.md).

Keep that string private - it's equivalent to being logged into your Telegram
account. Don't commit it to GitHub, don't share it, don't post it anywhere.
"""

from telethon.sync import TelegramClient
from telethon.sessions import StringSession

print("=== Telegram session generator ===\n")
api_id = input("Enter your api_id (from my.telegram.org): ").strip()
api_hash = input("Enter your api_hash (from my.telegram.org): ").strip()

with TelegramClient(StringSession(), int(api_id), api_hash) as client:
    session_string = client.session.save()

print("\n=== Success! Here is your session string ===\n")
print(session_string)
print("\nCopy the line above (all of it) into Render's environment variables")
print("as TELEGRAM_SESSION. Keep it private - treat it like a password.")
