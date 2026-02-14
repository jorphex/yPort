# yPort

A Telegram + Discord bot that shows your Yearn vault positions from wallet addresses or ENS names.

## Features

- Portfolio report with totals and recent yield changes.
- Vault suggestions for assets you already hold.
- Telegram-only: Daily reports (toggle per user).
- Discord‑only: scheduled top‑vaults report in a public channel.
- Buttons and modals for faster actions (no need to type everything).
- Shared cache and report engine across both platforms.
- Long reports are split into multiple messages to avoid truncation.

## Setup

Copy `.env.example` to `.env` and fill in the required tokens and IDs.

## Run With Docker

```bash
touch yport.db && \
docker build --no-cache -t yport . && \
docker run -d --restart always --name yport \
  --env-file .env \
  -v "$(pwd)/yport.db":/app/yport.db \
  yport && \
docker logs -f yport
```

## Run Locally

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python3 main.py
```

## Commands

Telegram:
- `/start`
- `/yport`
- `/addresses`
- `/dailytoggle`
- `/help`

Discord:
- `/yport`
- `/addresses`
- `/help`

## Notes

- The database file is `yport.db` unless you set `DB_PATH`.
- Reports are split by chain and by 10 vaults to stay within message limits.
