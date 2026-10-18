"""notify.py — send one message to every configured channel (Telegram / Email / Teams).

Channels are enabled by environment variables (backend/.env locally, GitHub secrets in Actions);
a channel with missing settings is skipped, and one channel failing never blocks the others.

  Telegram  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID            (bot from @BotFather)
  Email     SMTP_USER, SMTP_APP_PASSWORD, NOTIFY_EMAIL_TO   (Gmail app password; TO may be comma-separated)
            SMTP_HOST (default smtp.gmail.com), SMTP_PORT (default 465)
  Teams     TEAMS_WEBHOOK_URL                               (Teams "Workflows" incoming webhook)

Usage:
  python backend/notify.py --test                         send a test message
  python backend/notify.py --subject "..." --file report.md
  python backend/notify.py --status                       show which channels are configured
"""
import argparse
import html
import os
import smtplib
import ssl
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

BACKEND_DIR = os.path.dirname(os.path.abspath(__file__))
TELEGRAM_LIMIT = 4000  # API limit is 4096 chars per message
TIMEOUT = 20


def load_env():
    path = os.path.join(BACKEND_DIR, ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def _env(*names):
    vals = [os.environ.get(n, "").strip() for n in names]
    return vals if all(vals) else None


def configured():
    return {
        "telegram": bool(_env("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")),
        "email": bool(_env("SMTP_USER", "SMTP_APP_PASSWORD", "NOTIFY_EMAIL_TO")),
        "teams": bool(_env("TEAMS_WEBHOOK_URL")),
    }


def split_text(text, limit=TELEGRAM_LIMIT):
    """Split on line boundaries so each chunk fits; a single over-long line is hard-cut."""
    chunks, cur = [], ""
    for line in text.splitlines(keepends=True):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) > limit:
            chunks.append(cur)
            cur = ""
        cur += line
    if cur:
        chunks.append(cur)
    return chunks or [""]


def send_telegram(subject, body):
    token, chat = _env("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
    text = f"{subject}\n\n{body}" if subject else body
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chunk in split_text(text):
        # plain text (no parse_mode): Markdown escaping errors would drop the whole message
        r = requests.post(url, json={"chat_id": chat, "text": chunk, "disable_web_page_preview": True},
                          timeout=TIMEOUT)
        if r.status_code != 200 or not r.json().get("ok"):
            raise RuntimeError(f"Telegram HTTP {r.status_code}: {r.text[:200]}")


def md_to_html(body):
    """Minimal rendering: keep the plain-text layout (tables/alignment) in a monospace block."""
    return ("<div style=\"font-family:Consolas,Menlo,monospace;white-space:pre-wrap;font-size:14px\">"
            f"{html.escape(body)}</div>")


def send_email(subject, body):
    user, pw, to = _env("SMTP_USER", "SMTP_APP_PASSWORD", "NOTIFY_EMAIL_TO")
    rcpts = [a.strip() for a in to.split(",") if a.strip()]
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "465"))
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject or "通知"
    msg["From"] = user
    msg["To"] = ", ".join(rcpts)
    msg.attach(MIMEText(body, "plain", "utf-8"))
    msg.attach(MIMEText(md_to_html(body), "html", "utf-8"))
    ctx = ssl.create_default_context()
    if port == 465:
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=TIMEOUT) as s:
            s.login(user, pw.replace(" ", ""))  # Gmail shows app passwords with spaces
            s.sendmail(user, rcpts, msg.as_string())
    else:
        with smtplib.SMTP(host, port, timeout=TIMEOUT) as s:
            s.starttls(context=ctx)
            s.login(user, pw.replace(" ", ""))
            s.sendmail(user, rcpts, msg.as_string())


def send_teams(subject, body):
    (url,) = _env("TEAMS_WEBHOOK_URL")
    blocks = []
    if subject:
        blocks.append({"type": "TextBlock", "text": subject, "weight": "Bolder", "size": "Medium", "wrap": True})
    blocks.append({"type": "TextBlock", "text": body, "wrap": True, "fontType": "Monospace"})
    payload = {"type": "message", "attachments": [{
        "contentType": "application/vnd.microsoft.card.adaptive",
        "content": {"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard", "version": "1.4", "body": blocks}}]}
    r = requests.post(url, json=payload, timeout=TIMEOUT)
    if r.status_code not in (200, 202):
        raise RuntimeError(f"Teams HTTP {r.status_code}: {r.text[:200]}")


SENDERS = {"telegram": send_telegram, "email": send_email, "teams": send_teams}


def notify(subject, body, channels=None):
    """Send to every configured channel. Returns {channel: "ok" | "skipped" | "error: ..."}."""
    cfg = configured()
    result = {}
    for name, fn in SENDERS.items():
        if channels and name not in channels:
            continue
        if not cfg[name]:
            result[name] = "skipped"
            continue
        try:
            fn(subject, body)
            result[name] = "ok"
        except Exception as exc:  # one channel failing must not block the others
            result[name] = f"error: {str(exc)[:200]}"
    return result


def main(argv=None):
    ap = argparse.ArgumentParser(description="Send a notification to Telegram / Email / Teams")
    ap.add_argument("--subject", default="")
    ap.add_argument("--file", help="message body file (UTF-8)")
    ap.add_argument("--text", help="message body text")
    ap.add_argument("--channels", help="comma-separated subset, e.g. telegram,email")
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args(argv)
    load_env()
    if a.status:
        for k, v in configured().items():
            print(f"{k}: {'configured' if v else 'not configured'}")
        return 0
    if a.test:
        subject, body = "股票推薦系統：測試通知", "如果你收到這則訊息，代表這個通知管道設定成功。"
    else:
        subject = a.subject
        if a.file:
            with open(a.file, encoding="utf-8") as f:
                body = f.read()
        else:
            body = a.text or ""
        if not body.strip():
            print("empty body; nothing sent")
            return 1
    chans = [c.strip() for c in a.channels.split(",")] if a.channels else None
    res = notify(subject, body, chans)
    for k, v in res.items():
        print(f"{k}: {v}")
    sent = [v for v in res.values() if v == "ok"]
    failed = [v for v in res.values() if v.startswith("error")]
    if failed or not sent:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
