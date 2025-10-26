import os
import json
import urllib.request
import urllib.parse

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
MESSAGE = os.environ.get("MESSAGE") or os.environ.get("DEFAULT_MESSAGE") or "굿모닝! 오늘도 좋은 하루 되세요 ☀️"

def send_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",  # <b>굵게</b>, <i>기울임</i> 등 가능
        "disable_web_page_preview": True,
    }
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=encoded, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
        js = json.loads(body.decode("utf-8"))
        if not js.get("ok"):
            raise RuntimeError(f"Telegram API error: {js}")

if __name__ == "__main__":
    send_message(MESSAGE)
