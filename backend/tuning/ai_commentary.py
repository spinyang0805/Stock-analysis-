"""ai_commentary.py — plain-language commentary on the daily report.

  --mode cli   (user's PC, subscriptions): claude -p, codex exec, gemini -p in parallel → claude merges
  --mode groq  (cloud fallback when the PC is off): one Groq call, labelled 備援版
Never automates chat web pages. The models only see the report text; they must not invent numbers.

  python backend/tuning/ai_commentary.py --mode cli  [--as-of YYYYMMDD] [--dir history/out/daily]
  python backend/tuning/ai_commentary.py --mode groq [--as-of YYYYMMDD]
Writes <dir>/ai_<as_of>.md (exit 0) or exits 1 when no model produced text.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

TIMEOUT = 300
GROQ_MODEL = "qwen/qwen3.8-27b"

INSTRUCTION = (
    "你是台股投資助理。以下是系統今天收盤後產生的「每日股票報告」（Markdown）。請用繁體中文（台灣用語）寫一段"
    "給一般投資人看的解讀，300～500 字：\n"
    "1. 今日買進候選的重點與共同特徵（用白話解釋訊號是什麼意思）\n"
    "2. 追蹤中與持股的提醒：哪些要注意或出場、原因\n"
    "3. 風險提醒（例如試行標示、大盤狀況、樣本數少）\n"
    "規則：只能使用報告裡出現的數字與事實，不可自行編造股價、新聞或目標價；報告若標示「試行」必須提醒；"
    "不要給保證獲利的說法；最後一行加「以上為 AI 解讀，非投資建議」。只輸出解讀內文。"
)
MERGE = (
    "以下是三個 AI 對同一份每日股票報告的解讀草稿（A/B/C），以及原始報告。請整合成一份最好的繁體中文解讀"
    "（300～500 字）：保留三者一致的重點；若彼此矛盾，以原始報告的數字為準並刪除錯誤內容；不可加入報告沒有的"
    "數字或事實；最後一行「以上為 AI 解讀（Claude × Codex × Gemini 整合），非投資建議」。只輸出內文。"
)


def _run(cmd, stdin_text, cwd=None):
    exe = shutil.which(cmd[0])
    if not exe:
        return None, f"{cmd[0]} not found"
    try:
        r = subprocess.run([exe] + cmd[1:], input=stdin_text, capture_output=True, text=True, encoding="utf-8",
                           timeout=TIMEOUT, cwd=cwd)
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except OSError as exc:
        return None, str(exc)
    out = (r.stdout or "").strip()
    return (out, None) if r.returncode == 0 and len(out) > 50 else (None, f"rc={r.returncode} {r.stderr[-200:]}")


def run_cli(report):
    tmp = tempfile.mkdtemp(prefix="ai_report_")            # neutral cwd: the models need no repo access
    jobs = {
        "claude": (["claude", "-p", INSTRUCTION], report),
        "codex": (["codex", "exec", "-s", "read-only", "--skip-git-repo-check", "-C", tmp, "-"],
                  INSTRUCTION + "\n\n---\n" + report),
        "gemini": (["gemini", "-p", INSTRUCTION], report),
    }
    with ThreadPoolExecutor(3) as ex:
        futs = {k: ex.submit(_run, c, s, tmp) for k, (c, s) in jobs.items()}
        drafts = {k: f.result() for k, f in futs.items()}
    ok = {k: v[0] for k, v in drafts.items() if v[0]}
    for k, (txt, err) in drafts.items():
        print(f"[ai] {k}: {'ok' if txt else 'failed ' + str(err)}")
    if not ok:
        return None, []
    if len(ok) == 1:
        return next(iter(ok.values())), list(ok)
    body = "\n\n".join(f"### 草稿 {chr(65 + i)}\n{t}" for i, t in enumerate(ok.values()))
    merged, err = _run(["claude", "-p", MERGE], body + "\n\n### 原始報告\n" + report, tmp)
    if not merged:
        print(f"[ai] merge failed ({err}); using the first draft")
        merged = next(iter(ok.values()))
    return merged, list(ok)


def run_groq(report):
    import requests
    key = os.environ.get("GROQ_API_KEY", "").strip()
    if not key:
        return None
    try:
        r = requests.post("https://api.groq.com/openai/v1/chat/completions", timeout=60,
                          headers={"Authorization": f"Bearer {key}"},
                          json={"model": GROQ_MODEL, "temperature": 0.3, "max_tokens": 1200,
                                "reasoning_effort": "none", "reasoning_format": "hidden",
                                "messages": [{"role": "system", "content": INSTRUCTION},
                                             {"role": "user", "content": report[:12000]}]})
        if r.status_code != 200:
            print(f"[ai] groq HTTP {r.status_code}: {r.text[:200]}")
            return None
        import re
        txt = r.json()["choices"][0]["message"]["content"]
        return re.sub(r"<think>.*?</think>", "", txt, flags=re.S).strip() or None
    except Exception as exc:
        print(f"[ai] groq failed: {exc}")
        return None


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["cli", "groq"], required=True)
    ap.add_argument("--dir", default=os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__)))), "history", "out", "daily"))
    ap.add_argument("--as-of")
    a = ap.parse_args(argv)
    d = a.as_of
    if not d:
        with open(os.path.join(a.dir, "latest.json"), encoding="utf-8") as f:
            d = json.load(f)["as_of"]
    with open(os.path.join(a.dir, f"report_{d}.md"), encoding="utf-8") as f:
        report = f.read()
    if a.mode == "cli":
        text, used = run_cli(report)
        head = f"## AI 解讀（{' × '.join(used)}）" if text else None
    else:
        text = run_groq(report)
        head = "## AI 解讀（備援版：電腦未開機，改由 Groq 產生）" if text else None
    if not text:
        print("[ai] no commentary produced")
        return 1
    out = os.path.join(a.dir, f"ai_{d}.md")
    with open(out + ".tmp", "w", encoding="utf-8") as f:
        f.write(f"{head}\n\n{text}\n")
    os.replace(out + ".tmp", out)
    print(f"[ai] wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
