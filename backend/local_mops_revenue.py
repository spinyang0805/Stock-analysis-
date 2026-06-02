#!/usr/bin/env python3
"""
local_mops_revenue.py — Playwright 爬 MOPS 月營收，逐支股票抓取
================================================================
使用方式:
  cd backend
  python local_mops_revenue.py            # 全部股票
  python local_mops_revenue.py --limit 50 # 只跑前 50 支測試
  python local_mops_revenue.py --start 2330 # 從指定股票續跑
================================================================
"""
import argparse, asyncio, os, sys, time
from datetime import datetime

def _load_dotenv():
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_file):
        return
    with open(env_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

_load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("❌ DATABASE_URL 未設定")
    sys.exit(1)

try:
    from firebase import db
    from firebase_cache import _run, get_all_products_from_db
except Exception as e:
    print(f"❌ 匯入 firebase 失敗: {e}")
    sys.exit(1)

if db is None:
    print("❌ 資料庫連線失敗")
    sys.exit(1)


def _parse_revenue_table(text: str) -> dict:
    """Parse table inner_text (tab-separated) → revenue dict."""
    result = {}
    yoy_count = 0
    for line in text.splitlines():
        if "\t" not in line:
            continue
        parts = line.split("\t", 1)
        key = parts[0].strip()
        val = parts[1].replace(",", "").strip() if len(parts) > 1 else ""
        if not val:
            continue
        try:
            if "本月" in key:
                result["revenue"] = int(float(val))
            elif "增減百分比" in key:
                yoy_count += 1
                if yoy_count == 1:  # first 增減百分比 = MoM or YoY vs last year
                    result["revenue_yoy"] = float(val)
        except Exception:
            pass
    return result


def _save_revenue(code: str, data: dict, revenue_date: str):
    sql = """
        INSERT INTO fundamentals
            (stock_id, pe_ratio, pb_ratio, dividend_yield, eps,
             revenue, revenue_mom, revenue_yoy, revenue_date, source, updated_at)
        VALUES (%s, NULL, NULL, NULL, NULL, %s, NULL, %s, %s, 'mops_playwright', NOW())
        ON CONFLICT (stock_id) DO UPDATE SET
            revenue      = EXCLUDED.revenue,
            revenue_yoy  = EXCLUDED.revenue_yoy,
            revenue_date = EXCLUDED.revenue_date,
            source       = EXCLUDED.source,
            updated_at   = NOW()
    """
    _, err = _run(sql, (code, data.get("revenue"), data.get("revenue_yoy"), revenue_date))
    return err


async def scrape_all(codes: list, sleep_sec: float = 1.5):
    from playwright.async_api import async_playwright

    now = datetime.now()
    revenue_date = f"{now.year}-{now.month:02d}"
    written = skipped = errors = 0

    print(f"\n[月營收] 開始爬取 {len(codes)} 支股票，revenue_date={revenue_date}")
    print(f"  {'#':<5} {'代號':<7} {'月營收':>15} {'YoY%':>8}  狀態")
    print("  " + "-" * 55)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            locale="zh-TW",
            viewport={"width": 1280, "height": 900},
        )
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        page = await ctx.new_page()

        # Load page once
        await page.goto("https://mops.twse.com.tw/mops/#/web/t05st10_ifrs",
                        timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(1500)

        for i, code in enumerate(codes, 1):
            try:
                # Go back to search if needed
                modify_btn = page.locator("button:has-text('修改條件')")
                if await modify_btn.count() > 0 and await modify_btn.first.is_visible():
                    await modify_btn.first.click()
                    await page.wait_for_timeout(500)

                # Clear and fill company ID
                await page.fill("#companyId", "")
                await page.fill("#companyId", code)
                await page.wait_for_timeout(800)

                # Click autocomplete suggestion
                suggestion = page.locator(f"button:has-text('{code}')")
                if await suggestion.count() > 0:
                    await suggestion.first.click()
                    await page.wait_for_timeout(400)
                else:
                    # No autocomplete (company not found or delisted)
                    skipped += 1
                    print(f"  {i:<5} {code:<7} {'':>15} {'':>8}  ⏭ 找不到")
                    continue

                # Click 查詢
                await page.click("button:has-text('查詢')")
                await page.wait_for_timeout(3000)

                # Read table
                table = await page.query_selector("table")
                if not table:
                    skipped += 1
                    print(f"  {i:<5} {code:<7} {'':>15} {'':>8}  ⏭ 無資料")
                    continue

                text = await table.inner_text()
                data = _parse_revenue_table(text)

                if not data.get("revenue"):
                    skipped += 1
                    print(f"  {i:<5} {code:<7} {'':>15} {'':>8}  ⏭ 解析失敗")
                    continue

                err = _save_revenue(code, data, revenue_date)
                if err:
                    errors += 1
                    print(f"  {i:<5} {code:<7} {data['revenue']:>15,} {data.get('revenue_yoy',0):>7.1f}%  ❌ DB錯誤", flush=True)
                else:
                    written += 1
                    print(f"  {i:<5} {code:<7} {data['revenue']:>15,} {data.get('revenue_yoy',0):>7.1f}%  ✅", flush=True)

                await asyncio.sleep(sleep_sec)

            except Exception as e:
                errors += 1
                print(f"  {i:<5} {code:<7} {'':>15} {'':>8}  ❌ {str(e)[:40]}", flush=True)
                # Reload page on error
                try:
                    await page.goto("https://mops.twse.com.tw/mops/#/web/t05st10_ifrs",
                                    timeout=20000, wait_until="networkidle")
                    await page.wait_for_timeout(1000)
                except Exception:
                    pass

        await browser.close()

    print(f"\n{'='*55}")
    print(f"  ✅ 寫入 {written} 筆  ⏭ 略過 {skipped} 筆  ❌ 錯誤 {errors} 筆")
    print(f"{'='*55}\n")
    return written


def main():
    parser = argparse.ArgumentParser(description="MOPS 月營收 Playwright 爬蟲")
    parser.add_argument("--limit",   type=int, default=0,  help="只跑前 N 支（測試用）")
    parser.add_argument("--start",   type=str, default="", help="從指定代號開始（斷點續傳）")
    parser.add_argument("--sleep",   type=float, default=1.5, help="每支間隔秒數（預設 1.5）")
    args = parser.parse_args()

    # Get stock list from DB
    products = get_all_products_from_db(limit=5000)
    if not products:
        print("❌ 無法取得股票清單")
        sys.exit(1)
    codes = [p["code"] for p in products if p.get("code")]

    # Start from
    if args.start:
        try:
            idx = codes.index(args.start)
            codes = codes[idx:]
            print(f"從 {args.start} 開始，剩 {len(codes)} 支")
        except ValueError:
            print(f"找不到 {args.start}，從頭開始")

    # Limit
    if args.limit:
        codes = codes[:args.limit]

    print(f"\n{'='*55}")
    print(f"  📊 MOPS 月營收爬蟲（Playwright）")
    print(f"  股票數: {len(codes)}  間隔: {args.sleep}s")
    print(f"  預計時間: {len(codes) * (args.sleep + 3.5) / 60:.0f} 分鐘")
    print(f"{'='*55}")

    asyncio.run(scrape_all(codes, sleep_sec=args.sleep))


if __name__ == "__main__":
    main()
