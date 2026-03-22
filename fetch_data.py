
"""
Market Data Fetcher for GitHub Actions
=======================================
出力ファイル：
  data/daily.csv    : 全日次データ統合（FX + JGB）
  data/monthly.csv  : 月次データ（US10Y・DE10Y）
"""

import requests, csv, re
from datetime import date, datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0 (market-data-fetcher/1.0)"}
MOF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Referer": "https://www.mof.go.jp/jgbs/reference/interest_rate/index.htm",
}

DAILY_COLS   = ["date", "USD_JPY", "EUR_JPY", "JGB2Y", "JGB5Y", "JGB10Y", "JGB20Y", "JGB30Y"]
MONTHLY_COLS = ["date", "US10Y", "DE10Y"]

# ── ユーティリティ ──────────────────────────────────────────

def month_end(year, month):
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    return d.isoformat()

def load_rows(path):
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))

def save_rows(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

def merge_into(merged, date_key, updates):
    """mergedに新しいデータをマージ（既存行を上書き）"""
    if date_key not in merged:
        merged[date_key] = {"date": date_key}
    for k, v in updates.items():
        merged[date_key][k] = str(v)

def fill_cols(row, cols):
    """行に不足している列を空文字で補完"""
    for c in cols:
        if c not in row:
            row[c] = ""
    return row

# ── FX取得（Frankfurter）──────────────────────────────────

def fetch_fx(merged):
    print("--- FX (Frankfurter) ---")
    existing_dates = set(merged.keys())
    from_date = "1999-01-01"
    if existing_dates:
        last = max(d for d in existing_dates if merged[d].get("USD_JPY",""))
        if last:
            from_date = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    to_date = date.today().isoformat()
    if from_date > to_date:
        print("  最新データ取得済み")
        return

    print(f"  期間: {from_date} 〜 {to_date}")
    cur = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")

    while cur <= end:
        try:
            chunk_end = min(datetime(cur.year + 1, cur.month, cur.day) - timedelta(days=1), end)
        except ValueError:
            chunk_end = min(datetime(cur.year + 1, 1, 1) - timedelta(days=1), end)
        cf, ct = cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")

        for col, base, sym in [("USD_JPY","USD","JPY"),("EUR_JPY","EUR","JPY")]:
            try:
                url = f"https://api.frankfurter.dev/v1/{cf}..{ct}?base={base}&symbols={sym}"
                r = requests.get(url, headers=HEADERS, timeout=30)
                r.raise_for_status()
                data = r.json()
                for d, rates in data.get("rates", {}).items():
                    v = rates.get(sym)
                    if v:
                        merge_into(merged, d, {col: round(v, 2)})
                print(f"  [{col}] {cf}..{ct} → {len(data.get('rates',{}))}件")
            except Exception as e:
                print(f"  [ERROR] {col}: {e}")
        cur = chunk_end + timedelta(days=1)

# ── JGB取得（財務省）──────────────────────────────────────

MOF_COL_MAP = {"JGB2Y": 2, "JGB5Y": 5, "JGB10Y": 10, "JGB20Y": 12, "JGB30Y": 14}

def wareki_to_iso(s):
    m = re.match(r"^([STHMR])(\d+)\.(\d+)\.(\d+)$", s.strip())
    if not m: return None
    base = {"M":1868,"T":1912,"S":1926,"H":1989,"R":2019}[m.group(1)]
    return f"{base+int(m.group(2))-1}-{m.group(3).zfill(2)}-{m.group(4).zfill(2)}"

def fetch_jgb(merged):
    print("--- JGB (財務省) ---")
    existing_dates = set(merged.keys())
    last_jgb = None
    if existing_dates:
        jgb_dates = [d for d in existing_dates if merged[d].get("JGB10Y","")]
        if jgb_dates:
            last_jgb = max(jgb_dates)

    urls = [
        ("全期間", "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"),
        ("最新",   "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"),
    ]

    for label, url in urls:
        try:
            print(f"  [{label}] 取得中...")
            r = requests.get(url, headers=MOF_HEADERS, timeout=60)
            r.raise_for_status()
            text = r.content.decode("shift-jis", errors="replace")
            header_found = False
            count = 0
            for line in text.splitlines():
                cols = [c.strip() for c in line.split(",")]
                if not header_found:
                    if "10年" in cols or "10N" in cols:
                        header_found = True
                    continue
                d = wareki_to_iso(cols[0])
                if not d: continue
                if label == "最新" and last_jgb and d <= last_jgb: continue
                vals = {}
                for col, idx in MOF_COL_MAP.items():
                    if idx < len(cols):
                        v = cols[idx]
                        if v and v not in ("-","－","ND"):
                            try: vals[col] = round(float(v), 3)
                            except: pass
                if vals:
                    merge_into(merged, d, vals)
                    count += 1
            print(f"  [{label}] {count}件パース")
        except Exception as e:
            print(f"  [ERROR] {label}: {e}")

# ── 月次取得（OECD）──────────────────────────────────────

def fetch_monthly():
    print("--- monthly.csv (OECD) ---")
    path = OUTPUT_DIR / "monthly.csv"
    existing = load_rows(path)

    url = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.STES,DSD_STES@DF_FINMARK/"
        "USA+DEU.M.IRLT.PA._Z._Z._Z._Z.N"
        "?format=csvfilewithlabels&detail=dataonly"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        r.raise_for_status()
        lines = r.text.splitlines()
        hdr = lines[0].split(",")
        ref_idx    = hdr.index("REF_AREA")
        period_idx = hdr.index("TIME_PERIOD")
        val_idx    = hdr.index("OBS_VALUE")

        raw = {}
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) <= max(ref_idx, period_idx, val_idx): continue
            country = cols[ref_idx].strip()
            period  = cols[period_idx].strip()
            try: val = round(float(cols[val_idx]), 3)
            except: continue
            if period not in raw: raw[period] = {}
            raw[period][country] = val

        print(f"  [OECD] {len(raw)}ヶ月分")

        new_rows = {}
        for ym, vals in raw.items():
            y, m = map(int, ym.split("-"))
            d = month_end(y, m)
            new_rows[d] = {
                "date":  d,
                "US10Y": str(vals.get("USA", "")),
                "DE10Y": str(vals.get("DEU", "")),
            }

        merged_m = {r["date"]: r for r in existing}
        merged_m.update(new_rows)
        rows = sorted(merged_m.values(), key=lambda r: r["date"])
        save_rows(path, MONTHLY_COLS, rows)
        print(f"  → {len(rows)}行保存")
    except Exception as e:
        print(f"  [ERROR] OECD: {e}")

# ── メイン ──────────────────────────────────────────────────

if __name__ == "__main__":
    path = OUTPUT_DIR / "daily.csv"

    # 既存データをロード
    existing = load_rows(path)
    merged = {r["date"]: r for r in existing}

    # 各ソースからデータを取得してmergedに追加
    fetch_fx(merged)
    fetch_jgb(merged)

    # 全行を保存（列を補完してソート）
    rows = sorted(
        [fill_cols(r, DAILY_COLS) for r in merged.values()],
        key=lambda r: r["date"]
    )
    # 不要な列を除外
    rows = [{c: r.get(c,"") for c in DAILY_COLS} for r in rows]
    save_rows(path, DAILY_COLS, rows)
    print(f"\n=== daily.csv: {len(rows)}行保存 ===")

    # 月次
    fetch_monthly()
    print("\n=== 全処理完了 ===")
