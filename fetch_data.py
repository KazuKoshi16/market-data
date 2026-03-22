"""
Market Data Fetcher for GitHub Actions
=======================================
出力ファイル：
  data/fx_daily.csv       : USD_JPY・EUR_JPY（日次、Frankfurter API）
  data/jgb_daily.csv      : JGB 2Y/5Y/10Y/20Y/30Y（日次、財務省）
  data/rates_monthly.csv  : US10Y・DE10Y（月次、OECD）
"""

import requests, csv, re, os
from datetime import date, datetime, timedelta
from pathlib import Path

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)
HEADERS = {"User-Agent": "Mozilla/5.0 (market-data-fetcher/1.0)"}

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

# ── 1. 為替（Frankfurter）──────────────────────────────────

def fetch_fx():
    print("=== fx_daily.csv ===")
    path = OUTPUT_DIR / "fx_daily.csv"
    existing = load_rows(path)
    existing_dates = {r["date"] for r in existing}

    from_date = "1999-01-01"
    if existing_dates:
        last = max(existing_dates)
        from_date = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    to_date = date.today().isoformat()
    if from_date > to_date:
        print(f"  最新データ取得済み")
        return

    print(f"  期間: {from_date} 〜 {to_date}")
    new_rows = {}

    cur = datetime.strptime(from_date, "%Y-%m-%d")
    end = datetime.strptime(to_date, "%Y-%m-%d")
    while cur <= end:
        chunk_end = min(datetime(cur.year + 1, cur.month, cur.day) - timedelta(days=1), end)
        cf, ct = cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        for col, base, sym in [("USD_JPY","USD","JPY"),("EUR_JPY","EUR","JPY")]:
            try:
                url = f"https://api.frankfurter.dev/v1/{cf}..{ct}?base={base}&symbols={sym}"
                r = requests.get(url, headers=HEADERS, timeout=30)
                r.raise_for_status()
                for d, rates in r.json().get("rates", {}).items():
                    v = rates.get(sym)
                    if v:
                        if d not in new_rows: new_rows[d] = {}
                        new_rows[d][col] = round(v, 2)
                print(f"  [{col}] {cf}..{ct} → {len(r.json().get('rates',{}))}件")
            except Exception as e:
                print(f"  [ERROR] {col} {cf}: {e}")
        cur = chunk_end + timedelta(days=1)

    merged = {r["date"]: r for r in existing}
    for d, vals in new_rows.items():
        if d not in merged: merged[d] = {"date": d, "USD_JPY": "", "EUR_JPY": ""}
        merged[d].update({k: str(v) for k, v in vals.items()})

    rows = sorted(merged.values(), key=lambda r: r["date"])
    save_rows(path, ["date","USD_JPY","EUR_JPY"], rows)
    print(f"  → {len(rows)}行保存")

# ── 2. JGB（財務省）──────────────────────────────────────────

MOF_COLS = {"JGB2Y": 2, "JGB5Y": 5, "JGB10Y": 10, "JGB20Y": 12, "JGB30Y": 14}

def wareki_to_iso(s):
    m = re.match(r"^([STHMR])(\d+)\.(\d+)\.(\d+)$", s.strip())
    if not m: return None
    base = {"M":1868,"T":1912,"S":1926,"H":1989,"R":2019}[m.group(1)]
    return f"{base+int(m.group(2))-1}-{m.group(3).zfill(2)}-{m.group(4).zfill(2)}"

def fetch_jgb():
    print("=== jgb_daily.csv ===")
    path = OUTPUT_DIR / "jgb_daily.csv"
    existing = load_rows(path)
    existing_dates = {r["date"] for r in existing}
    last_date = max(existing_dates) if existing_dates else None

    urls = [
        ("全期間", "https://www.mof.go.jp/jgbs/reference/interest_rate/data/jgbcm_all.csv"),
        ("最新",   "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"),
    ]

    new_data = {}
    for label, url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            r.raise_for_status()
            text = r.content.decode("shift-jis", errors="replace")
            header_found = False
            count = 0
            for line in text.splitlines():
                cols = [c.strip() for c in line.split(",")]
                if not header_found:
                    if "10N" in cols: header_found = True
                    continue
                d = wareki_to_iso(cols[0])
                if not d: continue
                if label == "最新" and last_date and d <= last_date: continue
                if d not in new_data: new_data[d] = {}
                for col, idx in MOF_COLS.items():
                    if idx < len(cols):
                        v = cols[idx]
                        if v and v not in ("-","－","ND"):
                            try: new_data[d][col] = round(float(v), 3)
                            except: pass
                count += 1
            print(f"  [財務省/{label}] {count}件")
        except Exception as e:
            print(f"  [ERROR] 財務省/{label}: {e}")

    cols_all = list(MOF_COLS.keys())
    merged = {r["date"]: r for r in existing}
    for d, vals in new_data.items():
        if d not in merged: merged[d] = {"date": d, **{c: "" for c in cols_all}}
        for col, val in vals.items(): merged[d][col] = str(val)

    rows = sorted(merged.values(), key=lambda r: r["date"])
    save_rows(path, ["date"] + cols_all, rows)
    print(f"  → {len(rows)}行保存")

# ── 3. 金利月次（OECD: US10Y・DE10Y）────────────────────────

def fetch_rates_monthly():
    print("=== rates_monthly.csv ===")
    path = OUTPUT_DIR / "rates_monthly.csv"
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
            new_rows[d] = {"date": d, "US10Y": str(vals.get("USA","")), "DE10Y": str(vals.get("DEU",""))}

        merged = {r["date"]: r for r in existing}
        merged.update(new_rows)
        rows = sorted(merged.values(), key=lambda r: r["date"])
        save_rows(path, ["date","US10Y","DE10Y"], rows)
        print(f"  → {len(rows)}行保存")
    except Exception as e:
        print(f"  [ERROR] OECD: {e}")

# ── メイン ──────────────────────────────────────────────────

if __name__ == "__main__":
    fetch_fx()
    fetch_jgb()
    fetch_rates_monthly()
    print("\n=== 全処理完了 ===")
