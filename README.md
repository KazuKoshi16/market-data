# market-data

ポートフォリオ管理アプリ用の市場データ自動取得リポジトリ。

## 出力ファイル

| ファイル | 内容 | 頻度 | ソース |
|---|---|---|---|
| `data/fx_daily.csv` | USD_JPY・EUR_JPY | 日次 | Frankfurter API |
| `data/jgb_daily.csv` | JGB 2Y/5Y/10Y/20Y/30Y | 日次 | 財務省 |
| `data/rates_monthly.csv` | US10Y・DE10Y | 月次 | OECD |

## 自動実行

毎週火曜日 JST 10:00 に自動更新。  
手動実行: Actions タブ → "Update Market Data" → "Run workflow"

## データURL（GitHub Pages）

```
https://kazukoshi16.github.io/market-data/data/fx_daily.csv
https://kazukoshi16.github.io/market-data/data/jgb_daily.csv
https://kazukoshi16.github.io/market-data/data/rates_monthly.csv
```

## Phase 1.5 アプリからの読み込み方

```javascript
const FX_URL    = 'https://kazukoshi16.github.io/market-data/data/fx_daily.csv';
const JGB_URL   = 'https://kazukoshi16.github.io/market-data/data/jgb_daily.csv';
const RATES_URL = 'https://kazukoshi16.github.io/market-data/data/rates_monthly.csv';
```
