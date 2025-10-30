# data_activity_report

ポートフォリオ用のサンプルプロジェクトです。  
日次ログデータからデバイスの稼働状況を集計し、  
稼働日数・営業推定日数・稼働率・最大連続停止日数などを算出します。

---

##概要

**data_activity_report.py** は、日別のイベントログ（CSV/TSV形式）を読み込み、  
デバイス単位の稼働状況をまとめた Excel / CSV レポートを出力するツールです。

**特徴：**
- CSV / TSV 自動判定
- lookback_days による期間抽出
- 稼働率と最大連続停止日数の計算
- Excel（2シート構成）とCSV出力
- ログ出力対応

---

##ディレクトリ構成例

```
data_activity_report/
├─ data_activity_report.py
├─ README.md
├─ input/
│  └─ transaction_log.csv  # ダミーデータ
└─ output/
   ├─ activity_report.xlsx
   ├─ device_daily.csv
   ├─ device_summary.csv
   └─ logs/
```

---

##使い方
```bash
1️⃣ 依存ライブラリのインストール
pip install pandas numpy openpyxl

2️⃣ スクリプト実行
コードをコピーする
python data_activity_report.py

3️⃣ 出力ファイル
Excel: output/activity_report.xlsx（シート名：DeviceDaily, DeviceSummary）

CSV:

output/device_daily.csv

output/device_summary.csv

ログ: output/logs/data_activity_report.log

```

設定ファイル（任意）
同じフォルダに 以下形式のconfig.json を置くと、設定を上書きできます。

```
{
  "lookback_days": 30,
  "always_on_threshold": 0.95,
  "input_csv": "input/transaction_log.csv",
  "excel_output": "output/activity_report.xlsx",
  "device_daily_csv": "output/device_daily.csv",
  "device_summary_csv": "output/device_summary.csv",
  "log_path": "output/logs/data_activity_report.log"
}
```

出力項目の説明
```
カラム名	意味
device_id	デバイスID
store_id	店舗ID
active_days	稼働日数
business_days	営業推定日数
activity_rate	稼働率
max_consecutive_stops	最大連続停止日数
is_always_on	常時稼働フラグ（稼働率が閾値以上）
first_seen_date	初回観測日
last_seen_date	最終観測日
```

使用技術
Python 3.x
pandas / numpy / openpyxl

ライセンス
MIT License
© 2025 Mahiro Kajiura
