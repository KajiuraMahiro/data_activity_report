# -*- coding: utf-8 -*-\

import os\
import sys\
import json\
import traceback\
from datetime import datetime\
\
import pandas as pd\
import numpy as np\
\
# =========================\
# Default Config\
# =========================\
DEFAULT_CONFIG = \{\
    "lookback_days": 0,            # <=0 means all period\
    "always_on_threshold": 0.95,\
    "input_csv": "input/transaction_log.csv",\
    "excel_output": "output/activity_report.xlsx",\
    "device_daily_csv": "output/device_daily.csv",\
    "device_summary_csv": "output/device_summary.csv",\
    "log_path": "output/logs/data_activity_report.log",\
\}\
\
# =========================\
# Utilities\
# =========================\
\
def ensure_dirs(*paths):\
    for p in paths:\
        d = os.path.dirname(p)\
        if d and not os.path.exists(d):\
            os.makedirs(d, exist_ok=True)\
\
\
def log(msg, log_file=None):\
    ts = datetime.now().isoformat(timespec="seconds")\
    line = f"[data_activity_report] \{ts\} \{msg\}"\
    print(line, flush=True)\
    if log_file:\
        with open(log_file, "a", encoding="utf-8") as f:\
            f.write(line + "\\n")\
\
\
def load_config(cfg_path, base_cfg):\
    if os.path.exists(cfg_path):\
        try:\
            with open(cfg_path, "r", encoding="utf-8") as f:\
                user_cfg = json.load(f)\
            base_cfg.update(user_cfg)\
        except Exception:\
            pass\
    return base_cfg\
\
\
def normalize_columns(cols):\
    if len(cols) == 1 and ("\\t" in cols[0] or "," in cols[0]):\
        raw = cols[0]\
        if "\\t" in raw:\
            return [c.strip() for c in raw.split("\\t")]\
        else:\
            return [c.strip() for c in raw.split(",")]\
    return [c.strip() for c in cols]\
\
\
def read_tx_csv(path):\
    # ヘッダ1行で区切り推定\
    with open(path, "r", encoding="utf-8") as f:\
        first_line = f.readline().rstrip("\\n\\r")\
    sep = "\\t" if "\\t" in first_line else ("," if "," in first_line else None)\
\
    df = pd.read_csv(path, sep=sep, dtype=str, encoding="utf-8", engine="python")\
    df.columns = normalize_columns(list(df.columns))\
\
    required = [\
        "date", "device_id", "store_id",\
        "device_class", "store_name", "total_value", "event_count"\
    ]\
    missing = [k for k in required if k not in df.columns]\
    if missing:\
        raise ValueError(f"Missing CSV headers: \{missing\} / Found: \{df.columns.tolist()\}")\
\
    # 正規化
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date\
    for col in ["event_count", "total_value"]:\
        df[col] = pd.to_numeric(df[col], errors="coerce")\
    df["store_id"] = df["store_id"].astype(str)\
    df["device_id"] = df["device_id"].astype(str)\
\
    return df\
\
\
def apply_lookback(df, days):\
    if days and days > 0:\
        max_day = df["date"].max()\
        min_keep = pd.to_datetime(max_day) - pd.Timedelta(days=days - 1)\
        mask = pd.to_datetime(df["date"]) >= min_keep\
        return df.loc[mask].copy()\
    return df\
\
\
def build_store_day_status(df_tx):\
    g = (\
        df_tx.groupby(["date", "store_id"], as_index=False)\
             .agg(rows=("store_id", "size"),\
                  any_event=("event_count", lambda s: (s.fillna(0) > 0).any()))\
    )\
    # groupbyにより行数は常に0より大きくなる
    g["has_record"] = g["rows"] > 0\
    out = g[["date", "store_id", "has_record", "any_event"]].copy()\
    return out\
\
\
def build_device_daily(df_tx):\
    daily = (\
        df_tx.groupby(["date", "store_id", "device_id"], as_index=False)\
             .agg(\
                 event_count=("event_count", "sum"),\
                 total_value=("total_value", "sum"),\
             )\
    )\
    return daily\
\
\
def build_device_summary(df_device_daily, df_store_day_status, always_on_threshold=0.95):\
    base = (\
        df_device_daily.merge(\
            df_store_day_status[["date", "store_id", "has_record"]],\
            on=["date", "store_id"],\
            how="right"\
        )\
    )\
    base["event_count"] = pd.to_numeric(base["event_count"], errors="coerce").fillna(0)\
    base["has_record"] = base["has_record"].astype(bool)\
    base["is_active"] = (base["has_record"] == True) & (base["event_count"] > 0)\
\
    grp = base.groupby(["store_id", "device_id"], dropna=False, as_index=False).agg(\
        active_days=("is_active", "sum"),\
        business_days=("has_record", "sum"),\
        first_seen_date=("date", "min"),\
        last_seen_date=("date", "max"),\
    )\
    grp = grp[grp["device_id"].notna()].copy()\
    grp["device_id"] = grp["device_id"].astype(str)\
\
    grp["activity_rate"] = np.where(\
        grp["business_days"] > 0,\
        grp["active_days"] / grp["business_days"],\
        np.nan\
    )\
\
    def max_consecutive_stops(sub):\
        sub = sub.sort_values("date")\
        stopped = (sub["has_record"] == True) & (~sub["is_active"])\
        max_run = 0\
        run = 0\
        for v in stopped.values:\
            if v:\
                run += 1\
            else:\
                max_run = max(max_run, run)\
                run = 0\
        max_run = max(max_run, run)\
        return max_run\
\
    base_for_run = base.loc[base["device_id"].notna(), ["store_id", "device_id", "date", "has_record", "is_active"]].copy()\
    max_stop = (\
        base_for_run.groupby(["store_id", "device_id"]).apply(max_consecutive_stops).reset_index(name="max_consecutive_stops")\
    )\
\
    out = grp.merge(max_stop, on=["store_id", "device_id"], how="left")\
    out["is_always_on"] = (out["activity_rate"] >= always_on_threshold)\
\
    out = out[\
        [\
            "device_id", "store_id", "active_days", "business_days",\
            "activity_rate", "max_consecutive_stops", "is_always_on",\
            "first_seen_date", "last_seen_date",\
        ]\
    ].sort_values(["store_id", "device_id"]).reset_index(drop=True)\
\
    return out\
\
\
def write_excel(excel_path, df_device_daily, df_device_summary):\
    ensure_dirs(excel_path)\
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:\
        df_device_daily.to_excel(writer, sheet_name="DeviceDaily", index=False)\
        df_device_summary.to_excel(writer, sheet_name="DeviceSummary", index=False)\
\
\
def main():\
    # # 実行ファイルの場所をベースディレクトリとして設定
    if getattr(sys, 'frozen', False):\
        base_dir = os.path.dirname(sys.executable)\
    else:\
        base_dir = os.path.dirname(os.path.abspath(__file__))\
\
    cfg_path = os.path.join(base_dir, "config.json")\
    cfg = load_config(cfg_path, DEFAULT_CONFIG.copy())\
\
    input_csv = os.path.join(base_dir, cfg["input_csv"])\
    excel_out = os.path.join(base_dir, cfg["excel_output"])\
    daily_csv = os.path.join(base_dir, cfg["device_daily_csv"])\
    summary_csv = os.path.join(base_dir, cfg["device_summary_csv"])\
    log_path = os.path.join(base_dir, cfg["log_path"])\
\
    ensure_dirs(excel_out, daily_csv, summary_csv, log_path)\
\
    log(f"start: \{datetime.now().isoformat(timespec='seconds')\}", log_path)\
    log(f"script_dir: \{base_dir\}", log_path)\
    log(f"input_csv: \{input_csv\}", log_path)\
    log(f"output_dir: \{os.path.dirname(excel_out)\}", log_path)\
\
    try:\
        # load
        df_raw = read_tx_csv(input_csv)\
        log(f"input_rows: \{len(df_raw)\}", log_path)\
\
        # lookback
        lookback = int(cfg.get("lookback_days", 0) or 0)\
        if lookback > 0:\
            before = len(df_raw)\
            df_raw = apply_lookback(df_raw, lookback)\
            log(f"lookback_days=\{lookback\}: \{before\} -> \{len(df_raw)\} rows", log_path)\
\
        # intermediates
        df_device_daily = build_device_daily(df_raw)\
        df_store_day_status = build_store_day_status(df_raw)\
\
        # summary
        always_on_threshold = float(cfg.get("always_on_threshold", 0.95))\
        df_device_summary = build_device_summary(\
            df_device_daily,\
            df_store_day_status,\
            always_on_threshold=always_on_threshold,\
        )\
\
        # outputs
        write_excel(excel_out, df_device_daily, df_device_summary)\
        df_device_daily.to_csv(daily_csv, index=False, encoding="utf-8-sig")\
        df_device_summary.to_csv(summary_csv, index=False, encoding="utf-8-sig")\
\
        log(f"excel_written: \{excel_out\}", log_path)\
        log(f"csv_written: \{daily_csv\}", log_path)\
        log(f"csv_written: \{summary_csv\}", log_path)\
        log("done", log_path)\
\
    except Exception as e:\
        msg = f"ERROR: \{e\}"\
        log(msg, log_path)\
        tb = traceback.format_exc()\
        log(tb, log_path)\
        print(msg)\
        print(tb)\
\
\
if __name__ == "__main__":\
    main()\
