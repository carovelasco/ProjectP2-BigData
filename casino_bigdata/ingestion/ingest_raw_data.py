#Phase 3 - Data Ingestion

import os, json, hashlib, shutil, pandas as pd
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR  = BASE_DIR / "data" / "raw"
LOG_FILE = RAW_DIR / "ingestion_log.json"

def main():
    input_dir = RAW_DIR / "input"
    csv_files = list(input_dir.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in data/raw/input/")
        return

    entries = []
    for src in csv_files:
        dest = RAW_DIR / src.name
        shutil.copy2(src, dest)

        df = pd.read_csv(dest, low_memory=False)

        entry = {
            "file":         src.name,
            "raw_path":     str(dest),
            "size_mb":      round(os.path.getsize(dest) / 1024**2, 2),
            "md5":          hashlib.md5(open(dest, "rb").read()).hexdigest(),
            "rows":         len(df),
            "columns":      list(df.columns),
            "nulls":        {c: int(df[c].isna().sum()) for c in df.columns},
            "duplicates":   int(df.duplicated().sum()),
            "status":       "SUCCESS"
        }
        entries.append(entry)
        print(f" {src.name}")
        print(f"  Rows      : {len(df):,}")
        print(f"  Size      : {entry['size_mb']} MB")
        print(f"  Duplicates: {entry['duplicates']:,}")
        print(f"  Nulls     : {sum(entry['nulls'].values()):,}")

    json.dump(entries, open(LOG_FILE, "w"), indent=2)
    print(f"\nLog saved : {LOG_FILE}")

if __name__ == "__main__":
    main()