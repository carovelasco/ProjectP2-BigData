"""
=============================================================================
Project Structure Initializer
Project: Online Casino Big Data Pipeline
=============================================================================
Run this ONCE at the start to create the full project folder structure.
=============================================================================
"""

from pathlib import Path
import json
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent

STRUCTURE = {
    "data/raw/casino_games/input":  "Place downloaded Kaggle CSV files here",
    "data/raw/logs":                "Ingestion logs and run reports",
    "data/processed":               "Spark-cleaned and transformed data (Phase 4)",
    "data/curated":                 "Final analytical datasets (Phase 6)",
    "ingestion":                    "Phase 3 scripts",
    "processing":                   "Phase 4 PySpark scripts",
    "streaming":                    "Phase 5 streaming simulation",
    "analytics":                    "Phase 6 analytics and ML",
    "docs":                         "Architecture diagrams and reports",
    "notebooks":                    "Jupyter notebooks",
}

def init_structure():
    print("Initializing project folder structure...\n")
    for folder, description in STRUCTURE.items():
        path = BASE_DIR / folder
        path.mkdir(parents=True, exist_ok=True)
        # Write a .gitkeep with description
        gitkeep = path / ".gitkeep"
        if not gitkeep.exists():
            with open(gitkeep, "w") as f:
                f.write(f"# {description}\n")
        print(f"  ✓  {folder}/")

    # Write architecture manifest
    manifest = {
        "project":      "Online Casino Big Data Pipeline",
        "created_at":   datetime.utcnow().isoformat(),
        "layers": {
            "raw":       "data/raw/       — Original, unmodified source data",
            "processed": "data/processed/ — Cleaned and transformed (PySpark)",
            "curated":   "data/curated/   — Ready for analytics and dashboards"
        },
        "phases": {
            "1": "Problem Definition",
            "2": "Dataset Selection",
            "3": "Data Ingestion      → ingestion/",
            "4": "Batch Processing    → processing/",
            "5": "Streaming           → streaming/",
            "6": "Analytics & Viz     → analytics/",
            "7": "AI Component        → analytics/ml/"
        }
    }

    manifest_path = BASE_DIR / "docs" / "architecture_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  ✓  docs/architecture_manifest.json")

    print("\nStructure initialized successfully.")
    print(f"Base directory: {BASE_DIR}")
    print("\nNEXT STEP:")
    print(f"  → Place your Kaggle CSV in:  {BASE_DIR}/data/raw/casino_games/input/")
    print(f"  → Then run: python ingestion/ingest_raw_data.py")

if __name__ == "__main__":
    init_structure()
