import json
import re
from difflib import SequenceMatcher
from typing import Dict, Any

import pandas as pd

# ==========================================
# CONFIGURATION & CLEANING LOGIC
# ==========================================

STOP_WORDS = [
    r"\(Private\)",
    r"\(Standard\)",
    r"\*",
    r"Half Day",
    r"Full Day",
    r"Singletrack",
    r"Ebike",
    r"E-Bike",
    r"Private",
    r"Standard",
    r"Morning",
    r"Afternoon",
    r"Tour",
    r"3-Day",
    r"4-Day",
    r"5-Day",
    r"6-Day",
    r"Camp-based",
    r"Inn-based",
]

ARCTIC_PATH = "arctic_triptype.csv"
WEBSITE_PATH = "website_export.csv"
OUTPUT_JSON = "unified_tours.json"
SUMMARY_CSV = "unify_summary.csv"
FUZZY_THRESHOLD = 0.85


def normalize_name(name: str) -> str:
    if pd.isna(name):
        return ""
    clean = str(name)
    for pattern in STOP_WORDS:
        clean = re.sub(f"(?i){pattern}", "", clean)
    return re.sub(r"\s+", " ", clean).strip().lower()


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def serialize_arctic_row(row: pd.Series) -> Dict[str, Any]:
    record: Dict[str, Any] = {}
    for col in row.index:
        value = row.at[col]
        if pd.isna(value):
            record[str(col)] = None
        else:
            record[str(col)] = str(value)
    return record


def main():
    print("--- Starting Cluster & Unify ---")
    try:
        df_arctic = pd.read_csv(ARCTIC_PATH, dtype=str)
        df_web = pd.read_csv(WEBSITE_PATH, dtype=str)
    except Exception as exc:
        print(f"Error loading files: {exc}")
        return

    print(f"Loaded: {len(df_arctic)} Arctic Rows, {len(df_web)} Web Rows")

    arctic_clusters: Dict[str, list] = {}
    for _, row in df_arctic.iterrows():
        serialized = serialize_arctic_row(row)
        original_name = serialized.get("name", "Unknown")
        master_key = normalize_name(original_name)
        arctic_clusters.setdefault(master_key, []).append(serialized)

    print(f"Collapsed Arctic data into {len(arctic_clusters)} Unique Clusters.")

    final_tours = []
    matched_clusters = set()

    for _, web_row in df_web.iterrows():
        web_title = str(web_row.get("Title", ""))
        web_key = normalize_name(web_title)
        match_found = None

        if web_key in arctic_clusters:
            match_found = web_key
        else:
            best_score = 0
            best_candidate = None
            for cluster_key in arctic_clusters.keys():
                score = similarity(web_key, cluster_key)
                if score >= FUZZY_THRESHOLD and score > best_score:
                    best_score = score
                    best_candidate = cluster_key
            if best_candidate:
                match_found = best_candidate

        tour_record = {
            "Master_Name": web_title,
            "Website_ID": web_row.get("ID"),
            "Slug": web_row.get("Slug"),
            "Description_Short": web_row.get("short_description"),
            "Description_Long": web_row.get("Content"),
            "Arctic_Variants": [],
        }

        if match_found:
            tour_record["Arctic_Variants"] = arctic_clusters[match_found]
            matched_clusters.add(match_found)
            tour_record["Sync_Status"] = "Linked"
        else:
            tour_record["Sync_Status"] = "Web_Only"

        final_tours.append(tour_record)

    for cluster_key, variants in arctic_clusters.items():
        if cluster_key in matched_clusters:
            continue
        proxy_title = variants[0].get("name") or "Arctic Variant"
        display_title = re.sub(r"\(.*?\)", "", proxy_title).replace("*", "").strip()
        orphan_record = {
            "Master_Name": display_title,
            "Website_ID": None,
            "Slug": None,
            "Description_Short": "Arctic Only Record",
            "Description_Long": None,
            "Arctic_Variants": variants,
            "Sync_Status": "Arctic_Only",
        }
        final_tours.append(orphan_record)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as fh:
        json.dump(final_tours, fh, indent=4, ensure_ascii=False)
    print(f"Done. Exported {len(final_tours)} Unified Master Tours to '{OUTPUT_JSON}'.")

    summary_rows = []
    for record in final_tours:
        variant_ids = [
            variant.get("id")
            for variant in record["Arctic_Variants"]
            if variant.get("id")
        ]
        summary_rows.append(
            {
                "Master_Name": record["Master_Name"],
                "Sync_Status": record["Sync_Status"],
                "Variant_Count": len(record["Arctic_Variants"]),
                "Variant_IDs": ", ".join(variant_ids),
            }
        )
    pd.DataFrame(summary_rows).to_csv(SUMMARY_CSV, index=False)
    print(f"Saved summary to '{SUMMARY_CSV}'")


if __name__ == "__main__":
    main()
