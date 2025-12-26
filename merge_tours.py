import argparse
import html
import pandas as pd
import re
from difflib import SequenceMatcher
from typing import List, Dict, Any

# ==========================================
# CONFIGURATION
# ==========================================

WEBSITE_MAPPING = {
    # System IDs
    "Web_ID": ["ID"],
    "Web_Slug": ["Slug", "_wp_old_slug"],
    "Web_Permalink": ["Permalink"],
    # Core Identity
    "Tour_Name": ["Title"],
    "Subtitle": ["subtitle", "_subtitle"],
    "Region": ["region", "_region", "Region"],
    "Season": ["Season", "_season"],
    "Activity_Type": ["tour_type", "_tour_type", "Tour Type"],
    "Style": ["Style", "tour_style"],
    # Marketing Content
    "Short_Desc": ["short_description", "_short_description", "Excerpt"],
    "Long_Desc_HTML": ["Content", "description", "_description"],
    "Highlights": ["postinfo_bottom", "_bottom_content"],
    "Travel_Info": ["travel_info", "_travel_info"],
    "Weather_Info": ["weather", "_weather", "map-weather-tab"],
    "Special_Notes": ["special_note", "_special_note"],
    # Logistics
    "Duration_Web": ["duration", "_duration", "Day-Duration"],
    "Departs": ["departs", "_departs"],
    "Distance_Total": ["distance", "_distance"],
    "Skill_Level": ["skill_level", "_skill_level"],
    # Pricing
    "Price_Standard": ["standard_price", "_standard_price"],
    "Price_Private": ["private_tour_price", "_private_tour_price"],
    "Price_Single_Occ": ["single_occupancy", "_single_occupancy"],
    "Price_Bike_Rental": ["bike_rental_fee", "_bike_rental_fee", "bike_pricing"],
    "Price_Camp_Rental": ["camp_rental_fee", "_camp_rental_fee"],
    "Price_Shuttle": ["shuttle_fee", "_shuttle_fee"],
    # SEO
    "SEO_Title": ["yoast_wpseo_title"],
    "SEO_Desc": ["yoast_wpseo_metadesc"],
    # Images
    "Image_Feature_URL": ["Image URL", "Featured Image"],
}

ARCTIC_COLUMN_MAP = {
    "id": "Arctic_ID",
    "name": "Arctic_Name",
    "ordetails": "Arctic_Long_Desc",
    "invoicesectiondesc": "Arctic_Short_Desc",
    "duration": "Arctic_Duration",
    "keywords": "Arctic_Keywords",
    "color": "Arctic_Color",
    "minimumguests": "Arctic_Min_Guests",
}

NEW_FIELDS = [
    "Day_1_Mileage",
    "Day_1_Elevation",
    "Day_1_Camp",
    "Day_2_Mileage",
    "Day_2_Elevation",
    "Day_2_Camp",
    "Day_3_Mileage",
    "Day_3_Elevation",
    "Day_3_Camp",
    "Day_4_Mileage",
    "Day_4_Elevation",
    "Day_4_Camp",
    "Day_5_Mileage",
    "Day_5_Elevation",
    "Day_5_Camp",
    "Total_Elevation_Gain",
    "Max_Elevation",
    "Technical_Rating",
]

TEXT_COLUMNS = [
    "Short_Desc",
    "Long_Desc_HTML",
    "Arctic_Long_Desc",
    "Arctic_Short_Desc",
    "Highlights",
    "Travel_Info",
    "Weather_Info",
    "Special_Notes",
]

VARIATION_FIELDS = [
    "Master_Name",
    "Variant_Name",
    "Tour_Type",
    "Duration_Category",
    "Bike_Type",
]

WEBSITE_CSV = "website_export.csv"
ARCTIC_CSV = "arctic_triptype.csv"
OUTPUT_CSV = "master_tour_data.csv"
DEFAULT_THRESHOLD = 0.88


# ==========================================
# HELPER FUNCTIONS
# ==========================================


def clean_arctic_keywords(val):
    """Parses Arctic CSV keywords looking like '{""Camping"",""Intermediate""}'"""
    if pd.isna(val) or val == "":
        return ""
    cleaned = re.sub(r'["{}]', "", str(val))
    return cleaned.replace(",", ", ")


def normalize_name(name):
    """Normalizes tour names for matching (removes case, special chars)"""
    if pd.isna(name) or not str(name).strip():
        return ""
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def consolidate_column(row, options):
    """Finds the first non-empty value from the list of column options."""
    for col in options:
        if col in row.index and not pd.isna(row[col]) and str(row[col]).strip():
            return row[col]
    return None


def strip_html(value):
    if pd.isna(value) or not str(value).strip():
        return ""
    text = str(value)
    text = re.sub(
        r"<(script|style)[^>]*>.*?</\\1>", " ", text, flags=re.IGNORECASE | re.DOTALL
    )
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_csv(path):
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except FileNotFoundError:
        print(f"ERROR: {path} not found.")
        raise


def prepare_website_df(df_web):
    clean_rows = []
    for _, row in df_web.iterrows():
        new_row = {}
        for new_col, candidates in WEBSITE_MAPPING.items():
            new_row[new_col] = consolidate_column(row, candidates)
        new_row["match_key"] = normalize_name(new_row.get("Tour_Name"))
        clean_rows.append(new_row)
    df_clean = pd.DataFrame(clean_rows)
    df_clean["Match_Source"] = df_clean["match_key"].apply(
        lambda v: "exact" if v else ""
    )
    df_clean["Match_Confidence"] = df_clean["match_key"].apply(
        lambda v: 1.0 if v else ""
    )
    df_clean["Matched_Arctic_Name"] = ""
    return df_clean


def prepare_arctic_df(df_arctic):
    existing_cols = [c for c in ARCTIC_COLUMN_MAP if c in df_arctic.columns]
    df_arctic_clean = df_arctic[existing_cols].rename(columns=ARCTIC_COLUMN_MAP)

    if "Arctic_Keywords" in df_arctic_clean.columns:
        df_arctic_clean["Arctic_Keywords"] = df_arctic_clean["Arctic_Keywords"].apply(
            clean_arctic_keywords
        )

    if "Arctic_Name" in df_arctic_clean.columns:
        df_arctic_clean["match_key"] = df_arctic_clean["Arctic_Name"].apply(
            normalize_name
        )

    return df_arctic_clean


def add_new_fields(df):
    for field in NEW_FIELDS:
        if field not in df.columns:
            df[field] = ""
    return df


def add_variation_fields(df):
    if "Master_Name" not in df.columns:
        df["Master_Name"] = ""
    df["Master_Name"] = df["Tour_Name"].fillna(df["Arctic_Name"]).fillna("")

    if "Variant_Name" not in df.columns:
        df["Variant_Name"] = ""
    df["Variant_Name"] = df["Arctic_Name"].fillna(df["Tour_Name"]).fillna("")

    if "Tour_Type" not in df.columns:
        df["Tour_Type"] = ""
    df["Tour_Type"] = df["Tour_Name"].where(
        df["Activity_Type"].isna() | (df["Activity_Type"] == ""), df["Activity_Type"]
    )
    df.loc[df["Tour_Type"].isna(), "Tour_Type"] = ""

    if "Duration_Category" not in df.columns:
        df["Duration_Category"] = ""
    df["Duration_Category"] = (
        df["Duration_Web"].fillna(df["Arctic_Duration"]).fillna("")
    )

    if "Bike_Type" not in df.columns:
        df["Bike_Type"] = ""
    return df


def apply_text_cleanup(df):
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(strip_html)
    return df


def get_unmatched_indices(df_web, arctic_keys):
    return df_web.index[
        (df_web["match_key"].isna())
        | (df_web["match_key"].eq(""))
        | (~df_web["match_key"].isin(arctic_keys))
    ]


def best_fuzzy_match(name: str, arctic_records: List[Dict[str, Any]]):
    target = (name or "").strip()
    if not target:
        return None, None, 0.0
    best = (None, None, 0.0)
    for rec in arctic_records:
        candidate = rec.get("Arctic_Name", "") or ""
        score = SequenceMatcher(None, target.lower(), candidate.lower()).ratio()
        if score > best[2]:
            best = (rec["match_key"], candidate, score)
    return best


def auto_fuzzy_align(df_web, df_arctic, threshold):
    if threshold <= 0 or threshold > 1:
        threshold = DEFAULT_THRESHOLD
    arctic_records = (
        df_arctic[["match_key", "Arctic_Name", "Arctic_ID"]]
        .dropna(subset=["match_key"])
        .to_dict("records")
    )
    arctic_keys = {rec["match_key"] for rec in arctic_records}
    unmatched_idx = get_unmatched_indices(df_web, arctic_keys)
    count = 0
    for idx in unmatched_idx:
        match_key, match_name, score = best_fuzzy_match(
            df_web.at[idx, "Tour_Name"], arctic_records
        )
        if match_key and score >= threshold:
            df_web.at[idx, "match_key"] = match_key
            df_web.at[idx, "Match_Source"] = "auto_fuzzy"
            df_web.at[idx, "Match_Confidence"] = round(score, 4)
            df_web.at[idx, "Matched_Arctic_Name"] = match_name
            count += 1
    return df_web, count


def build_suggestions(target, arctic_records, limit):
    results = []
    for rec in arctic_records:
        candidate = rec.get("Arctic_Name", "") or ""
        score = SequenceMatcher(None, (target or "").lower(), candidate.lower()).ratio()
        results.append({**rec, "score": score})
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:limit]


def interactive_review(df_web, df_arctic, limit):
    arctic_records = (
        df_arctic[["match_key", "Arctic_Name", "Arctic_ID"]]
        .dropna(subset=["match_key"])
        .to_dict("records")
    )
    arctic_keys = {rec["match_key"] for rec in arctic_records}
    unmatched_idx = list(get_unmatched_indices(df_web, arctic_keys))

    if not unmatched_idx:
        print("All website tours already matched; nothing to review.")
        return df_web

    print("\n=== Interactive Match Review ===")
    print(
        "Type the option number to accept a suggestion, 's' to skip, 'q' to quit review,"
    )
    print("or enter part of an Arctic tour name/ID to search manually.\n")

    for idx in unmatched_idx:
        web_name = df_web.at[idx, "Tour_Name"] or "(no title)"
        print(f"\nWebsite tour: {web_name}")
        suggestions = build_suggestions(web_name, arctic_records, limit)
        for i, suggestion in enumerate(suggestions, start=1):
            arc_id = suggestion.get("Arctic_ID", "?")
            print(
                f"  {i}. {suggestion['Arctic_Name']} (Arctic ID: {arc_id}, score {suggestion['score']:.2f})"
            )

        choice = input("Select # / search text / 's' skip / 'q' quit: ").strip()

        if not choice:
            continue
        if choice.lower() == "q":
            print("Exiting review…")
            break
        if choice.lower() == "s":
            continue
        if choice.isdigit():
            idx_choice = int(choice) - 1
            if 0 <= idx_choice < len(suggestions):
                selected = suggestions[idx_choice]
                df_web = _assign_manual_match(df_web, idx, selected)
            else:
                print("Invalid option number.")
            continue

        matches = _search_arctic(choice, arctic_records)
        if not matches:
            print("No Arctic records matched that text.")
            continue
        if len(matches) == 1:
            df_web = _assign_manual_match(df_web, idx, matches[0])
        else:
            print("Multiple matches found:")
            for i, option in enumerate(matches[:limit], start=1):
                arc_id = option.get("Arctic_ID", "?")
                print(
                    f"  {i}. {option['Arctic_Name']} (Arctic ID: {arc_id}, score {option['score']:.2f})"
                )
            sub_choice = input("Pick one of these numbers or 's' to skip: ").strip()
            if sub_choice.isdigit():
                idx_choice = int(sub_choice) - 1
                if 0 <= idx_choice < len(matches[:limit]):
                    df_web = _assign_manual_match(df_web, idx, matches[idx_choice])
                else:
                    print("Invalid option number.")
            else:
                print("Skipping.")

    return df_web


def _assign_manual_match(df_web, row_idx, arctic_record):
    df_web.at[row_idx, "match_key"] = arctic_record["match_key"]
    df_web.at[row_idx, "Match_Source"] = "manual"
    df_web.at[row_idx, "Match_Confidence"] = round(arctic_record.get("score", 1.0), 4)
    df_web.at[row_idx, "Matched_Arctic_Name"] = arctic_record.get("Arctic_Name", "")
    print(
        f"Matched to: {arctic_record.get('Arctic_Name', '(unknown)')} (ID {arctic_record.get('Arctic_ID', '?')})"
    )
    return df_web


def _search_arctic(query, arctic_records):
    query_lower = query.lower()
    results = []
    for rec in arctic_records:
        name = rec.get("Arctic_Name", "") or ""
        arc_id = str(rec.get("Arctic_ID", ""))
        if query_lower in name.lower() or query_lower == arc_id.lower():
            rec_with_score = rec.copy()
            rec_with_score["score"] = SequenceMatcher(
                None, query_lower, name.lower()
            ).ratio()
            results.append(rec_with_score)
    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def summarize_matches(df_web, arctic_keys):
    unmatched = len(get_unmatched_indices(df_web, arctic_keys))
    summary = df_web["Match_Source"].value_counts().to_dict()
    return summary, unmatched


def parse_args():
    parser = argparse.ArgumentParser(
        description="Merge website and Arctic exports, clean fields, and align tours."
    )
    parser.add_argument(
        "--website", default=WEBSITE_CSV, help="Path to website export CSV"
    )
    parser.add_argument(
        "--arctic", default=ARCTIC_CSV, help="Path to Arctic export CSV"
    )
    parser.add_argument("--output", default=OUTPUT_CSV, help="Path for merged CSV")
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Auto-match similarity threshold between 0 and 1",
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help="Launch interactive CLI to review unmatched tours",
    )
    parser.add_argument(
        "--suggestions",
        type=int,
        default=5,
        help="Number of suggestions to display during review",
    )
    return parser.parse_args()


# ==========================================
# MAIN EXECUTION
# ==========================================


def main():
    args = parse_args()

    print("--- Starting Master Merge ---")
    df_web = load_csv(args.website)
    print(f"Loaded Website Data: {len(df_web)} rows")

    df_arctic = load_csv(args.arctic)
    print(f"Loaded Arctic Data: {len(df_arctic)} rows")

    df_web_clean = prepare_website_df(df_web)
    df_arctic_clean = prepare_arctic_df(df_arctic)

    df_web_clean, auto_matches = auto_fuzzy_align(
        df_web_clean, df_arctic_clean, args.threshold
    )
    if auto_matches:
        print(
            f"Auto-matched {auto_matches} tours via fuzzy scoring >= {args.threshold}."
        )

    if args.review:
        df_web_clean = interactive_review(
            df_web_clean, df_arctic_clean, args.suggestions
        )

    master_df = pd.merge(df_web_clean, df_arctic_clean, on="match_key", how="outer")
    master_df["Tour_Name"] = master_df["Tour_Name"].fillna(master_df["Arctic_Name"])

    master_df = add_variation_fields(master_df)
    master_df = add_new_fields(master_df)
    master_df = apply_text_cleanup(master_df)

    if "match_key" in master_df.columns:
        master_df = master_df.drop(columns=["match_key"])

    master_df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print("--- Success! ---")
    print(f"Merged data saved to '{args.output}'. Total Rows: {len(master_df)}")

    arctic_keys = set(df_arctic_clean["match_key"].dropna())

    summary, remaining = summarize_matches(df_web_clean, arctic_keys)
    print("Match summary:")
    for source, count in summary.items():
        label = source or "unmatched"
        print(f"  {label}: {count}")
    print(f"Remaining unmatched website tours: {remaining}")


if __name__ == "__main__":
    main()
