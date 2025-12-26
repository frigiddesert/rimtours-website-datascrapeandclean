import pandas as pd
import re
import json
from difflib import SequenceMatcher

# ==========================================
# 1. CONFIGURATION
# ==========================================
WEBSITE_CSV = 'website_export.csv' # Rename your Tours-Export file to this for simplicity
ARCTIC_CSV = 'arctic_triptype.csv'
PRICING_CSV = 'arctic_pricing_final.csv'

# MAPPING: Fix "Split Clusters"
MANUAL_OVERRIDES = {
    "amasa back full day": "Amasa Back / Ahab",
    "amasa back": "Amasa Back / Ahab",
    "raptor": "Raptor Route",
    "raptor full day": "Raptor Route",
    "e-crested butte singletrack": "Crested Butte eMTB Singletrack/Campout",
    "slickrock trail ebike": "Slickrock Ebike – Advanced", 
    "moab backcountry st": "Moab Backcountry Singletrack Camp"
}

# Words to strip for matching
STOP_WORDS = [
    r"\(Private\)", r"\(Standard\)", r"\*", r"Half Day", r"Full Day", 
    r"Singletrack", r"Ebike", r"E-Bike", r"Private", r"Standard", 
    r"Morning", r"Afternoon", r"Tour", r"\d+-Day", r"\d+ Day", 
    r"Camp-based", r"Inn-based"
]

# ==========================================
# 2. HELPERS
# ==========================================
def clean_html(raw_html):
    """Removes HTML tags and cleans whitespace."""
    if pd.isna(raw_html) or raw_html == "": return ""
    # Remove tags
    clean = re.sub(r'<[^<]+?>', ' ', str(raw_html))
    # Fix entities
    clean = clean.replace('&nbsp;', ' ').replace('&amp;', '&')
    # Collapse whitespace
    return re.sub(r'\s+', ' ', clean).strip()

def extract_filenames(url_string):
    """
    Input: "http://site.com/img1.jpg|http://site.com/img2.jpg"
    Output: "img1.jpg, img2.jpg"
    """
    if pd.isna(url_string): return ""
    urls = str(url_string).split('|')
    filenames = [url.split('/')[-1] for url in urls if url.strip()]
    return ", ".join(filenames[:5]) # Limit to 5 images to keep notes clean

def normalize_name(name):
    if pd.isna(name): return ""
    clean = str(name)
    clean_lower = clean.lower().strip()

    # Check Overrides - only apply once, not recursively
    for key, val in MANUAL_OVERRIDES.items():
        if key in clean_lower:
            clean = val
            break  # Only apply the first match

    clean_lower = clean.lower().strip()
    for pattern in STOP_WORDS:
        clean = re.sub(f'(?i){pattern}', '', clean)
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', clean)
    return re.sub(r'\s+', ' ', clean).strip().lower()

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# ==========================================
# 3. MAIN SCRIPT
# ==========================================
def main():
    print("--- Starting Advanced Unification ---")

    # 1. LOAD DATA
    try:
        df_arctic = pd.read_csv(ARCTIC_CSV, dtype=str)
        df_web = pd.read_csv(WEBSITE_CSV, dtype=str)
        try:
            df_prices = pd.read_csv(PRICING_CSV, dtype=str)
        except:
            df_prices = pd.DataFrame()
    except Exception as e:
        print(f"Error loading files: {e}")
        return

    # 2. CLUSTER ARCTIC DATA
    arctic_clusters = {}

    for idx, row in df_arctic.iterrows():
        original_name = row.get('name', 'Unknown')
        master_key = normalize_name(original_name)
        bg_id = str(row.get('businessgroupid', ''))
        
        # PRICING LOOKUP
        price_display = "TBD"
        if not df_prices.empty:
            p_rows = df_prices[df_prices['Arctic_ID'] == row.get('id')]
            if not p_rows.empty:
                # Prioritize Standard/Adult
                std_price = p_rows[p_rows['Price_Name'].str.contains('Standard|Adult|2\+', case=False, na=False)]
                amt = std_price.iloc[0]['Amount'] if not std_price.empty else p_rows.iloc[0]['Amount']
                price_display = f"${float(amt):,.0f}"

        variant_data = {
            "Arctic_ID": row.get('id'),
            "Shortname": row.get('shortname', ''), # <--- CRITICAL NEW FIELD
            "Name": original_name,
            "Business_Group": bg_id,
            "Type": "Private" if bg_id in ['9','10','11','12','21','24'] else "Standard",
            "Duration": row.get('duration'),
            "Color": row.get('color'),
            "Price": price_display
        }

        if master_key not in arctic_clusters: arctic_clusters[master_key] = []
        arctic_clusters[master_key].append(variant_data)

    print(f"Clustered Arctic Data into {len(arctic_clusters)} groups.")

    # 3. MATCH & MERGE WITH WEBSITE
    final_tours_map = {} 

    for idx, web_row in df_web.iterrows():
        web_title = str(web_row.get('Title', ''))
        web_key = normalize_name(web_title)
        
        # --- NEW FIELD EXTRACTION ---
        # We try standard WP headers first, then fallbacks
        def get_val(col_list):
            for c in col_list:
                if c in web_row and not pd.isna(web_row[c]): return web_row[c]
            return ""

        meta = {
            "Subtitle": get_val(['_subtitle', 'subtitle']),
            "Region": get_val(['_region', 'region', 'Region']),
            "Skill": get_val(['_skill_level', 'skill_level']),
            "Season": get_val(['_season', 'Season']),
            "Fees_Bike": clean_html(get_val(['_bike_rental_fee', 'field_562fabffa8d13'])),
            "Fees_Camp": clean_html(get_val(['_camp_rental_fee'])),
            "Fees_Shuttle": clean_html(get_val(['_shuttle_fee'])),
            "Images": extract_filenames(get_val(['Image URL', 'Featured Image']))
        }
        
        # Match Logic
        match_found = None
        if web_key in arctic_clusters:
            match_found = web_key
        else:
            best_score = 0
            for cluster_key in arctic_clusters.keys():
                score = similarity(web_key, cluster_key)
                if score > 0.85 and score > best_score:
                    best_score = score
                    match_found = cluster_key

        # Create Master Record
        if web_key not in final_tours_map:
            clean_display = re.sub(r'(?i)\d+-Day', '', web_title).strip()
            
            final_tours_map[web_key] = {
                "Master_Name": clean_display if len(clean_display) > 3 else web_title,
                "Website_ID": web_row.get('ID'),
                "Slug": web_row.get('Slug'),
                "Description_Short": clean_html(web_row.get('Excerpt') or web_row.get('short_description')),
                "Description_Long": clean_html(web_row.get('Content')), # Clean HTML here
                "Arctic_Variants": [],
                "Sync_Status": "Web_Only",
                "Meta": meta # Store the rich metadata here
            }

        # Link Variants
        if match_found:
            existing_ids = set(v['Arctic_ID'] for v in final_tours_map[web_key]["Arctic_Variants"])
            for variant in arctic_clusters[match_found]:
                if variant['Arctic_ID'] not in existing_ids:
                    final_tours_map[web_key]["Arctic_Variants"].append(variant)
            
            final_tours_map[web_key]["Sync_Status"] = "Linked"
            if match_found in arctic_clusters: del arctic_clusters[match_found]

    # 4. HANDLE ORPHANS
    for key, variants in arctic_clusters.items():
        name = re.sub(r'\(.*?\)', '', variants[0]['Name']).replace('*','').strip()
        final_tours_map[f"orphan_{key}"] = {
            "Master_Name": name,
            "Website_ID": None,
            "Description_Short": "Arctic Only",
            "Arctic_Variants": variants,
            "Sync_Status": "Arctic_Only",
            "Meta": {} # Empty meta for orphans
        }

    # 5. OUTPUT
    with open('unified_tours.json', 'w', encoding='utf-8') as f:
        json.dump(list(final_tours_map.values()), f, indent=4)
        
    print("Success! Unified JSON created with Fees, Images, and Shortnames.")

if __name__ == "__main__":
    main()