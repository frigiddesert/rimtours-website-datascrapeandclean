import pandas as pd
import re
import json
from difflib import SequenceMatcher

# ==========================================
# 1. CONFIGURATION
# ==========================================
WEBSITE_CSV = 'website_export.csv'
ARCTIC_CSV = 'arctic_triptype.csv'
PRICING_CSV = 'arctic_pricing_final.csv'

# Manual fixes for names that don't quite match
MANUAL_OVERRIDES = {
    "amasa back full day": "Amasa Back / Ahab",
    "amasa back": "Amasa Back / Ahab",
    "raptor": "Raptor Route",
    "raptor full day": "Raptor Route",
    "e-crested butte singletrack": "Crested Butte eMTB Singletrack/Campout",
    "slickrock trail ebike": "Slickrock Ebike – Advanced", 
    "moab backcountry st": "Moab Backcountry Singletrack Camp"
}

# Words to remove when comparing names
STOP_WORDS = [
    r"\(Private\)", r"\(Standard\)", r"\*", r"Half Day", r"Full Day", 
    r"Singletrack", r"Ebike", r"E-Bike", r"Private", r"Standard", 
    r"Morning", r"Afternoon", r"Tour", r"\d+-Day", r"\d+ Day", 
    r"Camp-based", r"Inn-based"
]

# ==========================================
# 2. DATA CLEANING FUNCTIONS
# ==========================================
def clean_html_text(raw_html):
    """
    Turns HTML tables/paragraphs into clean text.
    """
    if pd.isna(raw_html) or str(raw_html).strip() == "": 
        return ""
    
    txt = str(raw_html)
    
    # 1. Formatting fixes before stripping
    txt = txt.replace('</td>', ' ').replace('</tr>', '\n').replace('</p>', '\n\n')
    txt = txt.replace('<br>', '\n').replace('<br/>', '\n')
    
    # 2. Strip all tags
    txt = re.sub(r'<[^<]+?>', '', txt)
    
    # 3. Cleanup entities and whitespace
    txt = txt.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&#8217;', "'")
    
    # 4. Remove excessive newlines
    lines = [line.strip() for line in txt.split('\n') if line.strip()]
    return "\n".join(lines)

def extract_image_filenames(url_string):
    """Parses 'Image URL' column to get just filenames."""
    if pd.isna(url_string): return ""
    urls = str(url_string).split('|')
    # Keep top 6 unique images
    seen = set()
    clean_list = []
    for u in urls:
        filename = u.split('/')[-1].strip()
        if filename and filename not in seen:
            clean_list.append(filename)
            seen.add(filename)
    return ", ".join(clean_list[:6])

def extract_map_images(html_content):
    """Finds image URLs hidden inside HTML Content fields."""
    if pd.isna(html_content): return ""
    # Find all src="..."
    matches = re.findall(r'src="([^"]+)"', str(html_content))
    # Filter for typical image extensions to avoid scripts
    images = [m.split('/')[-1] for m in matches if m.lower().endswith(('.jpg', '.png', '.jpeg'))]
    return ", ".join(images)

def normalize_name(name):
    if pd.isna(name): return ""
    clean = str(name).lower().strip()

    # Check overrides - only apply first match to prevent recursion
    for k, v in MANUAL_OVERRIDES.items():
        if k in clean:
            clean = v.lower().strip()
            break  # Only apply first match

    for w in STOP_WORDS:
        clean = re.sub(f'(?i){w}', '', clean)

    clean = re.sub(r'[^a-z0-9\s]', '', clean)
    return re.sub(r'\s+', ' ', clean).strip()

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    print("--- Starting Advanced Import with ACF Field Mapping ---")

    # Load Files
    try:
        df_web = pd.read_csv(WEBSITE_CSV, dtype=str)
        df_arctic = pd.read_csv(ARCTIC_CSV, dtype=str)
        try: df_price = pd.read_csv(PRICING_CSV, dtype=str)
        except: df_price = pd.DataFrame()
    except Exception as e:
        print(f"Error: {e}")
        return

    # 1. CLUSTER ARCTIC DATA
    arctic_groups = {}
    for idx, row in df_arctic.iterrows():
        orig_name = row.get('name', 'Unknown')
        master_key = normalize_name(orig_name)
        
        # Get Price
        price = "TBD"
        if not df_price.empty:
            match = df_price[df_price['Arctic_ID'] == row.get('id')]
            if not match.empty:
                # Prioritize Standard
                std = match[match['Price_Name'].str.contains('Standard|Adult|2\+', case=False, na=False)]
                val = std.iloc[0]['Amount'] if not std.empty else match.iloc[0]['Amount']
                try:
                    price = f"${float(val):,.0f}"
                except:
                    price = val  # Keep as-is if conversion fails

        item = {
            "Arctic_ID": row.get('id'),
            "Shortname": row.get('shortname', ''), # <-- Valid Shortname
            "Name": orig_name,
            "Business_Group": row.get('businessgroupid'),
            "Type": "Private" if row.get('businessgroupid') in ['9','10','11','12', '21', '24'] else "Standard",
            "Duration": row.get('duration'),
            "Price": price
        }
        
        if master_key not in arctic_groups: arctic_groups[master_key] = []
        arctic_groups[master_key].append(item)

    # 2. MERGE WITH WEBSITE (The "Waterfall" Match)
    unified_list = []
    matched_keys = set()

    for idx, web in df_web.iterrows():
        title = str(web.get('Title', ''))
        key = normalize_name(title)
        
        # Helper to find column data safely
        def get(cols):
            for c in cols:
                if c in web and not pd.isna(web[c]): return web[c]
            return ""

        # --- ACF FIELD MAPPING (Based on acf-export-2025-12-23.json) ---
        meta = {
            # Marketing Text
            "Subtitle":     clean_html_text(get(['subtitle', '_subtitle'])),
            "Region":       get(['region', '_region', 'Region']), # E.g. "St. George"
            "Skill":        get(['skill_level', '_skill_level']),
            "Season":       get(['season', '_season', 'Season']),

            # Descriptions
            "Short_Description": clean_html_text(get(['short_description', '_short_description', 'Excerpt'])),
            "Long_Description": clean_html_text(get(['description', '_description', 'Content'])),

            # Logistics
            "Departs":      get(['departs', '_departs']),
            "Distance":     get(['distance', '_distance']), # E.g. "80 miles"

            # Pricing
            "Standard_Price": clean_html_text(get(['standard_price', '_standard_price'])),
            "Private_Price": clean_html_text(get(['private_tour_price', '_private_tour_price'])),

            # Fees
            "Bike_Fee":     clean_html_text(get(['bike_rental', '_bike_rental'])),
            "Camp_Fee":     clean_html_text(get(['camp_rental', '_camp_rental'])),
            "Shuttle_Fee":  clean_html_text(get(['shuttle_fee', '_shuttle_fee'])),

            # Additional Info
            "Special_Notes": clean_html_text(get(['special_notes', '_special_notes'])),
            "Dates":        clean_html_text(get(['dates', '_dates'])),

            # Links
            "Book_Link":    get(['reservation_link', '_reservation_link', 'Reservation Link']),
            "Travel_Link":  get(['travel_info_link', '_travel_info_link']),

            # Images
            "Gallery":      extract_image_filenames(get(['Image URL', 'Featured Image'])),
            "Thumbnail":    extract_image_filenames(get(['tour_thumbnail_image', '_tour_thumbnail_image']))
        }

        # Find Arctic Match
        match_key = None
        if key in arctic_groups:
            match_key = key
        else:
            best_s = 0
            for k in arctic_groups:
                s = similarity(key, k)
                if s > 0.85 and s > best_s:
                    best_s = s
                    match_key = k
        
        # Build Record
        clean_title = re.sub(r'(?i)\d+-Day', '', title).strip()
        record = {
            "Master_Name": clean_title if len(clean_title) > 3 else title,
            "Website_ID": web.get('ID'),
            "Slug": web.get('Slug'),
            "Description_Short": clean_html_text(get(['_short_description', 'Excerpt', 'short_description'])),
            "Description_Long": clean_html_text(get(['_description', 'Content', 'description'])),
            "Arctic_Variants": [],
            "Sync_Status": "Web_Only",
            "Meta": meta
        }

        if match_key:
            # Deduping variants
            seen_ids = set()
            for v in arctic_groups[match_key]:
                if v['Arctic_ID'] not in seen_ids:
                    record['Arctic_Variants'].append(v)
                    seen_ids.add(v['Arctic_ID'])
            
            record['Sync_Status'] = "Linked"
            matched_keys.add(match_key)
            if match_key in arctic_groups: del arctic_groups[match_key]

        unified_list.append(record)

    # 3. ADD ORPHANS (Arctic Only)
    for k, vars in arctic_groups.items():
        name = re.sub(r'\(.*?\)', '', vars[0]['Name']).replace('*','').strip()
        unified_list.append({
            "Master_Name": name,
            "Website_ID": None,
            "Description_Short": "Arctic Only",
            "Arctic_Variants": vars,
            "Sync_Status": "Arctic_Only",
            "Meta": {}
        })

    # Save
    with open('unified_tours.json', 'w', encoding='utf-8') as f:
        json.dump(unified_list, f, indent=4)
        
    print(f"Done. Processed {len(unified_list)} tours with ACF field mapping.")

if __name__ == "__main__":
    main()