import json
import os
import re
import pandas as pd
import docx  # pip install python-docx
from difflib import SequenceMatcher

# ==========================================
# CONFIGURATION
# ==========================================
OUTPUT_DIR = "outline_final_import"
PRICING_FILE = "arctic_pricing_final.csv"
TOURS_FILE = "unified_tours.json"
DOCS_FOLDER = "itinerary_docs"

# ==========================================
# PART 1: PRICING LOGIC
# ==========================================
def load_pricing_map():
    """
    Reads the CSV and determines the 'Best Display Price' for each Arctic ID.
    Prioritizes 'Standard... 2+' or 'Adult' rates.
    """
    try:
        df = pd.read_csv(PRICING_FILE)
    except FileNotFoundError:
        print("⚠️ Pricing file not found. Skipping prices.")
        return {}

    price_map = {}
    
    # Group by Arctic ID
    grouped = df.groupby('Arctic_ID')
    
    for arctic_id, group in grouped:
        best_price = "TBD"
        
        # Logic: Find the "Standard" price (usually 2+ people)
        # 1. Filter out "Solo", "MAC" (Agent), "Viator"
        valid_rows = group[~group['Price_Name'].str.contains('Solo|solo|MAC|Viator|Net|Employee', case=False, na=False)]
        
        # 2. Look for "Standard" or "Adult"
        standard = valid_rows[valid_rows['Price_Name'].str.contains('Standard|Adult|2\+', case=False, na=False)]
        
        target_row = None
        if not standard.empty:
            target_row = standard.iloc[0]
        elif not valid_rows.empty:
            target_row = valid_rows.iloc[0]  # Fallback to whatever is left
        else:
            # If everything was filtered out, take the cheapest non-zero
            target_row = group.sort_values('Amount').iloc[0]

        if target_row is not None:
            amt = float(target_row['Amount'])
            if amt > 200000:  # filter out that weird $229,000 error in your data
                best_price = "Call for Quote"
            else:
                best_price = f"${amt:,.0f}"

        price_map[str(arctic_id)] = best_price
        
    print(f"✅ Loaded pricing for {len(price_map)} products.")
    return price_map


# ==========================================
# PART 2: ITINERARY EXTRACTION
# ==========================================
def parse_docx_itinerary(filepath):
    try:
        doc = docx.Document(filepath)
    except:
        return None

    days = []
    # Regex to find "Day 1", "Day One", "Day 1:"
    day_pattern = re.compile(r"^(?:Day|DAY)\s+(\d+|One|Two|Three|Four|Five)(?:[:\.-]|\s|$)")

    current_day = {}
    
    for para in doc.paragraphs:
        text = para.text.strip()
        if not text: 
            continue
            
        match = day_pattern.match(text)
        if match:
            # Save previous day if exists
            if current_day: 
                days.append(current_day)
            
            # Start new day
            raw_num = match.group(1)
            # Normalize words to numbers
            mapping = {'One':'1','Two':'2','Three':'3','Four':'4','Five':'5'}
            day_num = mapping.get(raw_num, raw_num)
            
            current_day = {
                "Day": day_num,
                "Text": text,  # Header
                "Content": "",
                "Miles": "",
                "Elev": "",
                "Camp": ""
            }
        else:
            if current_day:
                current_day["Content"] += text + "\n"
                
                # Extract Stats on the fly
                # Miles
                m = re.search(r"(\d+(?:\.\d+)?)\s*(?:miles|mi\b)", text, re.IGNORECASE)
                if m: 
                    current_day["Miles"] = m.group(1)
                # Elev
                e = re.search(r"(\d{1,3}(?:,\d{3})?)\s*(?:ft|feet|’|')\s*(?:gain|climb)", text, re.IGNORECASE)
                if e: 
                    current_day["Elev"] = e.group(1)
                # Camp
                c = re.search(r"(?:Camp|Lodging|Overnight):\s*(.*)", text, re.IGNORECASE)
                if c: 
                    current_day["Camp"] = c.group(1).strip()

    if current_day: 
        days.append(current_day)
    return days


def load_itineraries():
    db = {}
    if not os.path.exists(DOCS_FOLDER):
        print("⚠️ Docs folder not found. Skipping itineraries.")
        return db

    files = [f for f in os.listdir(DOCS_FOLDER) if f.endswith(".docx")]
    print(f"📄 Parsing {len(files)} itinerary documents...")
    
    for f in files:
        path = os.path.join(DOCS_FOLDER, f)
        data = parse_docx_itinerary(path)
        if data:
            db[f] = data
    return db


def find_itinerary(tour_name, db):
    # Fuzzy match filename to tour name
    best_score = 0
    best_match = None
    for fname, data in db.items():
        clean_name = fname.replace(".docx","").replace("-"," ")
        score = SequenceMatcher(None, tour_name.lower(), clean_name.lower()).ratio()
        if score > 0.65 and score > best_score:
            best_score = score
            best_match = data
    return best_match


# ==========================================
# PART 3: CATEGORIZATION & GENERATION
# ==========================================
def categorize(tour):
    name = tour['Master_Name'].lower()
    variants = tour['Arctic_Variants']
    
    # 1. Archives
    if "oregon" in name or "sierra" in name: 
        return "Archive"
    
    # 2. Rentals/Services
    if "rental" in name or "service" in name or "shuttle" in name: 
        return "Rentals & Services"
    
    # 3. Multi-Day vs Day
    is_multiday = False
    for v in variants:
        # In the actual data, the field is 'businessgroupid' not 'Business_Group'
        bg = str(v.get('businessgroupid', ''))
        if bg in ['3', '4', '23', '26']:
            is_multiday = True
    if not variants and ("4-day" in name or "3-day" in name): 
        is_multiday = True
    
    if not is_multiday:
        return "Day Tours"
    else:
        # Multi-Day Logic
        if "arizona" in name or "sonoran" in name: 
            return "Multi-Day Tours/Arizona"
        if "colorado" in name or "durango" in name or "crested" in name or "kokopelli" in name: 
            return "Multi-Day Tours/Colorado"
        return "Multi-Day Tours/Utah"


def generate_markdown(tour, itinerary, price_map):
    title = tour['Master_Name']
    
    # 1. Variants Table with PRICING
    v_rows = []
    for v in tour['Arctic_Variants']:
        # Extract Arctic ID from the variant - in actual data it's 'id' field
        aid = str(v.get('id', 'N/A'))
        price = price_map.get(aid, "Check Arctic")
        
        # Extract other fields from the actual data structure
        v_name = v.get('name', 'N/A')
        v_duration = v.get('duration', 'N/A')
        # Get the type from keywords or other fields
        v_type = v.get('keywords', 'N/A') if v.get('keywords') else v.get('name', 'N/A')
        
        # | Name | ID | Price | Duration | Type |
        row = f"| **{v_name}** | {aid} | {price} | {v_duration} | {v_type} |"
        v_rows.append(row)
    
    v_table = "\n".join(v_rows) if v_rows else "| No Linked Variants | - | - | - | - |"

    # 2. Itinerary Table
    i_rows = []
    full_text = ""
    if itinerary:
        for d in itinerary:
            desc = d['Content'][:150].replace("\n", " ") + "..."
            i_rows.append(f"| {d['Day']} | {desc} | {d['Miles']} | {d['Elev']} | {d['Camp']} | All |")
            full_text += f"### Day {d['Day']}\n{d['Content']}\n\n"
    else:
        i_rows = ["| 1 | | | | | All |", "| 2 | | | | | All |"]
    i_table = "\n".join(i_rows)

    # 3. Clean Description
    raw_desc = tour.get('Description_Long', '') or ""
    if raw_desc is None or pd.isna(raw_desc):
        raw_desc = ""
    
    clean_desc = re.sub(r'<[^<]+?>', '', str(raw_desc))[:1200]

    # TEMPLATE
    return f"""# {title}

<!-- SYSTEM METADATA -->
| System | Status | Master ID |
| :--- | :--- | :--- |
| **SSOT** | Active | {tour.get('Website_ID', 'New')} |
| **Web** | {tour.get('Sync_Status', 'Linked')} | {tour.get('Slug', 'N/A')} |

---

## 1. The Shared DNA
**Short Description:**
> {tour.get('Description_Short', '')}

**Long Description:**
> {clean_desc}...

---

## 2. Arctic Configurations & Pricing
| Variant Name | Arctic ID | Price (Est) | Duration | Type |
| :--- | :--- | :--- | :--- | :--- |
{v_table}

---

## 3. Itinerary Logic
| Day | Route Description | Miles | Elev Gain | Camp/Lodging | Applies To |
| :--- | :--- | :--- | :--- | :--- | :--- |
{i_table}

---

## 4. Operational Notes
{full_text if full_text else "*No detailed itinerary extracted.*"}
"""


# ==========================================
# MAIN
# ==========================================
def main():
    print("--- Starting Wiki Build ---")
    
    # Load Data
    try:
        with open(TOURS_FILE, 'r') as f: 
            tours = json.load(f)
    except Exception as e:
        print(f"❌ Error loading unified_tours.json: {e}")
        return

    prices = load_pricing_map()
    itineraries = load_itineraries()
    
    # Create Dirs
    if not os.path.exists(OUTPUT_DIR): 
        os.makedirs(OUTPUT_DIR)
    
    print(f"\n🚀 Generating Markdown for {len(tours)} tours...")
    
    for tour in tours:
        name = tour['Master_Name']
        if any(x in name.lower() for x in ['fake', 'test', 'aaa ', 'sold out']): 
            continue
        
        # Determine Path
        subpath = categorize(tour)
        path = os.path.join(OUTPUT_DIR, subpath)
        if not os.path.exists(path): 
            os.makedirs(path)
        
        # Match Data
        itin = find_itinerary(name, itineraries)
        
        # Generate
        md = generate_markdown(tour, itin, prices)
        
        # Save
        safe_name = re.sub(r'[^\w\-_\. ]', '_', name) + ".md"
        with open(os.path.join(path, safe_name), 'w', encoding='utf-8') as f:
            f.write(md)
            
    print(f"\n✨ Done! Files are in '{OUTPUT_DIR}/'. Drag this folder into Outline.")

if __name__ == "__main__":
    main()