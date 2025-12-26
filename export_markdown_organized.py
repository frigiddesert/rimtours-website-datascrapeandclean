import json
import os
import re

# ==========================================
# CONFIGURATION
# ==========================================
OUTPUT_DIR = "outline_import"

# ------------------------------------------
# CATEGORIZATION LOGIC
# ------------------------------------------
def categorize_tour(tour):
    name = tour['Master_Name'].lower()
    variants = tour['Arctic_Variants']
    
    # 1. ARCHIVE (Oregon & California)
    if "oregon" in name or "sierra buttes" in name or "california" in name:
        return "Archive"

    # 2. RENTALS & SERVICES
    if "rental" in name or "service" in name or "shuttle" in name or "sales" in name:
        return "Rentals & Services"
        
    # 3. DETERMINE TYPE (Day vs Multi)
    # We look at Business Groups to decide.
    # Multi-Day Groups: 3 (Camp), 4 (Inn), 23 (MD-Ebike), 26 (Basecamp)
    # Day Groups: 5, 6, 7, 8, 9, 10, 11, 12, 21, 22, 24, 25
    
    is_multiday = False
    
    # Check all variants
    for v in variants:
        bg = str(v.get('businessgroupid', ''))
        if bg in ['3', '4', '23', '26']:
            is_multiday = True
            break # Once we see a multi-day variant, the whole folder is Multi-Day
    
    # Fallback for "Web Only" records (no variants to check)
    if not variants:
        if "4-day" in name or "3-day" in name or "5-day" in name or "inn tour" in name:
            is_multiday = True

    # 4. SORT INTO FOLDERS
    
    if not is_multiday:
        # ALL Day Tours go here
        return "Day Tours"
    
    else:
        # Multi-Day: Sort by State
        if "arizona" in name or "grand canyon" in name or "sonoran" in name:
            return "Multi-Day Tours/Arizona"
        
        elif "colorado" in name or "durango" in name or "crested butte" in name or "fruita" in name or "kokopelli" in name:
            # Kokopelli starts in Fruita, usually grouped with CO
            return "Multi-Day Tours/Colorado"
            
        else:
            # Default to Utah (Moab, Canyonlands, Bears Ears, Bryce/Zion)
            return "Multi-Day Tours/Utah"

# ==========================================
# MARKDOWN GENERATOR
# ==========================================
def get_variant_type(variant):
    """Determine variant type from name or keywords"""
    name = variant.get('name', '').lower()
    keywords = variant.get('keywords', '')
    
    if 'half day' in name:
        return 'Half Day'
    elif 'full day' in name:
        return 'Full Day'
    elif 'ebike' in name:
        return 'Ebike'
    elif 'private' in name:
        return 'Private'
    else:
        # Try to extract from keywords if available
        if keywords and 'Half-day' in keywords:
            return 'Half Day'
        elif keywords and 'Full-day' in keywords:
            return 'Full Day'
        else:
            return 'Multi-Day' if variant.get('duration', '0:00:00').count(':') == 2 and int(variant.get('duration', '0:00:00').split(':')[0]) > 8 else 'Day Tour'

def generate_markdown_content(tour):
    title = tour['Master_Name']
    
    # Metadata
    web_status = "Linked" if tour.get('Website_ID') else "Arctic Only"
    web_link = f"https://rimtours.com/{tour['Slug']}" if tour.get('Slug') else "N/A"
    short_desc = tour.get('Description_Short', 'TBD')
    
    # Description Cleaning
    raw_desc = tour.get('Description_Long', '') or ""
    if raw_desc is None:
        raw_desc = ""
    clean_desc = re.sub(r'<[^<]+?>', '', str(raw_desc)) # Strip HTML
    
    # Variants Table
    variant_rows = []
    for v in tour['Arctic_Variants']:
        variant_type = get_variant_type(v)
        # | Name | ID | Duration | Type | Group |
        row = f"| **{v.get('name', 'N/A')}** | {v.get('id', 'N/A')} | {v.get('duration', 'N/A')} | {variant_type} | {v.get('businessgroupid', 'N/A')} |"
        variant_rows.append(row)
    
    variant_table = "\n".join(variant_rows) if variant_rows else "| No Linked Variants | - | - | - | - |"

    # TEMPLATE
    content = f"""# {title}

<!-- SYSTEM METADATA -->
| System | Status | Master ID |
| :--- | :--- | :--- |
| **SSOT** | Active | {tour.get('Website_ID', 'New')} |
| **Web** | {web_status} | {web_link} |

---

## 1. The Shared DNA (Content)
*This description applies to ALL variations of {title}.*

**Short Description:**
> {short_desc}

**Long Description:**
> {clean_desc[:1200]}... (See website for full text)

**Highlights:**
*   [Highlight 1]
*   [Highlight 2]

---

## 2. Arctic Configurations (Discrete Records)
*Do not merge these rows. These link to specific invoices and business logic.*

| Variant Name | Arctic ID | Duration | Type | Bus. Group |
| :--- | :--- | :--- | :--- | :--- |
{variant_table}

---

## 3. Itinerary Logic
*Define the daily flow. Use 'Applies To' for differences.*

| Day | Route Description | Miles | Elev Gain | Camp/Lodging | Applies To |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | Start... | | | | All |
| 2 | | | | | All |

---

## 4. Pricing & Logistics
*Master reference.*

| Item | Price | Linked Variant |
| :--- | :--- | :--- |
| Standard | TBD | All |

"""
    return content

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    # 1. Load Data
    try:
        with open('unified_tours.json', 'r') as f:
            tours = json.load(f)
    except FileNotFoundError:
        print("Error: unified_tours.json not found.")
        return

    print(f"Processing {len(tours)} tours...")

    # 2. Create Output Directory
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # 3. Process Tours
    for tour in tours:
        name = tour['Master_Name']
        
        # Filter Junk
        if any(x in name.lower() for x in ['fake', 'test', 'aaa ', 'no new', 'sold out']):
            continue

        # Get Folder Path
        folder_subpath = categorize_tour(tour)
        full_folder_path = os.path.join(OUTPUT_DIR, folder_subpath)
        
        if not os.path.exists(full_folder_path):
            os.makedirs(full_folder_path)

        # Generate & Save
        md_content = generate_markdown_content(tour)
        safe_filename = re.sub(r'[^\w\-_\. ]', '_', name) + ".md"
        
        with open(os.path.join(full_folder_path, safe_filename), 'w', encoding='utf-8') as f:
            f.write(md_content)

    print(f"Success! Organized files created in '{OUTPUT_DIR}/'")

if __name__ == "__main__":
    main()