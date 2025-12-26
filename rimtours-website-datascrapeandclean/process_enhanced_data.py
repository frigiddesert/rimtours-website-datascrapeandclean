"""
Enhanced RimTours Data Processing System
Handles complex pricing information and ACF field value resolution
"""
import pandas as pd
import json
import re
import os
from difflib import SequenceMatcher

def clean_html_content(raw_html):
    """
    Converts HTML tables/paragraphs into clean text with proper formatting.
    """
    if pd.isna(raw_html) or str(raw_html).strip() == "": 
        return ""
    
    txt = str(raw_html)
    
    # 1. Handle HTML table structure before stripping tags
    # Replace table elements with newlines/spacing
    txt = txt.replace('<table>', '\nTABLE_START\n').replace('</table>', '\nTABLE_END\n')
    txt = txt.replace('<tr>', '\nROW: ').replace('</tr>', '\n')
    txt = txt.replace('<td>', ' | ').replace('</td>', ' |')
    txt = txt.replace('<th>', ' | ').replace('</th>', ' |')
    txt = txt.replace('<br>', '\n').replace('<br/>', '\n').replace('<p>', '\n').replace('</p>', '\n')
    
    # 2. Strip all remaining HTML tags
    txt = re.sub(r'<[^<]+?>', '', txt)
    
    # 3. Clean up entities and normalize whitespace
    txt = txt.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&#8217;', "'")
    
    # 4. Remove excessive newlines and normalize spacing
    lines = [line.strip() for line in txt.split('\n') if line.strip()]
    clean_text = "\n".join(lines)
    
    # 5. Fix common formatting issues in pricing tables
    clean_text = re.sub(r'\|\s*\|\s*', '\n', clean_text)  # Replace empty cell separators with newlines
    clean_text = re.sub(r'\s+', ' ', clean_text)  # Normalize internal spacing
    
    return clean_text.strip()

def parse_complex_pricing_table(pricing_text):
    """
    Parse complex pricing information that comes in table format
    """
    if pd.isna(pricing_text) or str(pricing_text).strip() == "":
        return []
    
    clean_text = clean_html_content(pricing_text)
    
    # Pattern to identify pricing table structures
    # Looks for patterns like "# pp in reservation | Solo $XXX pp | 2+ $YYY pp"
    lines = clean_text.split('\n')
    pricing_data = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Look for pricing patterns
        # Pattern 1: "# pp in reservation" followed by pricing options
        if '# pp in reservation' in line.lower():
            # This indicates a pricing structure line, parse the following content
            continue
        
        # Pattern 2: Solo pricing
        solo_pattern = r'(Solo|Single)\s*\$(\d+)\s*(pp|per person)'
        solo_match = re.search(solo_pattern, line, re.IGNORECASE)
        if solo_match:
            pricing_data.append({
                "type": "Solo",
                "price": f"${solo_match.group(2)} per {solo_match.group(3)}",
                "raw": line
            })
        
        # Pattern 3: Group pricing (2+, 4+, etc.)
        group_pattern = r'(\d+)\s*\+\s*\$(\d+)\s*(pp|per person)'
        for match in re.finditer(group_pattern, line, re.IGNORECASE):
            pricing_data.append({
                "type": f"{match.group(1)}+ Persons",
                "price": f"${match.group(2)} per {match.group(3)}",
                "raw": line
            })
        
        # Pattern 4: Range pricing (2-3, 4-8, etc.)
        range_pattern = r'(\d+)\s*-\s*(\d+)\s*\$(\d+)\s*(pp|per person)'
        for match in re.finditer(range_pattern, line, re.IGNORECASE):
            pricing_data.append({
                "type": f"{match.group(1)}-{match.group(2)} Persons",
                "price": f"${match.group(3)} per {match.group(4)}",
                "raw": line
            })
    
    return pricing_data

def resolve_acf_field_values(row_data, field_key):
    """
    Resolve ACF field keys to actual values from the row data
    """
    # The field_key (like 'field_562fac25a8d14') corresponds to a column in the export
    # In the export, the actual value is typically in the column without the underscore prefix
    # while the field key is in the column with the underscore prefix
    
    # Look for the corresponding value column
    for col in row_data.index:
        if str(col).startswith('_') and str(row_data[col]) == field_key:
            # Found the field key in a prefixed column, get the value from the non-prefixed version
            value_col = col[1:]  # Remove the underscore prefix
            if value_col in row_data:
                return row_data[value_col]
    
    # Alternative: if the field key itself is in a non-prefixed column as the header
    # This is the more likely scenario - the column header IS the field key
    for col in row_data.index:
        if str(col) == field_key and pd.notna(row_data[col]):
            return row_data[col]
    
    return None

def extract_real_field_values(row_series):
    """
    Extract actual field values instead of field references
    """
    result = {}
    
    for col in row_series.index:
        value = row_series[col]
        
        # If the value looks like a field reference, try to resolve it
        if pd.notna(value) and str(value).startswith('field_'):
            # This is likely a field reference - try to find the actual value
            actual_value = resolve_acf_field_reference(row_series, str(value))
            if actual_value:
                # Use the column name without prefix as the result key
                clean_col = str(col).lstrip('_')
                result[clean_col] = actual_value
            else:
                # If we can't resolve it, use the original (but mark it)
                clean_col = str(col).lstrip('_')
                result[clean_col] = f"[UNRESOLVED: {value}]"
        else:
            # Regular value, just add it
            clean_col = str(col).lstrip('_')
            result[clean_col] = value if pd.notna(value) else ""
    
    return result

def resolve_acf_field_reference(row_data, field_key):
    """
    Find the actual value for an ACF field key
    """
    # In the WordPress export, ACF field values are stored in columns named after the field keys
    # Look for any column that might contain the value for this field key
    for col, value in row_data.items():
        if pd.isna(value):
            continue
            
        # Check if this column contains the field key in its name or content
        if field_key in str(col) or (isinstance(value, str) and field_key in value):
            # If this column's name matches the field key pattern, return its value
            return value
    
    # Also check if there's a corresponding column with a different naming convention
    # Field keys in ACF often map to similarly named columns
    # e.g., field_562fac25a8d14 might correspond to a column named 'bike_rental' or similar
    field_suffix = field_key.replace('field_', '')
    
    for col, value in row_data.items():
        if pd.notna(value) and str(col) != field_key:
            # Check if this column might be related to the field key
            col_str = str(col).lower()
            if (field_suffix in col_str or 
                ('rental' in col_str and 'bike' in field_key) or
                ('fee' in col_str and ('shuttle' in field_key or 'camp' in field_key))):
                return value
    
    return None

def parse_tour_data_from_row(row):
    """
    Parse a single tour data row with proper field resolution
    """
    # Extract the actual values from the row, resolving field references where possible
    resolved_data = {}
    
    for col, value in row.items():
        clean_col = str(col).lstrip('_')  # Remove underscore prefixes
        
        if pd.isna(value):
            resolved_data[clean_col] = ""
        elif str(value).startswith('field_') and len(str(value)) > 10:
            # This looks like a field reference, try to resolve it
            resolved_value = resolve_acf_field_reference(row, str(value))
            if resolved_value and resolved_value != value:  # Only use if we got a different value
                resolved_data[clean_col] = resolved_value
            else:
                resolved_data[clean_col] = str(value)  # Keep field reference if unresolvable
        else:
            resolved_data[clean_col] = value
    
    # Clean up specific fields
    subtitle = clean_html_content(resolved_data.get('subtitle', ''))
    short_desc = clean_html_content(resolved_data.get('short_description', ''))
    long_desc = clean_html_content(resolved_data.get('description', ''))
    
    # Parse pricing information properly
    standard_price_raw = resolved_data.get('standard_price', '')
    private_price_raw = resolved_data.get('private_tour_price', '')
    
    standard_pricing = parse_complex_pricing_table(standard_price_raw) if standard_price_raw else []
    private_pricing = parse_complex_pricing_table(private_price_raw) if private_price_raw else []
    
    # Extract fees information
    bike_rental = clean_html_content(resolved_data.get('bike_rental', ''))
    camp_rental = clean_html_content(resolved_data.get('camp_rental', ''))
    shuttle_fee = clean_html_content(resolved_data.get('shuttle_fee', ''))
    
    # Extract other information
    departs = clean_html_content(resolved_data.get('departs', ''))
    distance = clean_html_content(resolved_data.get('distance', ''))
    special_notes = clean_html_content(resolved_data.get('special_notes', ''))
    dates = clean_html_content(resolved_data.get('dates', ''))
    
    # Extract images
    image_urls = resolved_data.get('featured_image', resolved_data.get('Image URL', ''))
    if pd.isna(image_urls):
        image_filenames = []
    else:
        # Extract filenames from image URLs
        urls = str(image_urls).split('|')
        image_filenames = []
        for url in urls:
            filename = url.split('/')[-1].strip()
            if filename and filename not in image_filenames:
                image_filenames.append(filename)
    
    return {
        'title': resolved_data.get('title', resolved_data.get('Title', 'Unknown')),
        'url': resolved_data.get('permalink', resolved_data.get('url', resolved_data.get('Permalink', ''))),
        'subtitle': subtitle,
        'region': clean_html_content(resolved_data.get('region', resolved_data.get('Region', ''))),
        'skill_level': clean_html_content(resolved_data.get('skill_level', '')),
        'season': clean_html_content(resolved_data.get('season', resolved_data.get('Season', ''))),
        'short_description': short_desc,
        'long_description': long_desc,
        'departs': departs,
        'distance': distance,
        'standard_pricing': standard_pricing,
        'private_pricing': private_pricing,
        'bike_rental': bike_rental,
        'camp_rental': camp_rental,
        'shuttle_fee': shuttle_fee,
        'special_notes': special_notes,
        'dates': dates,
        'images': image_filenames,
        'website_id': resolved_data.get('id', resolved_data.get('ID', '')),
        'slug': resolved_data.get('slug', resolved_data.get('Slug', '')),
        'duration': clean_html_content(resolved_data.get('duration', resolved_data.get('Duration', ''))),
        'tour_type': resolved_data.get('tour_type', resolved_data.get('Tour Type', ''))
    }

def generate_enhanced_markdown(tour_data):
    """
    Generate enhanced markdown with properly formatted pricing and resolved field values
    """
    title = tour_data['title']
    subtitle = tour_data['subtitle']
    region = tour_data['region']
    skill = tour_data['skill_level']
    season = tour_data['season']
    short_desc = tour_data['short_description']
    long_desc = tour_data['long_description']
    images = tour_data['images']
    
    markdown = f"""# {title}

<!-- SYSTEM METADATA -->
| Arctic Code | System Status | Website ID |
| :--- | :--- | :--- |
| **[TO BE LINKED]** | Processed | {tour_data['website_id']} |

---

## 1. The Shared DNA
**Subtitle:** {subtitle}  
**Region:** {region}  
**Skill Level:** {skill}  
**Season:** {season}

**Short Description:**
> {short_desc}

**Long Description:**
> {long_desc[:1500] if long_desc else ''}...

**Images (Filenames):**
`{', '.join(images) if images else 'No images found'}`

## 🌐 Website Links
- **Tour Page:** [{tour_data['url']}]({tour_data['url']})

## 💵 Pricing Information
"""
    
    # Add standard pricing in a clean format
    if tour_data['standard_pricing']:
        markdown += "**Standard Pricing:**\n\n"
        markdown += "| Group Size | Price |\n"
        markdown += "| :--- | :--- |\n"
        for pricing in tour_data['standard_pricing']:
            markdown += f"| {pricing['type']} | {pricing['price']} |\n"
        markdown += "\n"
    else:
        # If we have raw standard price data, try to format it cleanly
        raw_standard = tour_data.get('standard_price_raw', '')
        if raw_standard and 'field_' not in str(raw_standard):
            clean_standard = clean_html_content(raw_standard)
            if clean_standard:
                markdown += f"**Standard Price:**\n{clean_standard}\n\n"
    
    # Add private pricing in a clean format  
    if tour_data['private_pricing']:
        markdown += "**Private Pricing:**\n\n"
        markdown += "| Group Size | Price |\n"
        markdown += "| :--- | :--- |\n"
        for pricing in tour_data['private_pricing']:
            markdown += f"| {pricing['type']} | {pricing['price']} |\n"
        markdown += "\n"
    else:
        raw_private = tour_data.get('private_price_raw', '')
        if raw_private and 'field_' not in str(raw_private):
            clean_private = clean_html_content(raw_private)
            if clean_private:
                markdown += f"**Private Price:**\n{clean_private}\n\n"
    
    # Add fees section if any fees are available
    has_fees = any([
        tour_data['bike_rental'],
        tour_data['camp_rental'], 
        tour_data['shuttle_fee']
    ])
    
    if has_fees:
        markdown += "## 💰 Fees & Logistics\n"
        markdown += "| Item | Cost / Details |\n"
        markdown += "| :--- | :--- |\n"
        if tour_data['bike_rental']:
            markdown += f"| **Bike Rental** | {clean_html_content(tour_data['bike_rental'])} |\n"
        if tour_data['camp_rental']:
            markdown += f"| **Camp Kit** | {clean_html_content(tour_data['camp_rental'])} |\n"
        if tour_data['shuttle_fee']:
            markdown += f"| **Shuttle Service** | {clean_html_content(tour_data['shuttle_fee'])} |\n"
        markdown += "\n"
    
    # Add additional information if available
    if tour_data['departs'] or tour_data['distance'] or tour_data['special_notes']:
        markdown += "## 📋 Additional Information\n"
        if tour_data['departs']:
            markdown += f"**Departs From:** {tour_data['departs']}\n\n"
        if tour_data['distance']:
            markdown += f"**Distance:** {tour_data['distance']}\n\n"
        if tour_data['special_notes']:
            markdown += f"**Special Notes:** {tour_data['special_notes']}\n\n"
    
    # Add dates if available
    if tour_data['dates']:
        markdown += "## 📅 Available Dates\n"
        markdown += f"{clean_html_content(tour_data['dates'])}\n\n"
    
    markdown += "## 📝 Full Description\n"
    markdown += f"{long_desc}\n"
    
    return markdown

def process_all_tours(input_csv_path, output_dir):
    """
    Process all tours from the input CSV and generate clean markdown files
    """
    print("🔄 Processing all tours with enhanced field resolution...")
    
    # Load the data
    df = pd.read_csv(input_csv_path, dtype=str)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    processed_count = 0
    
    for idx, row in df.iterrows():
        try:
            # Parse the tour data with field resolution
            tour_data = parse_tour_data_from_row(row)
            
            # Generate markdown
            markdown_content = generate_enhanced_markdown(tour_data)
            
            # Create safe filename
            safe_title = re.sub(r'[^\w\-_\. ]', '_', tour_data['title'])
            filename = f"{safe_title}.md"
            filepath = os.path.join(output_dir, filename)
            
            # Write the file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"✅ Generated: {filename}")
            processed_count += 1
            
        except Exception as e:
            print(f"❌ Error processing tour at index {idx}: {str(e)}")
            continue
    
    print(f"🎉 Completed! Processed {processed_count} tour files in {output_dir}/")
    print(f"📁 Files include properly formatted pricing and resolved ACF field values")

if __name__ == "__main__":
    # Process the website export data
    process_all_tours('website_export.csv', 'enhanced_markdown_output')