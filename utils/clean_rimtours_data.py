"""
Utility functions for cleaning and processing RimTours data
"""
import pandas as pd
import re
import html
from datetime import datetime

def clean_html_text(raw_html):
    """
    Clean HTML text and convert to plain text
    """
    if pd.isna(raw_html) or str(raw_html).strip() == "":
        return ""
    
    # Decode HTML entities
    txt = html.unescape(str(raw_html))
    
    # Remove HTML tags
    txt = re.sub(r'<[^<]+?>', ' ', txt)
    
    # Clean up spacing
    txt = re.sub(r'\s+', ' ', txt).strip()
    
    return txt

def extract_image_filenames(url_string):
    """
    Extract filenames from image URLs
    """
    if pd.isna(url_string) or str(url_string).strip() == "":
        return []
    
    urls = str(url_string).split('|')
    filenames = []
    
    for url in urls:
        filename = url.split('/')[-1].strip()
        if filename and filename not in filenames:
            filenames.append(filename)
    
    return filenames

def normalize_tour_name(name):
    """
    Normalize tour names for grouping variants
    """
    if pd.isna(name):
        return ""
    
    clean = str(name).lower().strip()
    
    # Remove day counts and other descriptors
    clean = re.sub(r'\s*\d+-Day|\s*\d+-day|\s*\(\w+\)|\s+full\s+day|\s+half\s+day', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    return clean

def categorize_business_group(duration, tour_type):
    """
    Categorize tours based on duration and type
    """
    if pd.isna(duration):
        duration = ""
    
    if pd.isna(tour_type):
        tour_type = ""
    
    duration_lower = str(duration).lower()
    type_lower = str(tour_type).lower()
    
    # Multi-day tours (Business Groups 1, 3, 4)
    multi_day_indicators = ['day', 'night', 'multi', 'week', 'long']
    is_multi_day = any(indicator in duration_lower or indicator in type_lower 
                       for indicator in multi_day_indicators) and '1-' not in duration_lower
    
    # Day tours (Business Group 2)
    day_indicators = ['single', 'day', 'half', 'full']
    is_day_tour = any(indicator in duration_lower or indicator in type_lower 
                      for indicator in day_indicators) and not is_multi_day
    
    if is_multi_day:
        # Determine specific business group based on complexity
        if 'camp' in type_lower or 'inn' in type_lower:
            return '3'  # Camping-based multi-day
        elif 'ebike' in type_lower or 'e-bike' in type_lower:
            return '4'  # eBike multi-day
        else:
            return '1'  # Standard multi-day
    elif is_day_tour:
        return '2'  # Day tour
    else:
        # Default to standard multi-day if uncertain
        return '1'

def clean_price_data(price_string):
    """
    Clean and standardize price information
    """
    if pd.isna(price_string):
        return {"price": "N/A", "currency": "USD", "type": "unknown"}
    
    price_text = str(price_string)
    
    # Extract dollar amounts
    price_match = re.search(r'\$(\d+(?:,\d{3})*(?:\.\d{2})?)', price_text)
    if price_match:
        amount = float(price_match.group(1).replace(',', ''))
        
        # Determine price type
        if 'private' in price_text.lower():
            price_type = 'private'
        elif 'standard' in price_text.lower() or 'adult' in price_text.lower():
            price_type = 'standard'
        elif 'solo' in price_text.lower() or 'single' in price_text.lower():
            price_type = 'solo'
        else:
            price_type = 'general'
        
        return {
            "price": f"${amount:,.2f}",
            "currency": "USD",
            "type": price_type,
            "original": price_text
        }
    
    return {"price": "N/A", "currency": "USD", "type": "unknown", "original": price_text}

def validate_tour_data(tour_dict):
    """
    Validate tour data structure
    """
    required_fields = ['title', 'url', 'description']
    missing_fields = []
    
    for field in required_fields:
        if field not in tour_dict or pd.isna(tour_dict[field]) or str(tour_dict[field]).strip() == "":
            missing_fields.append(field)
    
    if missing_fields:
        return False, f"Missing required fields: {missing_fields}"
    
    return True, "Valid"

def process_tour_dataframe(df):
    """
    Process a tour dataframe with cleaning and validation
    """
    # Clean text fields
    text_fields = ['title', 'subtitle', 'description', 'region', 'skill_level', 'season']
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].apply(clean_html_text)
    
    # Add business group categorization if duration is available
    if 'duration' in df.columns and 'tour_type' in df.columns:
        df['business_group'] = df.apply(
            lambda row: categorize_business_group(row['duration'], row['tour_type']), 
            axis=1
        )
    
    # Clean and process price data if available
    if 'price' in df.columns:
        df['price_cleaned'] = df['price'].apply(clean_price_data)
        df['standard_price'] = df['price_cleaned'].apply(lambda x: x.get('price', 'N/A'))
        df['price_type'] = df['price_cleaned'].apply(lambda x: x.get('type', 'unknown'))
    
    # Add validation column
    df['validation_status'] = df.apply(
        lambda row: validate_tour_data(row)[1], 
        axis=1
    )
    
    return df

def merge_with_acf_data(tour_df, acf_df):
    """
    Merge scraped tour data with ACF field data
    """
    # Normalize titles for matching
    tour_df['title_normalized'] = tour_df['title'].apply(normalize_tour_name)
    acf_df['title_normalized'] = acf_df['Title'].apply(normalize_tour_name)
    
    # Merge on normalized titles
    merged_df = pd.merge(
        tour_df, 
        acf_df, 
        left_on='title_normalized', 
        right_on='title_normalized', 
        how='left',
        suffixes=('', '_acf')
    )
    
    # Clean up duplicate columns
    cols_to_drop = [col for col in merged_df.columns if col.endswith('_acf') and col.replace('_acf', '') in merged_df.columns]
    merged_df = merged_df.drop(columns=cols_to_drop)
    
    return merged_df

def generate_markdown_report(df, output_path):
    """
    Generate a markdown report of the cleaned data
    """
    report = f"# RimTours Data Report\n\n"
    report += f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    report += f"## Summary\n\n"
    report += f"- Total tours: {len(df)}\n"
    report += f"- Valid tours: {len(df[df['validation_status'] == 'Valid'])}\n"
    report += f"- Invalid tours: {len(df[df['validation_status'] != 'Valid'])}\n"
    
    if 'business_group' in df.columns:
        report += f"- Business Group 1 (Multi-day): {len(df[df['business_group'] == '1'])}\n"
        report += f"- Business Group 2 (Day Tours): {len(df[df['business_group'] == '2'])}\n"
        report += f"- Business Group 3 (Camping): {len(df[df['business_group'] == '3'])}\n"
        report += f"- Business Group 4 (eBike): {len(df[df['business_group'] == '4'])}\n"
    
    report += f"\n## Data Fields\n\n"
    report += "| Field | Type | Sample |\n"
    report += "|-------|------|--------|\n"
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        sample = str(df[col].iloc[0]) if len(df) > 0 else "N/A"
        sample = sample[:50] + "..." if len(sample) > 50 else sample
        report += f"| {col} | {dtype} | {sample} |\n"
    
    report += f"\n## Sample Records\n\n"
    sample_df = df.head(5) if len(df) > 5 else df
    for idx, row in sample_df.iterrows():
        report += f"### Tour {idx+1}: {row.get('title', 'Unknown')}\n"
        report += f"- URL: {row.get('url', 'N/A')}\n"
        report += f"- Description: {row.get('description', '')[:100]}...\n"
        if 'business_group' in row:
            report += f"- Business Group: {row['business_group']}\n"
        report += "\n"
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Report generated: {output_path}")

# Example usage
if __name__ == "__main__":
    # This would be used as utilities within other scripts
    print("RimTours data cleaning utilities loaded")