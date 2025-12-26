"""
Main Processing Script for RimTours Website Data
Combines scraping, cleaning, and output generation
"""
import pandas as pd
import json
import os
from datetime import datetime
from scripts.scrape_rimtours import RimToursDataScraper
from utils.clean_rimtours_data import process_tour_dataframe, generate_markdown_report, merge_with_acf_data

def main():
    print("Starting RimTours Website Data Processing Pipeline...")
    
    # Create output directories
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    # Step 1: Scrape tour data
    print("Step 1: Scraping tour data from website...")
    scraper = RimToursDataScraper()
    
    # For demo purposes, we'll use a predefined list of URLs
    # In production, this would be obtained from scrape_tour_listings()
    demo_urls = [
        "https://rimtours.com/tours/the-maze-5-day/",
        "https://rimtours.com/tours/kokopelli-trail/",
        "https://rimtours.com/tours/white-rim-4-day/",
        "https://rimtours.com/tours/grand-canyon-4-day/",
        "https://rimtours.com/tours/courthouse-loop/"
    ]
    
    # In a real scenario, you'd use:
    # tour_urls = scraper.scrape_tour_listings()
    # raw_data = scraper.scrape_tour_pages(tour_urls)
    
    # Load real data from website_export.csv
    print("Loading real data from website_export.csv...")
    try:
        df_raw = pd.read_csv('../website_export.csv')
        print(f"Loaded {len(df_raw)} tour records from website_export.csv")

        # Convert dataframe to the format expected by processing functions
        tour_data = []
        for idx, row in df_raw.iterrows():
            # Extract ACF field data with proper checks to avoid field_xxx references
            subtitle = ""
            description = ""
            region = ""
            skill_level = ""
            season = ""
            duration = ""
            depart_location = ""
            distance = ""
            available_dates = ""

            # Try different possible column names for each field, avoiding field references
            for col in df_raw.columns:
                col_lower = col.lower()
                val = row[col] if pd.notna(row[col]) else ""

                # Only use the value if it's not a field reference and is not empty
                if 'subtitle' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    subtitle = str(val)
                elif 'description' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    description = str(val)
                elif 'region' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    region = str(val)
                elif 'skill' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    skill_level = str(val)
                elif 'season' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    season = str(val)
                elif 'duration' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    duration = str(val)
                elif 'depart' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    depart_location = str(val)
                elif 'distance' in col_lower and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    distance = str(val)
                elif ('date' in col_lower or 'avail' in col_lower) and pd.notna(val) and str(val).strip() and 'field_' not in str(val):
                    available_dates = str(val)

            tour_record = {
                "title": row.get('Title', row.get('title', 'Unknown Tour')),
                "url": row.get('Permalink', row.get('permalink', f"https://rimtours.com/tour-{idx}")),
                "subtitle": subtitle,
                "description": description,
                "images": [url for url in str(row.get('Image URL', '') or '').split('|') if url.strip() and 'field_' not in url and url.startswith('http')] if pd.notna(row.get('Image URL', '')) else [],
                "prices": [price for price in str(row.get('standard_price', '') or '').split('\n') if price.strip() and 'field_' not in str(price)] if pd.notna(row.get('standard_price', '')) else [],
                "region": region,
                "skill_level": skill_level,
                "season": season,
                "duration": duration,
                "depart_location": depart_location,
                "distance": distance,
                "land_agency": "This tour is operated under permit with the Moab Field Office of the Bureau of Land Management. Rim Tours is an equal opportunity provider.",
                "available_dates": available_dates,
                "scraped_at": datetime.now().isoformat()
            }
            tour_data.append(tour_record)

        print(f"Converted {len(tour_data)} tours from raw data")

    except FileNotFoundError:
        print("ERROR: website_export.csv not found in parent directory. Using fallback demo data...")
        # Create demo data as fallback
        tour_data = [
            {
                "title": "The Maze 5-Day",
                "url": "https://rimtours.com/tours/the-maze-5-day/",
                "subtitle": "Like nothing else... where outlaws escaped into wildly remote and inaccessible canyons",
                "description": "Simply stated, the Maze is like nothing else. Legends of the old west tell stories of outlaws escaping into its wildly remote and inaccessible canyons.",
                "images": ["https://rimtours.com/image1.jpg", "https://rimtours.com/image2.jpg"],
                "prices": ["5-Day: $1575", "Deposit / Cancellation policy"],
                "region": "Moab Area",
                "skill_level": "",
                "season": "Fall|Spring",
                "duration": "5-Day/4-Night",
                "depart_location": "Green River, UT",
                "distance": "107 (excluding hiking on Day 3)",
                "land_agency": "This tour is operated under permit with the Moab Field Office of the Bureau of Land Management. Rim Tours is an equal opportunity provider.",
                "available_dates": "9/16-20, 2025",
                "scraped_at": datetime.now().isoformat()
            },
            {
                "title": "Kokopelli Trail",
                "url": "https://rimtours.com/tours/kokopelli-trail/",
                "subtitle": "From singletrack to slickrock, pinyon pines to aspens",
                "description": "This is a journey through the lands of the mythical 'Kokopelli' or wandering flute player exemplified in Southwest rock art.",
                "images": ["https://rimtours.com/image3.jpg"],
                "prices": ["$1475", "Deposit / Cancellation policy"],
                "region": "Colorado|Moab Area",
                "skill_level": "",
                "season": "Fall|Spring",
                "duration": "4-Day/3-Night",
                "depart_location": "Grand Junction, CO",
                "distance": "130 miles",
                "land_agency": "This tour is operated under permit with the Manti-La Sal National Forest and the Moab Field Office of the Bureau of Land Management. Rim Tours is an equal opportunity provider.",
                "available_dates": "4/15-4/19, 2026",
                "scraped_at": datetime.now().isoformat()
            }
        ]
    
    # Step 2: Clean and process the data
    print("Step 2: Cleaning and processing tour data...")
    df_raw = pd.DataFrame(tour_data)
    
    # Process the dataframe
    df_processed = process_tour_dataframe(df_raw.copy())
    
    # Add business group classification
    df_processed['business_group'] = df_processed.apply(
        lambda row: '3' if 'maze' in row['title'].lower() else 
                   '4' if 'kokopelli' in row['title'].lower() else '1',
        axis=1
    )
    
    # Step 3: Save processed data
    print("Step 3: Saving processed data...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as JSON
    json_path = f"data/processed/rimtours_cleaned_data_{timestamp}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(df_processed.to_dict('records'), f, indent=2, ensure_ascii=False)
    
    # Save as CSV
    csv_path = f"data/processed/rimtours_cleaned_data_{timestamp}.csv"
    df_processed.to_csv(csv_path, index=False, encoding='utf-8')
    
    # Step 4: Generate markdown report
    print("Step 4: Generating markdown report...")
    report_path = f"output/rimtours_data_report_{timestamp}.md"
    generate_markdown_report(df_processed, report_path)
    
    # Step 5: Generate final markdown files for each tour
    print("Step 5: Generating individual tour markdown files...")
    base_output_dir = f"../markdown/tour_markdowns_{timestamp}"
    os.makedirs(base_output_dir, exist_ok=True)

    for idx, row in df_processed.iterrows():
        tour_name = row['title'].replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_')
        tour_name = "".join(c for c in tour_name if c.isalnum() or c in (' ', '-', '_')).rstrip()

        # Determine the subdirectory based on business group
        business_group = str(row.get('business_group', 'Unknown'))
        if business_group in ['1', '3', '4']:
            # Multi-day tours
            if 'arizona' in row['title'].lower():
                subdir = 'Multi-Day Tours/Arizona'
            elif 'colorado' in row['title'].lower():
                subdir = 'Multi-Day Tours/Colorado'
            else:
                subdir = 'Multi-Day Tours/Utah'
        elif business_group == '2':
            # Day tours
            subdir = 'Day Tours'
        elif any(word in row['title'].lower() for word in ['rental', 'service', 'shuttle']):
            # Rentals & Services
            subdir = 'Rentals & Services'
        else:
            # Default to Day Tours if business group is unknown
            subdir = 'Day Tours'

        # Create the subdirectory
        output_subdir = os.path.join(base_output_dir, subdir)
        os.makedirs(output_subdir, exist_ok=True)

        markdown_content = f"""# {row['title']}

<!-- SYSTEM METADATA -->
| System Status | Website ID | Permalink | Business Group |
| :--- | :--- | :--- | :--- |
| Processed | {idx} | {row['url']} | {row.get('business_group', 'N/A')} |

---

## 1. The Shared DNA
**Subtitle:** {row['subtitle']}
**Region:** {row['region']}
**Skill Level:** {row['skill_level']}
**Season:** {row['season']}

**Short Description:**
> {row['description'][:200]}...

**Long Description:**
> {row['description']}

**Images:**
`{', '.join(row['images']) if isinstance(row['images'], list) else row['images']}`

## 🌐 Website Links
- **Tour Page:** [{row['url']}]({row['url']})

## 💵 Pricing Information
"""

        # Process pricing information to make it more readable
        standard_price = row.get('standard_price', 'N/A')
        private_price = row.get('private_price', 'N/A')

        if standard_price and standard_price != 'N/A':
            markdown_content += f"**Standard Price:** {standard_price}\n\n"

        if private_price and private_price != 'N/A':
            markdown_content += f"**Private Price:** {private_price}\n\n"

        # Add parsed pricing information if available
        parsed_std_pricing = row.get('parsed_standard_pricing', [])
        parsed_priv_pricing = row.get('parsed_private_pricing', [])

        if parsed_std_pricing:
            markdown_content += "**Standard Pricing Options:**\n"
            for pricing in parsed_std_pricing:
                if pricing.strip():
                    markdown_content += f"- {pricing}\n"
            markdown_content += "\n"

        if parsed_priv_pricing:
            markdown_content += "**Private Pricing Options:**\n"
            for pricing in parsed_priv_pricing:
                if pricing.strip():
                    markdown_content += f"- {pricing}\n"
            markdown_content += "\n"

        markdown_content += f"""## 📋 Additional Information
**Departs From:** {row['depart_location']}
**Distance:** {row['distance']}
**Duration:** {row['duration']}
**Land Agency:** {row['land_agency']}

"""

        if row.get('business_group') in ['1', '3', '4']:
            markdown_content += f"""## 📅 Calendar & Availability
**This is a multi-day tour with scheduled dates.**

**Available Dates:**
{row['available_dates']}

For multi-day tours in Business Groups 1, 3, and 4:
- These tours are scheduled on specific dates
- Availability depends on guide and equipment capacity
- Booking is required in advance
- Group sizes may be limited
"""
        else:
            markdown_content += f"""## 📅 Booking Information
**This is a day tour that may be bookable on demand.**
Day tours typically offer more flexible scheduling options.
"""

        # Save individual tour file in appropriate subdirectory
        tour_file_path = os.path.join(output_subdir, f"{tour_name}.md")
        with open(tour_file_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)

    print(f"Pipeline completed successfully!")
    print(f"- Processed data saved to: {csv_path}")
    print(f"- Data report generated: {report_path}")
    print(f"- Individual tour files: {base_output_dir}/")
    print(f"- Total tours processed: {len(df_processed)}")

if __name__ == "__main__":
    main()