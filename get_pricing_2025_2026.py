import requests
import pandas as pd
import json
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
# 1. Insert Credentials Here
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

def main():
    print("--- Fetching pricing for trips in date range 7/1/2025 to 12/31/2026 ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Endpoint for trip instances (these have the actual dates and associated pricing)
    trip_instances_url = "https://rimtours.arcticres.com/api/rest/trip"
    
    # Parameters for date filtering - try to get future dates
    # Since we want 2025-2026, we can try to limit to a reasonable future range
    start_date = "2025-07-01" 
    end_date = "2026-12-31"
    
    params = {
        "fromdate": start_date,
        "todate": end_date
    }
    
    all_pricing_rows = []
    offset = 0
    batch_size = 50  # Most APIs default to this
    
    print(f"Fetching trip instances from {start_date} to {end_date} with pagination...")
    
    while True:
        # Add pagination parameters 
        req_params = params.copy()
        req_params["start"] = offset
        req_params["number"] = batch_size
        
        try:
            print(f"  Fetching batch starting at record {offset}...")
            response = requests.get(trip_instances_url, auth=auth, params=req_params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                total = data.get('total', 0)
                entries = data.get('entries', [])
                
                print(f"    Got {len(entries)} entries in this batch (total should be in range)")
                
                if not entries:  # No more data
                    break
                
                batch_pricing_count = 0
                for trip in entries:
                    if not isinstance(trip, dict):
                        continue
                    
                    # Get trip instance info
                    trip_instance_id = trip.get('id')
                    trip_type_id = trip.get('triptypeid')
                    trip_name = trip.get('name', f"Trip Type ID {trip_type_id}")
                    
                    # Get the trip date to track year
                    # Look for date fields in the trip instance
                    trip_date = None
                    for date_field in ['start', 'starttime', 'date', 'modifiedon', 'createdon']:
                        if date_field in trip and trip[date_field]:
                            trip_date = trip[date_field]
                            break
                    
                    # Extract the year from the date
                    trip_year = 'Unknown'
                    if trip_date and trip_date != 'N/A':
                        try:
                            # Parse the date string to extract the year
                            # Arctic API dates may be in different formats
                            if 'T' in str(trip_date):  # ISO format like "2025-07-15T08:00:00-06:00"
                                trip_year = str(trip_date)[:4]
                            elif '/' in str(trip_date):  # Format like "2025-07-15" or "07/15/2025"
                                if str(trip_date).startswith('20'):  # "2025-07-15"
                                    trip_year = str(trip_date)[:4]
                                else:  # "07/15/2025"
                                    trip_year = str(trip_date).split('/')[-1].split('T')[0]
                            else:
                                # Try to extract 4-digit year from string
                                import re
                                year_match = re.search(r'\b(20\d{2})\b', str(trip_date))
                                if year_match:
                                    trip_year = year_match.group(1)
                        except:
                            trip_year = 'Unknown'
                    
                    # Only process if year is 2025 or 2026 (or unknown - we'll keep for now)
                    if trip_year not in ['2025', '2026', 'Unknown']:
                        continue
                    
                    # Get pricing levels for this trip instance
                    levels = trip.get('pricinglevels', [])
                    
                    if not levels:
                        continue  # Skip if no pricing levels
                    
                    for level in levels:
                        if not isinstance(level, dict):
                            continue
                        
                        # Skip deleted prices
                        if level.get('deleted'): 
                            continue
                        
                        all_pricing_rows.append({
                            "Trip_Instance_ID": trip_instance_id,
                            "Trip_Type_ID": trip_type_id,
                            "Trip_Name": trip_name,
                            "Trip_Year": trip_year,
                            "Price_Level_ID": level.get('id'),
                            "Price_Name": level.get('name', 'N/A'),  # e.g. "Standard", "Deposit"
                            "Amount": level.get('amount', 'N/A'),    # e.g. 1250.00
                            "Description": level.get('description', 'N/A'),
                            "Is_Default": level.get('default', 'N/A'),
                            "Show_Online": level.get('showonline', 'N/A'),
                            "Unique_Name": level.get('uniquename', 'N/A')
                        })
                        batch_pricing_count += 1
                
                print(f"    Added {batch_pricing_count} pricing records from this batch")
                
                # Check if we've reached the end
                if len(entries) < batch_size:
                    print(f"    Reached end of results (got {len(entries)} < {batch_size})")
                    break
                
                offset += batch_size
                
                # Safety check to avoid infinite loops
                if offset > total:
                    print(f"    Safety break: offset ({offset}) > total ({total})")
                    break
                    
        except Exception as e:
            print(f"  Error in batch at offset {offset}: {e}")
            break
    
    # Export to CSV
    if all_pricing_rows:
        df = pd.DataFrame(all_pricing_rows)
        df.to_csv('arctic_pricing_2025_2026.csv', index=False)
        print(f"\nSuccess! Saved {len(df)} pricing rows for 2025-2026 to 'arctic_pricing_2025_2026.csv'")
        
        # Show summary by year to identify changes
        print("\nSummary by Year:")
        if 'Trip_Year' in df.columns:
            year_summary = df.groupby('Trip_Year').size()
            for year, count in year_summary.items():
                print(f"  {year}: {count} pricing records")
        
        print(f"\nTotal records: {len(df)}")
        print(f"  Unique trip types: {df['Trip_Type_ID'].nunique()}")
        print(f"  Unique pricing combinations: {df.groupby(['Trip_Type_ID', 'Price_Name', 'Amount']).size().shape[0]}")
        
        print("\nSample pricing data:")
        for i, row in df.head(10).iterrows():
            print(f"  {row['Trip_Year']} | {row['Trip_Name'][:20]}... | {row['Price_Name'][:20]}... | ${row['Amount']}")
        
        # Show pricing changes between years if applicable
        if 'Trip_Year' in df.columns:
            print("\nIdentifying potential pricing changes between years...")
            # Group by trip type, price name, and year to see changes
            price_changes = df.groupby(['Trip_Type_ID', 'Price_Name', 'Trip_Year'])['Amount'].first().unstack(fill_value=None)
            if len(price_changes.columns) > 1:
                print("Trip Type | Price Name |", " | ".join(price_changes.columns))
                for idx, row in price_changes.iterrows():
                    trip_type_id, price_name = idx
                    if len([x for x in row if pd.notna(x)]) > 1:  # Has values in multiple years
                        values = " | ".join([str(x) if pd.notna(x) else "-" for x in row])
                        print(f"{trip_type_id} | {price_name} | {values}")
    else:
        print("No pricing data found for the specified date range.")

if __name__ == "__main__":
    main()