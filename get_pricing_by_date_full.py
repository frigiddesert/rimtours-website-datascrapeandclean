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
    print("--- Fetching ALL pricing for trips in date range 7/1/2025 to 12/31/2026 ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Date range
    start_date = "2025-07-01"
    end_date = "2026-12-31"
    
    # Endpoint for trip instances
    trip_instances_url = "https://rimtours.arcticres.com/api/rest/trip"
    
    # Parameters for date filtering
    base_params = {
        "fromdate": start_date,
        "todate": end_date
    }
    
    all_pricing_rows = []
    offset = 0
    batch_size = 50  # Most APIs default to a batch size like this
    
    print(f"Fetching trip instances from {start_date} to {end_date} with pagination...")
    
    while True:
        # Add pagination parameters 
        params = base_params.copy()
        params["start"] = offset
        params["number"] = batch_size
        
        try:
            print(f"  Fetching batch starting at record {offset}...")
            response = requests.get(trip_instances_url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                total = data.get('total', 0)
                entries = data.get('entries', [])
                
                print(f"    Got {len(entries)} entries in this batch")
                
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
        df.to_csv('arctic_pricing_by_date_FULL.csv', index=False)
        print(f"\nSuccess! Saved {len(df)} total pricing rows for date range to 'arctic_pricing_by_date_FULL.csv'")
        
        # Show summary
        print("\nSummary:")
        print(f"  - {len(df)} total pricing records")
        print(f"  - Date range: {start_date} to {end_date}")
        print(f"  - Unique trip instances: {df['Trip_Instance_ID'].nunique()}")
        print(f"  - Unique trip types: {df['Trip_Type_ID'].nunique()}")
        
        print("\nSample pricing data:")
        for i, row in df.head(5).iterrows():
            print(f"  {row['Trip_Name'][:25]}... | {row['Price_Name'][:25]}... | ${row['Amount']}")
    else:
        print("No pricing data found in the date range.")

if __name__ == "__main__":
    main()