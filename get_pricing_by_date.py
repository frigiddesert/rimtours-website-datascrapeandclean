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
    
    # Date range
    start_date = "2025-07-01"
    end_date = "2026-12-31"
    
    # Endpoint for trip instances
    trip_instances_url = "https://rimtours.arcticres.com/api/rest/trip"
    
    # Parameters for date filtering
    params = {
        "fromdate": start_date,
        "todate": end_date
    }
    
    try:
        print(f"Fetching trip instances from {start_date} to {end_date}...")
        response = requests.get(trip_instances_url, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and 'entries' in data:
            total = data.get('total', 0)
            entries = data.get('entries', [])
            print(f"Found {len(entries)} trip instances out of total {total} in date range")
            
            if total > 50:  # API might be paginated
                print(f"Warning: API returned only first 50 of {total} records. Pagination might be needed.")
                # For now, we'll work with the first 50, but we might need to implement pagination later
            
            pricing_rows = []
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
                    
                    pricing_rows.append({
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
            
            # Export to CSV
            if pricing_rows:
                df = pd.DataFrame(pricing_rows)
                df.to_csv('arctic_pricing_by_date.csv', index=False)
                print(f"Success! Saved {len(df)} pricing rows for date range to 'arctic_pricing_by_date.csv'")
                
                # Show summary
                print("\nSummary:")
                print(f"  - {len(df)} pricing records for {len(entries)} trip instances")
                print(f"  - Date range: {start_date} to {end_date}")
                print(f"  - Unique trip types: {df['Trip_Type_ID'].nunique()}")
                
                print("\nSample pricing data:")
                for i, row in df.head(3).iterrows():
                    print(f"  Trip: {row['Trip_Name'][:30]}... | Price: {row['Price_Name']} | Amount: ${row['Amount']}")
            else:
                print("No pricing data found in the date range.")
        else:
            print("Unexpected response format")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()