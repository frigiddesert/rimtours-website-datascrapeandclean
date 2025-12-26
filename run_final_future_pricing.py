import requests
import pandas as pd
import datetime
import os

# ==========================================
# CONFIGURATION
# ==========================================
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

# We target 'trip' (instances) to get date-specific data
BASE_URL = "https://rimtours.arcticres.com/api/rest/trip"

def main():
    print("--- Fetching FUTURE Pricing (Instances) with Proper Pagination ---")
    
    auth = (API_USERNAME, API_PASSWORD)
    
    # 1. SET DATE FILTER
    # Arctic allows filtering by start date. 
    # We ask for trips starting from Tomorrow.
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"Querying trips starting after: {tomorrow}...")
    
    # Initialize variables for pagination
    offset = 0
    batch_size = 50  # Arctic API typically uses 50 as batch size
    all_items = []
    seen_prices = set()  # Stores keys like "192_Standard"
    clean_rows = []

    while True:
        params = {
            "start": tomorrow,  # Filter: Only future trips
            "fromdate": tomorrow,  # Alternative parameter name that might work
            "start": offset,
            "number": batch_size
        }
        
        try:
            print(f"  Fetching batch at offset {offset}...")
            response = requests.get(BASE_URL, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                entries = data.get('entries', [])
                total = data.get('total', 0)
                
                print(f"    Got {len(entries)} entries in this batch")
                
                if not entries:  # No more data
                    break
                
                batch_unique_count = 0
                for trip in entries:
                    # Trip Type ID (e.g., 192 for White Rim)
                    triptype_id = str(trip.get('triptypeid'))
                    trip_name = trip.get('name')
                    
                    # Check if trip date is in the future (safety check)
                    trip_start = trip.get('start', '')
                    if trip_start and isinstance(trip_start, str) and trip_start.startswith('20'):
                        trip_year = int(trip_start[:4])
                        current_year = datetime.date.today().year
                        if trip_year < current_year:
                            # Skip if this trip is in the past (despite our filter)
                            continue
                    
                    levels = trip.get('pricinglevels', [])
                    
                    for level in levels:
                        p_name = level.get('name')
                        p_amount = level.get('amount')
                        
                        # Unique Key: TripType + PriceName
                        # e.g. "192_Standard"
                        unique_key = f"{triptype_id}_{p_name}"
                        
                        if unique_key not in seen_prices:
                            seen_prices.add(unique_key)
                            
                            clean_rows.append({
                                "Arctic_ID": triptype_id,
                                "Tour_Name": trip_name,
                                "Price_Name": p_name,
                                "Amount": p_amount,
                                "Sample_Date": trip.get('start') # Good for verifying it's in the future!
                            })
                            batch_unique_count += 1

                print(f"    Added {batch_unique_count} new unique pricing records from this batch")
                
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

    # 3. EXPORT
    if clean_rows:
        df = pd.DataFrame(clean_rows)
        # Sort by Name
        df = df.sort_values(by=['Tour_Name', 'Price_Name'])
        
        df.to_csv('arctic_pricing_final.csv', index=False)
        print(f"Success! Extracted {len(df)} unique future price points.")
        print(f"Total unique price combinations found: {len(seen_prices)}")
        
        print("\nFirst 10 rows:")
        print(df.head(10))
        
        print(f"\nFuture dates breakdown:")
        future_dates = df[df['Sample_Date'].apply(lambda x: str(x).startswith('2025') or str(x).startswith('2026') or str(x).startswith('2027'))]
        print(f"Found {len(future_dates)} records with dates in 2025-2027")
    else:
        print("No future pricing found. (Are trips scheduled for 2025 yet?)")

if __name__ == "__main__":
    main()