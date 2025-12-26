import requests
import pandas as pd
import datetime
import base64
import os
import json

# ==========================================
# CONFIGURATION
# ==========================================
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

# We target 'trip' (instances) to get date-specific data
BASE_URL = "https://rimtours.arcticres.com/api/rest/trip"

def get_last_processed_offset():
    """Get the last processed offset from a checkpoint file"""
    checkpoint_file = 'future_pricing_checkpoint.txt'
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            try:
                return int(f.read().strip())
            except:
                return 0
    return 0

def save_checkpoint(offset):
    """Save the current offset to a checkpoint file"""
    with open('future_pricing_checkpoint.txt', 'w') as f:
        f.write(str(offset))

def main():
    print("--- Fetching FUTURE Pricing (Instances) with Resume Capability ---")
    
    auth = (API_USERNAME, API_PASSWORD)
    
    # 1. SET DATE FILTER
    # Arctic allows filtering by start date. 
    # We ask for trips starting from Tomorrow.
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    params = {
        "start": tomorrow,  # Filter: Only future trips
    }
    
    print(f"Querying trips starting after: {tomorrow}...")
    
    # Initialize CSV file with headers if it doesn't exist
    output_file = 'arctic_pricing.csv'
    headers_written = os.path.exists(output_file)
    
    # Get the starting offset from checkpoint
    start_offset = get_last_processed_offset()
    print(f"Starting from offset: {start_offset}")
    
    # Track seen prices to avoid duplicates
    seen_prices = set()  # Stores keys like "192_Standard"
    offset = start_offset
    batch_size = 50
    
    while True:
        # Add pagination parameters 
        req_params = params.copy()
        req_params["start"] = offset
        req_params["number"] = batch_size
        
        try:
            print(f"  Fetching batch starting at record {offset}...")
            response = requests.get(BASE_URL, auth=auth, params=req_params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                total = data.get('total', 0)
                entries = data.get('entries', [])
                
                print(f"    Got {len(entries)} entries in this batch")
                
                if not entries:  # No more data
                    print("    No more entries to process, exiting...")
                    break
                
                batch_rows = []  # Store this batch's data in memory before writing
                for trip in entries:
                    # Trip Type ID (e.g., 192 for White Rim)
                    # Note: In 'trip' endpoint, this might be 'triptypeid' or nested in 'triptype'
                    triptype_id = str(trip.get('triptypeid'))
                    trip_name = trip.get('name')
                    
                    levels = trip.get('pricinglevels', [])
                    
                    for level in levels:
                        p_name = level.get('name')
                        p_amount = level.get('amount')
                        
                        # Unique Key: TripType + PriceName
                        # e.g. "192_Standard"
                        unique_key = f"{triptype_id}_{p_name}"
                        
                        if unique_key not in seen_prices:
                            seen_prices.add(unique_key)

                            batch_rows.append({
                                "Arctic_ID": triptype_id,
                                "Tour_Name": trip_name,
                                "Price_Name": p_name,
                                "Amount": p_amount,
                                "Sample_Date": trip.get('start')  # Good for verifying it's future!
                            })

                print(f"    Processing {len(batch_rows)} unique pricing records from this batch")
                
                # Write this batch to CSV if we have new unique records
                if batch_rows:
                    df_batch = pd.DataFrame(batch_rows)
                    mode = 'a' if os.path.exists(output_file) else 'w'
                    header = not os.path.exists(output_file)  # Write header only for first batch
                    df_batch.to_csv(output_file, mode=mode, header=header, index=False)
                    print(f"    Saved {len(batch_rows)} unique records to CSV")
                
                # Update checkpoint
                save_checkpoint(offset + batch_size)
                
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
            print(f"  Checkpoint saved at offset {offset}. Process can be resumed.")
            break
    
    # Final summary
    if os.path.exists(output_file):
        df = pd.read_csv(output_file)
        # Sort by Name
        df = df.sort_values(by=['Tour_Name', 'Price_Name'])
        
        # Rewrite the entire file with sorted data
        df.to_csv(output_file, index=False)
        
        print(f"\nFinal result: Saved {len(df)} unique future price points to 'arctic_pricing.csv'")
        print(f"Total unique price combinations found: {len(seen_prices)}")
        print("\nFirst 10 rows:")
        print(df.head(10))
    else:
        print("No future pricing found. (Are trips scheduled for 2025 yet?)")

if __name__ == "__main__":
    main()