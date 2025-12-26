import requests
import pandas as pd
import json

# ==========================================
# CONFIGURATION
# ==========================================
# 1. Insert Credentials Here
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

def main():
    print("--- Fetching ALL unique pricing levels by trip type (ignoring individual dates) ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Get all pricing levels for trip types (not individual instances)
    pricing_url = "https://rimtours.arcticres.com/api/rest/trip/pricinglevel"
    
    all_pricing_rows = []
    processed_combinations = set()  # Track unique (parentid, name, amount) combinations
    
    # Create a mapping from parentid to triptype names to avoid duplicate lookups
    triptype_mapping = {}
    
    offset = 0
    batch_size = 50
    
    print("Fetching all pricing levels with pagination...")
    
    while True:
        params = {
            "start": offset,
            "number": batch_size
        }
        
        try:
            print(f"  Fetching batch starting at record {offset}...")
            response = requests.get(pricing_url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                total = data.get('total', 0)
                entries = data.get('entries', [])
                
                print(f"    Got {len(entries)} entries in this batch")
                
                if not entries:  # No more data
                    break
                
                batch_unique_count = 0
                for item in entries:
                    if not isinstance(item, dict):
                        continue
                    
                    parent_id = item.get('parentid')  # Links to triptype
                    price_id = item.get('id')
                    price_name = item.get('name', 'N/A')
                    price_amount = item.get('amount', 'N/A')
                    
                    # Only process unique combinations (to avoid duplicates)
                    combination_key = (parent_id, price_name, price_amount)
                    if combination_key in processed_combinations:
                        continue
                    processed_combinations.add(combination_key)
                    
                    # Get trip type name by querying the API (only if not already cached)
                    if parent_id and parent_id not in triptype_mapping:
                        try:
                            trip_url = f"https://rimtours.arcticres.com/api/rest/triptype/{parent_id}"
                            trip_response = requests.get(trip_url, auth=auth)
                            trip_response.raise_for_status()
                            trip_data = trip_response.json()
                            if isinstance(trip_data, dict):
                                triptype_mapping[parent_id] = trip_data.get('name', f"Trip ID {parent_id}")
                            else:
                                triptype_mapping[parent_id] = f"Trip ID {parent_id}"
                        except:
                            triptype_mapping[parent_id] = f"Trip ID {parent_id}"
                    
                    trip_name = triptype_mapping.get(parent_id, f"Trip ID {parent_id}")
                    
                    # Skip if it's marked as deleted
                    if item.get('deleted'):
                        continue
                    
                    all_pricing_rows.append({
                        "Trip_Type_ID": parent_id,
                        "Trip_Name": trip_name,
                        "Price_Level_ID": price_id,
                        "Price_Name": price_name,
                        "Amount": price_amount,
                        "Description": item.get('description', 'N/A'),
                        "Is_Default": item.get('default', 'N/A'),
                        "Show_Online": item.get('showonline', 'N/A'),
                        "Unique_Name": item.get('uniquename', 'N/A')
                    })
                    batch_unique_count += 1
                
                print(f"    Added {batch_unique_count} unique pricing records from this batch")
                
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
        df.to_csv('arctic_unique_pricing_FULL.csv', index=False)
        print(f"\nSuccess! Saved {len(df)} total unique pricing rows to 'arctic_unique_pricing_FULL.csv'")
        
        # Show summary
        print("\nSummary:")
        print(f"  - {len(df)} total unique pricing combinations")
        print(f"  - Across {df['Trip_Type_ID'].nunique()} unique trip types")
        
        print("\nSample unique pricing data:")
        for i, row in df.head(10).iterrows():
            print(f"  {row['Trip_Name'][:30]}... | {row['Price_Name'][:25]}... | ${row['Amount']}")
    else:
        print("No unique pricing data found.")

if __name__ == "__main__":
    main()