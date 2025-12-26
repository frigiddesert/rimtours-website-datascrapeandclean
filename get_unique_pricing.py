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
    print("--- Fetching unique pricing levels by trip type (ignoring individual dates) ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Get all pricing levels for trip types (not individual instances)
    # This will give us the template pricing, not the date-specific instance pricing
    pricing_url = "https://rimtours.arcticres.com/api/rest/trip/pricinglevel"
    
    try:
        print("Fetching all pricing levels...")
        response = requests.get(pricing_url, auth=auth)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and 'entries' in data:
            total = data.get('total', 0)
            entries = data.get('entries', [])
            print(f"Found {len(entries)} pricing level entries out of total {total}")
            
            # Create a mapping from parentid to triptype names
            # We'll use the triptype endpoint to get names
            triptype_mapping = {}
            
            # Process entries and link to trip type names
            pricing_rows = []
            processed_combinations = set()  # Track unique (parentid, name, amount) combinations
            
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
                
                # Get trip type name by querying the API
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
                
                pricing_rows.append({
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
            
            # Export to CSV
            if pricing_rows:
                df = pd.DataFrame(pricing_rows)
                df.to_csv('arctic_unique_pricing.csv', index=False)
                print(f"Success! Saved {len(df)} unique pricing rows to 'arctic_unique_pricing.csv'")
                
                # Show summary
                print("\nSummary:")
                print(f"  - {len(df)} unique pricing combinations")
                print(f"  - Across {df['Trip_Type_ID'].nunique()} unique trip types")
                
                print("\nSample unique pricing data:")
                for i, row in df.head(10).iterrows():
                    print(f"  {row['Trip_Name'][:25]}... | {row['Price_Name'][:30]}... | ${row['Amount']}")
            else:
                print("No unique pricing data found.")
        else:
            print("Unexpected response format")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()