import requests
import pandas as pd
import json

# ==========================================
# CONFIGURATION
# ==========================================
# 1. Insert Credentials Here
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

# 2. Base URL - Using the correct endpoint for pricing levels
BASE_URL = "https://rimtours.arcticres.com/api/rest/trip/pricinglevel"

def main():
    print(f"--- Connecting to Arctic API: {BASE_URL} ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    try:
        response = requests.get(BASE_URL, auth=auth)
        response.raise_for_status() # Check for 401/403/500 errors
        data = response.json()
        
    except Exception as e:
        print(f"API Error: {e}")
        return

    # Handle response structure
    if isinstance(data, dict) and 'entries' in data:
        items = data['entries']
        print(f"Found {len(items)} pricing level entries via 'entries' key")
    elif isinstance(data, list):
        items = data
        print(f"Found {len(items)} pricing level entries in list")
    elif isinstance(data, dict):
        # If it's a single item, wrap it in a list
        items = [data]
        print(f"Found 1 pricing level entry as dict")
    else:
        items = []
        print("No pricing level entries found")

    print(f"Sample keys from first pricing item: {list(items[0].keys()) if items and isinstance(items[0], dict) else 'N/A'}")

    pricing_rows = []
    for item in items:
        if isinstance(item, dict):
            # Try to link back to trip type by getting the parent ID
            parent_id = item.get('parentid')  # This should link to the triptype ID
            
            # Look up the trip name based on parent ID by fetching the trip type info
            trip_name = "Unknown"
            if parent_id:
                # Try to get the trip type information
                try:
                    trip_url = f"https://rimtours.arcticres.com/api/rest/triptype/{parent_id}"
                    trip_response = requests.get(trip_url, auth=auth)
                    trip_response.raise_for_status()
                    trip_data = trip_response.json()
                    if isinstance(trip_data, dict):
                        trip_name = trip_data.get('name', f"Trip ID {parent_id}")
                except:
                    trip_name = f"Trip ID {parent_id}"  # Fallback if we can't get the name
            
            pricing_rows.append({
                "Price_ID": item.get('id'),
                "Arctic_ID": parent_id,  # Links to the triptype
                "Tour_Name": trip_name,
                "Price_Name": item.get('name', 'N/A'),  # e.g. "Standard", "Deposit"
                "Amount": item.get('amount', 'N/A'),    # e.g. 1250.00
                "Description": item.get('description', 'N/A'),
                "Is_Default": item.get('default', 'N/A'),
                "Show_Online": item.get('showonline', 'N/A'),
                "Unique_Name": item.get('uniquename', 'N/A')
            })

    # Export if we found anything
    if pricing_rows:
        df = pd.DataFrame(pricing_rows)
        df.to_csv('arctic_pricing.csv', index=False)
        print(f"Success! Saved {len(df)} pricing rows to 'arctic_pricing.csv'")
        
        # Show a sample of what we found
        print("\nSample of pricing data:")
        for i, row in df.head(3).iterrows():
            print(f"  Tour: {row['Tour_Name'][:30]}... | Price: {row['Price_Name']} | Amount: {row['Amount']}")
    else:
        print("No pricing data found.")

if __name__ == "__main__":
    main()