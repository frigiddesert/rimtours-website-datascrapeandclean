import requests
import pandas as pd
import json

# ==========================================
# CONFIGURATION
# ==========================================
# 1. Insert Credentials Here
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

# 2. Base URL (We target 'triptype' to get the definitions, not specific dates)
BASE_URL = "https://rimtours.arcticres.com/api/rest/triptype"

def main():
    print(f"--- Connecting to Arctic API: {BASE_URL} ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # We ask for the 'pricinglevels' sub-object explicitly
    # Note: Syntax might vary slightly depending on API version, 
    # but usually GET /triptype returns the full object or requires ?scope=pricinglevels
    params = {
        "scope": "pricinglevels" 
    }
    
    try:
        response = requests.get(BASE_URL, auth=auth, params=params)
        response.raise_for_status() # Check for 401/403/500 errors
        data = response.json()
        
    except Exception as e:
        print(f"API Error: {e}")
        return

    # Debug: print the structure first
    print("Response type:", type(data))
    print("First few items:", data[:3] if isinstance(data, list) else str(data)[:200])
    
    # Handle if Arctic returns a list directly or wraps it in 'response'/'d'
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and 'response' in data:
        items = data['response']
    elif isinstance(data, dict) and 'd' in data:  # Another common format
        items = data['d']
    elif isinstance(data, dict):
        # If it's just a single dict, wrap it in a list
        items = [data]
    else:
        # Fallback
        items = data

    print(f"Fetched {len(items) if isinstance(items, list) else 1} items. Parsing pricing...")

    if isinstance(items, list):
        print(f"First item type: {type(items[0]) if items else 'N/A'}")
        if items and isinstance(items[0], dict):
            print(f"First item keys: {list(items[0].keys()) if items else 'N/A'}")
    
    pricing_rows = []

    for item in items:
        if not isinstance(item, dict):
            print(f"Skipping non-dict item: {item}")
            continue
            
        # The ID here (e.g., 191) matches your arctic_triptype.csv ID
        triptype_id = str(item.get('id')) 
        triptype_name = item.get('name')
        
        # Extract Pricing Levels
        levels = item.get('pricinglevels', [])
        
        if not levels:
            # Debug: Check if keys imply pricing is elsewhere
            # print(f"No pricing found for {triptype_name}")
            continue
            
        for level in levels:
            # Skip deleted prices
            if level.get('deleted'): continue
            
            pricing_rows.append({
                "Arctic_ID": triptype_id,
                "Tour_Name": triptype_name,
                "Price_Name": level.get('name'),         # e.g. "Standard", "Deposit"
                "Amount": level.get('amount'),           # e.g. 1250.00
                "Description": level.get('description'),
                "Online_Link": item.get('onlinebookingurl') # Useful for the Wiki button!
            })

    # Export
    if pricing_rows:
        df = pd.DataFrame(pricing_rows)
        df.to_csv('arctic_pricing.csv', index=False)
        print(f"Success! Saved {len(df)} pricing rows to 'arctic_pricing.csv'")
    else:
        print("Warning: No pricing levels found. Check API permissions or Scope.")

if __name__ == "__main__":
    main()