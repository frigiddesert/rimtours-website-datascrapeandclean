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
    
    # Try with scope parameter
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

    # Arctic API returns data in the format:
    # {'start': 0, 'page': 0, 'number': 50, 'total': 127, 'entries': [ ... ]}
    if isinstance(data, dict) and 'entries' in data:
        items = data['entries']
    elif isinstance(data, list):
        items = data
    else:
        # Fallback
        items = data

    print(f"Fetched {len(items)} Trip Types. Parsing pricing...")

    # Debugging: Let's look at a few entries to see if pricinglevels are there
    for i, item in enumerate(items[:3]):  # Just look at first 3
        print(f"Entry {i+1}: ID={item.get('id')}, Name={item.get('name')}")
        print(f"  Keys available: {list(item.keys())}")
        if 'pricinglevels' in item:
            print(f"  Found pricinglevels: {len(item['pricinglevels'])} entries")
        else:
            print(f"  No 'pricinglevels' key found. Other related keys: {[k for k in item.keys() if 'price' in k.lower() or 'level' in k.lower()]}")
        print()

    # Let's also try without the scope parameter to see if that makes a difference
    print("=== Trying WITHOUT scope parameter ===")
    try:
        response2 = requests.get(BASE_URL, auth=auth)  # No scope parameter
        response2.raise_for_status()
        data2 = response2.json()
    except Exception as e:
        print(f"API Error without scope: {e}")
        return

    if isinstance(data2, dict) and 'entries' in data2:
        items2 = data2['entries']
    else:
        items2 = data2

    print(f"Without scope: Fetched {len(items2)} Trip Types")
    
    for i, item in enumerate(items2[:3]):  # Just look at first 3
        print(f"Entry {i+1}: ID={item.get('id')}, Name={item.get('name')}")
        print(f"  Keys available: {list(item.keys())}")
        if 'pricinglevels' in item:
            print(f"  Found pricinglevels: {len(item['pricinglevels'])} entries")
        else:
            print(f"  No 'pricinglevels' key found. Other related keys: {[k for k in item.keys() if 'price' in k.lower() or 'level' in k.lower()]}")
        print()

if __name__ == "__main__":
    main()