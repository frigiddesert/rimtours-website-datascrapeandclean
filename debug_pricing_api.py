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
    print("--- Connecting to Arctic API to get pricing data ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Try getting pricing levels directly from a different endpoint
    pricing_url = "https://rimtours.arcticres.com/api/rest/pricinglevel"
    
    try:
        response = requests.get(pricing_url, auth=auth)
        response.raise_for_status() # Check for 401/403/500 errors
        data = response.json()
        
        print(f"Got response from pricinglevel endpoint: {type(data)}")
        
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
        
        if items:
            print(f"Sample keys from first pricing item: {list(items[0].keys()) if items and isinstance(items[0], dict) else 'N/A'}")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error from pricinglevel endpoint: {e}")
        print("This endpoint might not be accessible or might have a different structure.")
        
        # If that doesn't work, try getting individual trip type with its pricing
        print("Trying to get a single trip type with pricing details directly...")
        single_trip_url = "https://rimtours.arcticres.com/api/rest/triptype/10"  # Using ID 10 from our earlier debug
        
        try:
            response = requests.get(single_trip_url, auth=auth)
            response.raise_for_status()
            single_trip = response.json()
            
            print(f"Single trip type structure: {type(single_trip)}")
            if isinstance(single_trip, dict):
                print(f"Keys for trip ID 10: {list(single_trip.keys())}")
                if 'pricinglevels' in single_trip:
                    print(f"Found {len(single_trip['pricinglevels'])} pricing levels in single trip")
        except Exception as e2:
            print(f"Error getting single trip type: {e2}")
            print("Let's try to see if there are other related endpoints")
            
            # Try to find pricing information by looking at multiple trip types
            print("Trying to get first 3 trip types individually to see if any contain pricing info...")
            for trip_id in [10, 29, 34]:  # IDs from our earlier debug
                try:
                    trip_url = f"https://rimtours.arcticres.com/api/rest/triptype/{trip_id}"
                    trip_response = requests.get(trip_url, auth=auth)
                    trip_response.raise_for_status()
                    trip_data = trip_response.json()
                    
                    print(f"Trip ID {trip_id}: {type(trip_data)}")
                    if isinstance(trip_data, dict):
                        has_pricing = 'pricinglevels' in trip_data
                        print(f"  Has pricinglevels: {has_pricing}")
                        if has_pricing and trip_data['pricinglevels']:
                            print(f"  Contains {len(trip_data['pricinglevels'])} pricing levels")
                        
                        # Look for any pricing-related keys
                        pricing_keys = [k for k in trip_data.keys() if any(word in k.lower() for word in ['price', 'level', 'rate', 'cost', 'amount', 'fee'])]
                        if pricing_keys:
                            print(f"  Potential pricing-related keys: {pricing_keys}")
                except Exception as e3:
                    print(f"  Error getting trip ID {trip_id}: {e3}")
                    continue

    # If we have some pricing data from any approach, let's save it
    pricing_rows = []
    if 'items' in locals() and items:
        for item in items:
            if isinstance(item, dict):
                pricing_rows.append({
                    "Price_ID": item.get('id'),
                    "Price_Name": item.get('name'),
                    "Amount": item.get('amount'),
                    "Description": item.get('description'),
                    "Trip_Type_ID": item.get('triptypeid')  # Common field linking to triptype
                })

    # Export if we found anything
    if pricing_rows:
        df = pd.DataFrame(pricing_rows)
        df.to_csv('arctic_pricing.csv', index=False)
        print(f"Success! Saved {len(df)} pricing rows to 'arctic_pricing.csv'")
    else:
        print("Still no pricing data found. Need to research the correct API endpoint structure.")

if __name__ == "__main__":
    main()