import requests
import pandas as pd
import datetime
import base64

# ==========================================
# CONFIGURATION
# ==========================================
API_USERNAME = "abwzxjtwhjlu"
API_PASSWORD = "DZgzeYb##BzEkazZqQr87isJ"

# We target 'trip' (instances) to get date-specific data
BASE_URL = "https://rimtours.arcticres.com/api/rest/trip"

def main():
    print("--- Fetching FUTURE Pricing (Instances) ---")
    
    auth = (API_USERNAME, API_PASSWORD)
    
    # 1. SET DATE FILTER
    # Arctic allows filtering by start date. 
    # We ask for trips starting from Tomorrow.
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    params = {
        "start": tomorrow,  # Filter: Only future trips
        "number": 500       # Changed "limit" to "number" as Arctic API uses "number" for pagination
    }
    
    print(f"Querying trips starting after: {tomorrow}...")
    
    try:
        response = requests.get(BASE_URL, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"API Error: {e}")
        return

    items = data if isinstance(data, list) else data.get('entries', [])
    print(f"Sampled {len(items)} future scheduled trips.")

    # 2. DEDUPLICATE (The "Cruncher" Logic)
    # We don't need 500 examples of the same price. We just need one per Tour Type.
    seen_prices = set() # Stores keys like "192_Standard"
    clean_rows = []

    for trip in items:
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
                
                clean_rows.append({
                    "Arctic_ID": triptype_id,
                    "Tour_Name": trip_name,
                    "Price_Name": p_name,
                    "Amount": p_amount,
                    "Sample_Date": trip.get('start') # Good for verifying it's 2025!
                })

    # 3. EXPORT
    if clean_rows:
        df = pd.DataFrame(clean_rows)
        # Sort by Name
        df = df.sort_values(by=['Tour_Name', 'Price_Name'])
        
        df.to_csv('arctic_pricing_original.csv', index=False)
        print(f"Success! Extracted {len(df)} unique future price points.")
        print(df.head())
    else:
        print("No future pricing found. (Are trips scheduled for 2025 yet?)")

if __name__ == "__main__":
    main()