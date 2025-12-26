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
    print("--- Investigating Arctic API date filtering options ---")
    
    # Arctic uses Basic Auth
    auth = (API_USERNAME, API_PASSWORD)
    
    # Let's first look at a broader endpoint to understand the structure
    # Try getting trip instances (not just types) which should have dates
    trip_instances_url = "https://rimtours.arcticres.com/api/rest/trip"
    
    # Try with date filters
    params_options = [
        {"start": "2025-07-01", "end": "2026-12-31"},
        {"fromdate": "2025-07-01", "todate": "2026-12-31"},
        {"date_from": "2025-07-01", "date_to": "2026-12-31"},
        {"startDate": "2025-07-01", "endDate": "2026-12-31"},
        {"start_date": "2025-07-01", "end_date": "2026-12-31"},
        {"datefilter": "2025-07-01,2026-12-31"}
    ]
    
    print("Testing trip instances endpoint with date filters...")
    for i, params in enumerate(params_options):
        print(f"\nTesting params option {i+1}: {params}")
        try:
            response = requests.get(trip_instances_url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and 'entries' in data:
                total = data.get('total', 'unknown')
                entries = data.get('entries', [])
                print(f"  Got {len(entries)} entries out of total {total}")
                
                if entries and isinstance(entries[0], dict):
                    keys = list(entries[0].keys())
                    date_keys = [k for k in keys if 'date' in k.lower() or 'time' in k.lower() or 'start' in k.lower() or 'end' in k.lower()]
                    print(f"  Available date-related keys: {date_keys}")
                    
                    # Show first entry structure
                    first_entry = entries[0]
                    start_date = next((first_entry.get(k) for k in keys if 'start' in k.lower()), 'N/A')
                    end_date = next((first_entry.get(k) for k in keys if 'end' in k.lower()), 'N/A')
                    date = next((first_entry.get(k) for k in keys if 'date' in k.lower()), 'N/A')
                    print(f"  Sample dates - Start: {start_date}, End: {end_date}, Date: {date}")
                    
                    # Check if this contains pricing levels
                    if 'pricinglevels' in first_entry or 'prices' in first_entry:
                        print("  Contains pricing information!")
                        break
            else:
                print(f"  Response format: {type(data)}")
                
        except Exception as e:
            print(f"  Error with params {params}: {e}")

    print("\n--- Testing specific date range for pricing ---")
    print("Since pricing levels might be tied to trip instances,")
    print("we may need to first get trips in our date range, then fetch pricing for those trips.")

    # Try getting trips in our target date range
    params = {
        "fromdate": "2025-07-01",
        "todate": "2026-12-31"
    }
    
    try:
        response = requests.get(trip_instances_url, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and 'entries' in data:
            total = data.get('total', 'unknown')
            entries = data.get('entries', [])
            print(f"\nGot {len(entries)} trip instances out of total {total} in date range")
            
            if entries and isinstance(entries[0], dict):
                first_trip = entries[0]
                print(f"Sample trip keys: {list(first_trip.keys())[:10]}...")  # First 10 keys
                
                # Look for fields that might help identify the trip type
                triptype_id = first_trip.get('triptypeid') or first_trip.get('tripTypeId') or first_trip.get('typeId')
                print(f"Sample trip type ID: {triptype_id}")
                
                # Look for pricing-related fields
                pricing_fields = [k for k in first_trip.keys() if 'price' in k.lower() or 'level' in k.lower() or 'cost' in k.lower()]
                print(f"Pricing-related fields: {pricing_fields}")
                
        else:
            print("No entries found in the date range")
            
    except Exception as e:
        print(f"Error getting trips in date range: {e}")
        print("The date filtering might work differently, or the parameter names could be different.")

if __name__ == "__main__":
    main()