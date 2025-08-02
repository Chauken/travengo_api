import pandas as pd
import pymongo
import json
import os
from bson import ObjectId
from pymongo import MongoClient
from tqdm import tqdm

# Custom JSON encoder to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MongoJSONEncoder, self).default(obj)

def create_airport_collection():
    """
    Read airports.csv, transform the data, and store in MongoDB.
    """
    print("Starting airport data import...")

    # Connect to MongoDB
    try:
        client = MongoClient('mongodb://192.168.1.57:27017/')
        db = client['travengo']
        airport_collection = db['airports']
        
        # Drop existing collection to start fresh
        airport_collection.drop()
        
        # Create index for faster queries (only for non-empty IATA codes)
        airport_collection.create_index([('iata_code', pymongo.ASCENDING)], unique=True, sparse=True)
        print("Connected to MongoDB successfully")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    try:
        # Read the CSV file
        df = pd.read_csv('csvs/airports.csv')
        print(f"Read {len(df)} records from CSV")

        # Clean and transform data
        airports = []
        skipped_count = 0
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing airports"):
            # Skip entries without IATA code or with NaN IATA code
            if pd.isna(row.get('iata_code')) or row.get('iata_code', '') == '':
                skipped_count += 1
                continue
                
            airport = {
                "iata_code": row.get('iata_code', ''),
                "icao_code": row.get('icao_code', ''),
                "name": row.get('name', ''),
                "type": row.get('type', ''),
                "municipality": row.get('municipality', ''),
                "country": row.get('iso_country', ''),
                "region": row.get('iso_region', ''),
                "continent": row.get('continent', ''),
                "location": {
                    "latitude": float(row.get('latitude_deg', 0)) if pd.notna(row.get('latitude_deg')) else 0.0,
                    "longitude": float(row.get('longitude_deg', 0)) if pd.notna(row.get('longitude_deg')) else 0.0
                },
                "elevation_ft": int(row.get('elevation_ft', 0)) if pd.notna(row.get('elevation_ft')) else None,
                "scheduled_service": True if row.get('scheduled_service', '').lower() == 'yes' else False,
                "gps_code": row.get('gps_code', ''),
                "local_code": row.get('local_code', '')
            }
            
            # Clean up the airport object by removing None and empty values
            airport = {k: v for k, v in airport.items() if v not in [None, '']}
            
            # Example code for fetching city data based on coordinates
            # This would require a geocoding library like geopy
            # Note: This is commented out as it would require API keys and have rate limits
            # from geopy.geocoders import Nominatim
            # 
            # def get_city_from_coordinates(lat, lng):
            #     try:
            #         geolocator = Nominatim(user_agent="travengo-app")
            #         location = geolocator.reverse(f"{lat}, {lng}")
            #         if location and location.raw.get('address'):
            #             address = location.raw['address']
            #             city_name = address.get('city', address.get('town', address.get('village', '')))
            #             return {"name": city_name} if city_name else None
            #     except Exception as e:
            #         print(f"Geocoding error: {e}")
            #     return None
            # 
            # if pd.notna(row.get('latitude_deg')) and pd.notna(row.get('longitude_deg')):
            #     city_data = get_city_from_coordinates(row['latitude_deg'], row['longitude_deg'])
            #     if city_data:
            #         airport['city'] = city_data
            
            airports.append(airport)

        # Insert into MongoDB
        if airports:
            # Create models directory if it doesn't exist
            os.makedirs('models', exist_ok=True)
            
            # Insert data in batches to avoid potential memory issues
            batch_size = 1000
            total_imported = 0
            
            for i in range(0, len(airports), batch_size):
                batch = airports[i:i+batch_size]
                result = airport_collection.insert_many(batch)
                total_imported += len(result.inserted_ids)
                print(f"Imported batch {i//batch_size + 1}/{(len(airports)-1)//batch_size + 1} ({total_imported}/{len(airports)} airports)")
            
            print(f"Successfully imported {total_imported} airports to MongoDB (skipped {skipped_count} without IATA code)")
            
            # Save a subset of airports to JSON (first 1000) to keep file size reasonable
            # Get a fresh copy from the database to include the _id fields
            sample_airports = list(airport_collection.find().limit(1000))
            
            # Create models directory if it doesn't exist
            os.makedirs('models', exist_ok=True)
            
            with open('models/airports.json', 'w') as f:
                json.dump({"airports": sample_airports}, f, indent=2, cls=MongoJSONEncoder)
            print(f"Sample of {len(sample_airports)} airports also saved to models/airports.json")
        else:
            print("No airports to import")
    
    except Exception as e:
        print(f"Error during import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_airport_collection()
    print("Airport data import completed!")