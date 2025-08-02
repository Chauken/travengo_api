import pandas as pd
import pymongo
import json
import os
import random
from bson import ObjectId
from pymongo import MongoClient
from tqdm import tqdm

# Custom JSON encoder to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MongoJSONEncoder, self).default(obj)

def generate_price_from_rating(rating):
    """
    Generate a fake price based on hotel rating.
    """
    base_prices = {
        'OneStar': 50,
        'TwoStar': 90,
        'ThreeStar': 150,
        'FourStar': 250,
        'FiveStar': 400
    }
    
    # Get base price from rating or use default
    base_price = base_prices.get(rating, 100)
    
    # Add some randomness (±20%)
    variation = random.uniform(0.8, 1.2)
    price = round(base_price * variation)
    
    return price

def create_hotel_collection(limit_rows=None):
    """
    Read hotels.csv, transform the data, and store in MongoDB.
    
    Args:
        limit_rows (int, optional): Limit to first N rows for testing. None means process all rows.
    """
    print("Starting hotel data import...")

    # Connect to MongoDB
    try:
        client = MongoClient('mongodb://192.168.1.57:27017/')
        db = client['travengo']
        hotel_collection = db['hotels']
        
        # Drop existing collection to start fresh
        hotel_collection.drop()
        
        # Create index for faster queries - use sparse=True to avoid indexing null values
        hotel_collection.create_index([('hotel_code', pymongo.ASCENDING)], unique=True, sparse=True)
        print("Connected to MongoDB successfully")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    try:
        # Read the CSV file with Latin-1 encoding which is more permissive
        if limit_rows:
            df = pd.read_csv('csvs/hotels.csv', encoding='latin1', nrows=limit_rows)
            print(f"Limited to first {limit_rows} rows for testing")
        else:
            df = pd.read_csv('csvs/hotels.csv', encoding='latin1')
        print(f"Read {len(df)} records from CSV")
        
        # Trim whitespace from column names
        df.columns = [col.strip() for col in df.columns]
        
        # Print column names for debugging
        print("CSV columns:", list(df.columns))

        # Clean and transform data
        hotels = []
        skipped_count = 0
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing hotels"):
            # Skip entries without HotelCode
            hotel_code = row.get('HotelCode')
            if pd.isna(hotel_code) or hotel_code is None or str(hotel_code).strip() == '':
                skipped_count += 1
                continue
            
            # Generate price based on rating
            hotel_rating = row.get('HotelRating', '')
            price = generate_price_from_rating(hotel_rating)
            
            # Parse location from Map column (format: "latitude|longitude")
            location = {"latitude": 0.0, "longitude": 0.0}
            map_data = row.get('Map', '')
            if map_data and isinstance(map_data, str) and '|' in map_data:
                try:
                    lat, lng = map_data.split('|')
                    location = {
                        "latitude": float(lat.strip()),
                        "longitude": float(lng.strip())
                    }
                except (ValueError, TypeError):
                    # If conversion fails, keep default values
                    pass
            
            hotel = {
                "hotel_code": row.get('HotelCode', ''),
                "name": row.get('HotelName', ''),
                "rating": row.get('HotelRating', ''),
                "country": {
                    "code": row.get('countyCode', ''),
                    "name": row.get('countyName', '')
                },
                "city": {
                    "code": row.get('cityCode', ''),
                    "name": row.get('cityName', '')
                },
                "address": row.get('Address', ''),
                "attractions": row.get('Attractions', ''),
                "description": row.get('Description', ''),
                "facilities": row.get('HotelFacilities', ''),
                "contact": {
                    "phone": row.get('PhoneNumber', ''),
                    "fax": row.get('FaxNumber', ''),
                    "website": row.get('HotelWebsiteUrl', ''),
                    "pincode": row.get('PinCode', '')
                },
                "price": {
                    "base_price": price,
                    "currency": "USD"
                },
                "location": location
            }
            
            # Clean up the hotel object by removing None and empty values
            hotel = {k: v for k, v in hotel.items() if v not in [None, '']}
            
            # Clean nested dictionaries
            for key in list(hotel.keys()):
                if isinstance(hotel[key], dict):
                    hotel[key] = {k: v for k, v in hotel[key].items() if v not in [None, '']}
                    if not hotel[key]:  # Remove empty dictionaries
                        del hotel[key]
            
            hotels.append(hotel)

        # Print diagnostic information
        print(f"Processed {len(df)} records, found {len(hotels)} valid hotels, skipped {skipped_count}")
        
        # Insert into MongoDB
        if hotels:
            # Create models directory if it doesn't exist
            os.makedirs('models', exist_ok=True)
            
            # Insert data in batches to avoid potential memory issues
            batch_size = 500
            total_imported = 0
            
            for i in range(0, len(hotels), batch_size):
                try:
                    batch = hotels[i:i+batch_size]
                    result = hotel_collection.insert_many(batch, ordered=False)
                    total_imported += len(result.inserted_ids)
                except pymongo.errors.BulkWriteError as bwe:
                    # Handle the case where some documents were inserted successfully
                    if hasattr(bwe, 'details') and 'nInserted' in bwe.details:
                        total_imported += bwe.details['nInserted']
                    print(f"Warning: Some items in batch {i//batch_size + 1} couldn't be inserted. Continuing...")
                print(f"Imported batch {i//batch_size + 1}/{(len(hotels)-1)//batch_size + 1} ({total_imported}/{len(hotels)} hotels)")
            
            print(f"Successfully imported {total_imported} hotels to MongoDB (skipped {skipped_count} without HotelCode)")
            
            # Save a subset of hotels to JSON (first 1000) to keep file size reasonable
            # Get a fresh copy from the database to include the _id fields
            sample_hotels = list(hotel_collection.find().limit(1000))
            
            with open('models/hotels.json', 'w') as f:
                json.dump({"hotels": sample_hotels}, f, indent=2, cls=MongoJSONEncoder)
            print(f"Sample of {len(sample_hotels)} hotels also saved to models/hotels.json")
        else:
            print("No hotels to import")
    
    except Exception as e:
        print(f"Error during import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Import hotel data from CSV to MongoDB')
    parser.add_argument('--limit', type=int, default=100, help='Limit number of rows to import (default: 100, use 0 for no limit)')
    
    args = parser.parse_args()
    
    # Convert 0 to None (no limit)
    row_limit = None if args.limit == 0 else args.limit
    
    create_hotel_collection(row_limit)
    print("Hotel data import completed!")
