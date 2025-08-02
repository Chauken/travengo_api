import pandas as pd
import pymongo
import json
import os
import random
from datetime import datetime, timedelta
from bson import ObjectId
from pymongo import MongoClient
from tqdm import tqdm

# Custom JSON encoder to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MongoJSONEncoder, self).default(obj)

# Activity categories and types
ACTIVITY_CATEGORIES = {
    "Outdoor": [
        "Hiking", "Kayaking", "Cycling", "Boat Tour", "Safari", "Zip-lining", 
        "Snorkeling", "Diving", "Surfing", "Skiing", "Paragliding"
    ],
    "Cultural": [
        "Museum Visit", "Historic Site Tour", "Art Gallery", "Theater Performance", 
        "Local Craft Workshop", "Traditional Dance Show", "Food Tour"
    ],
    "Entertainment": [
        "Theme Park", "Concert", "Live Music", "Comedy Show", "Sports Event", 
        "Cinema", "Festival", "Nightlife Tour"
    ],
    "Wellness": [
        "Spa Day", "Yoga Class", "Meditation Retreat", "Hot Springs", "Fitness Class"
    ],
    "Educational": [
        "Cooking Class", "Wine Tasting", "Language Workshop", "Wildlife Tour", 
        "Photography Tour", "Guided City Tour"
    ],
    "Food & Dining": [
        "Fine Dining", "Local Restaurant", "Food Market", "Street Food Tour", "Culinary Experience",
        "Cooking Class", "Wine Tasting", "Brewery Tour", "Food Festival", "Chef's Table"
    ]
}

# Sample activity descriptions
ACTIVITY_DESCRIPTIONS = {
    "Hiking": [
        "Explore scenic trails with breathtaking views of the surrounding landscape.",
        "Guided hiking tour through lush forests and mountain paths with expert naturalists.",
        "Trek through diverse terrains and discover hidden natural wonders."
    ],
    "Museum Visit": [
        "Discover fascinating exhibits showcasing local history and culture.",
        "Guided tour of world-class art and historical artifacts with expert commentary.",
        "Interactive museum experience with hands-on exhibits for all ages."
    ],
    "Theme Park": [
        "Enjoy thrilling rides and entertainment for the whole family.",
        "Experience state-of-the-art attractions and live shows in a fun-filled environment.",
        "Adventure through themed zones with exciting rides and character encounters."
    ],
    "Spa Day": [
        "Relax with rejuvenating treatments in a tranquil atmosphere.",
        "Indulge in luxurious wellness therapies and beauty treatments.",
        "Unwind with massage therapies and wellness rituals in a serene setting."
    ],
    "Cooking Class": [
        "Learn to prepare authentic local dishes with expert chefs.",
        "Hands-on culinary experience featuring regional ingredients and techniques.",
        "Master traditional recipes and enjoy the meal you prepare."
    ],
    "Fine Dining": [
        "Experience exquisite cuisine at a renowned restaurant with expert chefs.",
        "Savor gourmet dishes featuring local and international flavors in an elegant setting.",
        "Indulge in a multi-course tasting menu with carefully selected wine pairings."
    ],
    "Local Restaurant": [
        "Enjoy authentic regional specialties at a favorite local eatery.",
        "Taste traditional dishes made with fresh local ingredients in a cozy atmosphere.",
        "Experience genuine local cuisine loved by residents and visitors alike."
    ],
    "Street Food Tour": [
        "Sample diverse local flavors from the best street food vendors in the city.",
        "Discover hidden culinary gems and popular street food spots with expert guides.",
        "Taste a variety of authentic street foods while learning about local food culture."
    ],
    "default": [
        "A fantastic experience that showcases the best of local culture and attractions.",
        "Popular activity among tourists and locals alike, offering unique experiences.",
        "Highly-rated experience with professional guides and excellent service.",
        "Unforgettable adventure that creates lasting memories of your trip."
    ]
}

def generate_activities_from_locations(db, limit_per_location=3, test_mode=False):
    """
    Generate mock activity data based on hotel and airport locations.
    
    Args:
        db: MongoDB database connection
        limit_per_location: Number of activities to generate per location
        test_mode: If True, only generate a small number of activities for testing
    """
    locations = []
    
    # Get locations from hotels
    hotels_cursor = db.hotels.find({}, {"location": 1, "city.name": 1, "country": 1})
    for hotel in hotels_cursor:
        if "location" in hotel and "city" in hotel:
            locations.append({
                "latitude": hotel["location"].get("latitude", 0),
                "longitude": hotel["location"].get("longitude", 0),
                "city": hotel["city"].get("name", "Unknown"),
                "country": hotel["country"].get("code", "")
            })
    
    # Get locations from airports
    airports_cursor = db.airports.find({}, {"location": 1, "municipality": 1, "country": 1})
    for airport in airports_cursor:
        if "location" in airport and "municipality" in airport:
            locations.append({
                "latitude": airport["location"].get("latitude", 0),
                "longitude": airport["location"].get("longitude", 0),
                "city": airport.get("municipality", "Unknown"),
                "country": airport.get("country", "")
            })
    
    # Remove duplicates and limit if in test mode
    unique_cities = set()
    filtered_locations = []
    for location in locations:
        city_country = f"{location['city']}_{location['country']}"
        if city_country not in unique_cities and location['city'] != "Unknown":
            unique_cities.add(city_country)
            filtered_locations.append(location)
    
    if test_mode:
        filtered_locations = filtered_locations[:10]  # Limit to 10 locations for testing
    
    print(f"Generating activities for {len(filtered_locations)} unique locations")
    
    # Generate activities
    activities = []
    activity_id = 1000
    
    for location in tqdm(filtered_locations, desc="Generating activities"):
        # Generate 1-3 activities per location
        num_activities = random.randint(1, limit_per_location)
        
        for _ in range(num_activities):
            # Slightly vary the coordinates to distribute activities around the location
            lat_variation = random.uniform(-0.01, 0.01)
            lng_variation = random.uniform(-0.01, 0.01)
            
            # Select random category and activity type
            category = random.choice(list(ACTIVITY_CATEGORIES.keys()))
            activity_type = random.choice(ACTIVITY_CATEGORIES[category])
            
            # Generate random price
            base_price = {
                "Outdoor": random.randint(30, 120),
                "Cultural": random.randint(15, 80),
                "Entertainment": random.randint(40, 200),
                "Wellness": random.randint(60, 250),
                "Educational": random.randint(25, 100),
                "Food & Dining": random.randint(20, 150)
            }[category]
            
            # Add some randomness to price
            price = round(base_price * random.uniform(0.8, 1.2))
            
            # Get description
            descriptions = ACTIVITY_DESCRIPTIONS.get(activity_type, ACTIVITY_DESCRIPTIONS["default"])
            description = random.choice(descriptions)
            
            # Generate random duration (1-8 hours)
            duration = random.randint(1, 8)
            
            # Generate name
            city_prefix = location['city'] if random.random() > 0.5 else ""
            activity_name = f"{city_prefix} {activity_type}".strip()
            if random.random() > 0.7:  # Sometimes add an adjective
                adjectives = ["Amazing", "Exciting", "Unforgettable", "Premium", "Exclusive", "Ultimate"]
                activity_name = f"{random.choice(adjectives)} {activity_name}"
            
            # Generate random rating between 3.5 and 5.0
            rating = round(random.uniform(3.5, 5.0), 1)
            
            # Create activity object
            activity = {
                "activity_id": str(activity_id),
                "name": activity_name,
                "category": category,
                "type": activity_type,
                "description": description,
                "location": {
                    "latitude": location["latitude"] + lat_variation,
                    "longitude": location["longitude"] + lng_variation,
                    "city": location["city"],
                    "country": location["country"]
                },
                "price": {
                    "amount": price,
                    "currency": "USD"
                },
                "duration_hours": duration,
                "rating": rating,
                "reviews_count": random.randint(5, 500),
                "availability": {
                    "days_available": random.sample(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], random.randint(5, 7))
                },
                "booking_required": random.random() > 0.3  # 70% require booking
            }
            
            activities.append(activity)
            activity_id += 1
    
    return activities

def create_activities_collection(test_mode=False):
    """
    Generate mock activity data and store in MongoDB.
    
    Args:
        test_mode: If True, only generate a small number of activities for testing
    """
    print("Starting activity data generation...")

    # Connect to MongoDB
    try:
        client = MongoClient('mongodb://192.168.1.57:27017/')
        db = client['travengo']
        activity_collection = db['activities']
        
        # Drop existing collection to start fresh
        activity_collection.drop()
        
        # Create indexes for faster queries
        activity_collection.create_index([('activity_id', pymongo.ASCENDING)], unique=True)
        
        # Create text index for searching
        activity_collection.create_index(
            [
                ('name', pymongo.TEXT), 
                ('description', pymongo.TEXT),
                ('category', pymongo.TEXT),
                ('type', pymongo.TEXT),
                ('location.city', pymongo.TEXT),
                ('location.country', pymongo.TEXT)
            ],
            weights={
                'name': 10,
                'category': 8,
                'type': 8,
                'description': 5,
                'location.city': 10,
                'location.country': 3
            },
            name='search_index')
        print("Connected to MongoDB successfully")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    try:
        # Generate activities based on hotel and airport locations
        activities = generate_activities_from_locations(db, limit_per_location=5, test_mode=test_mode)
        
        print(f"Generated {len(activities)} activities")
        
        # Insert into MongoDB
        if activities:
            # Create models directory if it doesn't exist
            os.makedirs('models', exist_ok=True)
            
            # Insert data in batches to avoid potential memory issues
            batch_size = 500
            total_imported = 0
            
            for i in range(0, len(activities), batch_size):
                try:
                    batch = activities[i:i+batch_size]
                    result = activity_collection.insert_many(batch)
                    total_imported += len(result.inserted_ids)
                except Exception as e:
                    print(f"Error inserting batch: {e}")
                
                print(f"Imported batch {i//batch_size + 1}/{(len(activities)-1)//batch_size + 1} ({total_imported}/{len(activities)} activities)")
            
            print(f"Successfully imported {total_imported} activities to MongoDB")
            
            # Save activities to JSON
            with open('models/activities.json', 'w') as f:
                json.dump({"activities": activities[:1000]}, f, indent=2, cls=MongoJSONEncoder)
            print(f"Sample of {min(1000, len(activities))} activities saved to models/activities.json")
        else:
            print("No activities to import")
    
    except Exception as e:
        print(f"Error during import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate and import activity data to MongoDB')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited data')
    
    args = parser.parse_args()
    
    create_activities_collection(test_mode=args.test)
    print("Activity data import completed!")
