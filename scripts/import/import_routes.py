import pandas as pd
import pymongo
import json
import random
from tqdm import tqdm
from geopy.distance import great_circle
import os
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime, timedelta

# Custom JSON encoder to handle MongoDB ObjectId
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super(MongoJSONEncoder, self).default(obj)

def calculate_flight_time(origin_lat, origin_lon, dest_lat, dest_lon):
    """
    Calculate approximate flight time based on distance between airports
    Returns flight duration in minutes
    """
    # Calculate distance in km
    try:
        origin = (origin_lat, origin_lon)
        destination = (dest_lat, dest_lon)
        distance_km = great_circle(origin, destination).kilometers
        
        # Flight time calculation:
        # - 30 minutes for takeoff and landing
        # - Average speed of 800 km/h for distance
        flight_time_minutes = (distance_km / 800) * 60 + 30
        
        # Round to nearest 5 minutes
        flight_time_minutes = round(flight_time_minutes / 5) * 5
        
        return int(flight_time_minutes)
    except Exception as e:
        # Log the exception for debugging purposes
        print(f"Error calculating flight time: {e}")
        # Default to 120 minutes if calculation fails
        return 120

def calculate_price(flight_time, airline, randomize=True):
    """Calculate price based on flight time and airline.
    Returns a dictionary with economy, business, and first class prices.
    If randomize is True, adds some random variation to the prices."""
    # Base price calculation based on flight time (minutes)
    base_price = flight_time * 0.5 + 50
    
    # Add some randomness to the base price (±30%)
    if randomize:
        base_price *= random.uniform(0.7, 1.3)
    
    # Adjust based on airline (some airlines are more expensive)
    # This is just a placeholder - in reality, you'd want to use actual airline data
    if airline and len(airline) > 0:
        # Use the first character of the airline code to create some price variation
        price_factor = (ord(airline[0]) % 20) / 100 + 0.9  # 0.9 to 1.1
        base_price *= price_factor
    
    # Calculate class prices
    economy = int(base_price)
    business = int(economy * 2.5)  # Business is typically 2-3x economy
    first = int(economy * 6)  # First class is typically 5-7x economy
    
    return {
        'economy': economy,
        'business': business,
        'first': first
    }

def generate_departure_times(count=5):
    """
    Generate sensible departure times throughout the day
    Returns a list of departure times in HH:MM format
    """
    times = []
    
    # Early morning (5-8AM)
    early_morning = random.randint(0, min(2, count))
    for _ in range(early_morning):
        hour = random.randint(5, 8)
        minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        times.append(f"{hour:02d}:{minute:02d}")
    
    # Day time (9AM-4PM)
    day_count = random.randint(0, min(count - len(times), count // 2 + 1))
    for _ in range(day_count):
        hour = random.randint(9, 16)
        minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        times.append(f"{hour:02d}:{minute:02d}")
    
    # Evening (5PM-11PM)
    while len(times) < count:
        hour = random.randint(17, 23)
        minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        times.append(f"{hour:02d}:{minute:02d}")
    
    # Ensure times are unique and sorted
    times = list(set(times))
    times.sort()
    
    # If we lost some due to duplicates, add more
    while len(times) < count:
        hour = random.randint(8, 21)
        minute = random.choice([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55])
        time = f"{hour:02d}:{minute:02d}"
        if time not in times:
            times.append(time)
    
    times.sort()
    return times[:count]

def generate_date_range_for_2025():
    """Generate all dates in 2025 as a list of datetime objects."""
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)
    current_date = start_date
    dates = []
    
    while current_date <= end_date:
        dates.append(current_date)
        current_date += timedelta(days=1)
    
    return dates

def get_day_of_week(dt):
    """Convert datetime day of week (0=Monday) to string name."""
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    return days[dt.weekday()]

def create_routes_collection():
    """
    Read routes.csv, transform the data, and store in MongoDB.
    Add synthetic price and flight time data based on distance.
    """
    print("Starting routes data import...")

    # Connect to MongoDB
    try:
        client = MongoClient('mongodb://192.168.1.57:27017/')
        db = client['travengo']
        routes_collection = db['routes']
        airports_collection = db['airports']
        
        # Drop existing collection to start fresh
        routes_collection.drop()
        
        # Create indexes for faster queries
        routes_collection.create_index([('source_airport', pymongo.ASCENDING)])
        routes_collection.create_index([('destination_airport', pymongo.ASCENDING)])
        routes_collection.create_index([('airline', pymongo.ASCENDING)])
        print("Connected to MongoDB successfully")
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return

    try:
        # Read the CSV file
        df = pd.read_csv('csvs/routes.csv')
        print(f"Read {len(df)} records from CSV")
        
        # Create a dictionary of airports for quick lookup
        airport_data = {}
        airports_cursor = airports_collection.find({}, {'iata_code': 1, 'location.latitude': 1, 'location.longitude': 1})
        for airport in airports_cursor:
            if 'iata_code' in airport and airport['iata_code']:
                airport_data[airport['iata_code']] = {
                    'latitude': airport.get('location', {}).get('latitude', 0),
                    'longitude': airport.get('location', {}).get('longitude', 0)
                }
        
        print(f"Loaded {len(airport_data)} airports for distance calculation")

        # Clean and transform data
        routes = []
        skipped_count = 0
        valid_count = 0
        
        column_map = {
            'airline': 'airline',
            'airline ID': 'airline_id',
            ' source airport': 'source_airport',  # Note the space before 'source airport'
            ' source airport id': 'source_airport_id',
            ' destination apirport': 'destination_airport',  # Note the typo in 'apirport'
            ' destination airport id': 'destination_airport_id',
            ' codeshare': 'codeshare',
            ' stops': 'stops',
            ' equipment': 'equipment'
        }
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing routes"):
            # Make sure source and destination IATA codes exist and are valid
            source_iata = row.get(' source airport', '').strip()
            dest_iata = row.get(' destination apirport', '').strip()  # Note the typo in CSV column name
            
            if not source_iata or not dest_iata or source_iata not in airport_data or dest_iata not in airport_data:
                skipped_count += 1
                continue
            
            # Get coordinates for flight time calculation
            source_coords = airport_data[source_iata]
            dest_coords = airport_data[dest_iata]
            
            # Debug coordinate values
            if source_coords['latitude'] == 0 and source_coords['longitude'] == 0:
                print(f"Warning: Source airport {source_iata} has coordinates (0,0)")
                skipped_count += 1
                continue
                
            if dest_coords['latitude'] == 0 and dest_coords['longitude'] == 0:
                print(f"Warning: Destination airport {dest_iata} has coordinates (0,0)")
                skipped_count += 1
                continue
                
            # Calculate distance
            try:
                distance_km = great_circle(
                    (source_coords['latitude'], source_coords['longitude']),
                    (dest_coords['latitude'], dest_coords['longitude'])
                ).kilometers
            except Exception as e:
                print(f"Error calculating distance: {e} for {source_iata}-{dest_iata}")
                skipped_count += 1
                continue
                
            # Skip if distance calculation failed
            if distance_km == 0:
                print(f"Warning: Distance calculation is 0 for {source_iata}-{dest_iata}")
                skipped_count += 1
                continue
            
            # Calculate flight time
            flight_time = calculate_flight_time(
                source_coords['latitude'], source_coords['longitude'],
                dest_coords['latitude'], dest_coords['longitude']
            )
            
            # We'll generate specific prices and departure times for each day individually
            
            # Create base route data - common to all flights
            base_route = {}
            for csv_col, mongo_field in column_map.items():
                if csv_col in row and pd.notna(row[csv_col]):
                    base_route[mongo_field] = row[csv_col].strip() if isinstance(row[csv_col], str) else row[csv_col]
            
            # Add calculated fields common to all flights
            base_route['flight_time_minutes'] = flight_time
            base_route['distance_km'] = distance_km
            
            # Get all dates for 2025
            all_2025_dates = generate_date_range_for_2025()
            
            # Define how many flights per week for this route (between 1 and 4)
            weekly_frequency = random.randint(1, 4)
            
            # Select which days this route operates (e.g., Mon/Wed/Fri)
            operating_days = random.sample(range(7), weekly_frequency)
            
            # Get all operating dates for this route in 2025
            route_dates = [date for date in all_2025_dates if date.weekday() in operating_days]
            
            # For each operating date, create flight documents
            for flight_date in route_dates:
                # Get string day of week
                day_of_week = get_day_of_week(flight_date)
                
                # How many flights for this route on this day
                daily_flights = random.randint(1, 3)
                day_departure_times = generate_departure_times(daily_flights)
                
                for departure_time in day_departure_times:
                    # Create a new flight based on the base route
                    flight = base_route.copy()
                    
                    # Add date-specific information
                    hrs, mins = map(int, departure_time.split(':'))
                    
                    # Create UTC datetime for departure and arrival
                    departure_datetime = flight_date.replace(
                        hour=hrs,
                        minute=mins,
                        second=0,
                        microsecond=0
                    )
                    
                    # Store as ISO format
                    flight['departure_datetime'] = departure_datetime.isoformat() + 'Z'  # 'Z' indicates UTC
                    flight['day_of_week'] = day_of_week
                    
                    # Calculate arrival datetime
                    arrival_datetime = departure_datetime + timedelta(minutes=flight_time)
                    flight['arrival_datetime'] = arrival_datetime.isoformat() + 'Z'
                    
                    # Also store simpler time strings for legacy compatibility
                    flight['departure_time'] = departure_time
                    flight['arrival_time'] = arrival_datetime.strftime('%H:%M')
                    
                    # Flag if arrival is next day
                    next_day = 1 if arrival_datetime.date() > departure_datetime.date() else 0
                    flight['next_day'] = next_day
                    
                    # Generate prices with some daily variation
                    day_price = calculate_price(flight_time, flight.get('airline', ''), randomize=True)
                    
                    # Add price info - flatten the price structure
                    flight['price_economy'] = day_price['economy']
                    flight['price_business'] = day_price['business']
                    flight['price_first'] = day_price['first']
                    
                    # No more availability arrays
                    flight['has_economy'] = True
                    flight['has_business'] = True 
                    flight['has_first'] = random.random() > 0.3  # 70% chance of having first class
                    
                    # Generate a flight number if not present
                    if 'airline' in flight and not flight.get('flight_number'):
                        airline_code = flight['airline'][:2]
                        flight['flight_number'] = f"{airline_code}{random.randint(100, 9999)}"
                    
                    # Add the flight to our collection
                    routes.append(flight)
                    valid_count += 1
            
            # Print progress periodically
            if valid_count % 1000 == 0:
                print(f"Processed {valid_count} valid routes so far")

        # Insert into MongoDB
        if routes:
            # Insert data in batches to avoid potential memory issues
            batch_size = 1000
            total_imported = 0
            
            for i in range(0, len(routes), batch_size):
                batch = routes[i:i+batch_size]
                result = routes_collection.insert_many(batch)
                total_imported += len(result.inserted_ids)
                print(f"Imported batch {i//batch_size + 1}/{(len(routes)-1)//batch_size + 1} ({total_imported}/{len(routes)} routes)")
            
            print(f"Successfully imported {total_imported} routes to MongoDB (skipped {skipped_count} invalid routes)")
            
            # Save a subset of routes to JSON (first 1000) to keep file size reasonable
            # Get a fresh copy from the database to include the _id fields
            sample_routes = list(routes_collection.find().limit(1000))
            
            # Create models directory if it doesn't exist
            os.makedirs('models', exist_ok=True)
            
            with open('models/routes.json', 'w') as f:
                json.dump({"routes": sample_routes}, f, indent=2, cls=MongoJSONEncoder)
            print(f"Sample of {len(sample_routes)} routes also saved to models/routes.json")
        else:
            print("No routes to import")
    
    except Exception as e:
        print(f"Error during import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_routes_collection()
    print("Routes data import completed!")
