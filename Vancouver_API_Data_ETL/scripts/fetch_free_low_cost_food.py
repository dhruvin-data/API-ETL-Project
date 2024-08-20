import requests
import psycopg2
from psycopg2.extras import execute_values

# PostgreSQL connection details
DB_NAME = "api_data_v2"
USER = "postgres"
PASSWORD = "Nivurhd"
HOST = "localhost"
PORT = "5432"

# Corrected API endpoint for Free and Low-Cost Food Programs
url = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/free-and-low-cost-food-programs/records"

def fetch_and_store_data():
    # Establish database connection
    conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cur = conn.cursor()

    # Truncate the existing data in the table
    cur.execute("TRUNCATE TABLE free_low_cost_food;")
    conn.commit()

    # Initialize an empty list to hold all data
    all_data = []
    offset = 0
    limit = 20  # You specified a limit of 20 in the API path

    while True:
        # Set parameters with pagination handling
        params = {
            'limit': limit,
            'offset': offset  # Offset to fetch the next set of records
        }

        response = requests.get(url, params=params)
        data = response.json()

        # Check for errors in the API response
        if response.status_code != 200 or 'error_code' in data:
            print("API Response Error:", data)
            break

        # Retrieve the records
        records = data.get('results', [])
        if not records:
            break

        all_data.extend(records)
        offset += limit  # Move to the next set of records

        print(f"Retrieved {len(records)} records, total so far: {len(all_data)}")

    if not all_data:
        print("No data retrieved from the API.")
        return

    # Prepare data for insertion
    insert_data = []

    for item in all_data:
        # Extract the 'program_name' correctly
        program_name = item.get('program_name', '')

        # Attempt to extract the 'last update date' more robustly
        last_update_raw = item.get('last_update_date') or item.get('last update date')

        # Handle geometry data
        geom = None
        latitude = None
        longitude = None
        if item.get('latitude') and item.get('longitude'):
            latitude = item.get('latitude')
            longitude = item.get('longitude')
            geom = f"SRID=4326;POINT({longitude} {latitude})"  # WKT format for PostgreSQL

        # Convert non-boolean values to None
        def convert_to_boolean(value):
            if isinstance(value, str):
                if value.lower() in ['true', 'yes']:
                    return True
                elif value.lower() in ['false', 'no']:
                    return False
            return None  # If value is "Unknown" or anything else, set it to None

        insert_data.append((
            program_name,  # Corrected to capture program name
            item.get('description', ''),
            item.get('program_status', ''),
            item.get('organization_name', ''),
            item.get('location_address', ''),
            item.get('local_areas', ''),
            convert_to_boolean(item.get('provides_meals', False)),
            convert_to_boolean(item.get('provides_hampers', False)),
            convert_to_boolean(item.get('delivery_available', False)),
            convert_to_boolean(item.get('takeout_available', False)),
            item.get('wheelchair_accessible', ''),
            item.get('meal_cost', ''),
            item.get('hamper_cost', ''),
            convert_to_boolean(item.get('signup_required', False)),
            item.get('signup_phone_number', ''),
            item.get('signup_email', ''),
            latitude,  # Latitude
            longitude,  # Longitude
            last_update_raw,  # Use raw value directly for inspection
            geom  # WKT string for geom
        ))

    if insert_data:
        # Insert data into the PostgreSQL table
        insert_query = """
            INSERT INTO free_low_cost_food (
                program_name, description, program_status, organization_name, location_address,
                local_areas, provides_meals, provides_hampers, delivery_available, takeout_available,
                wheelchair_accessible, meal_cost, hamper_cost, signup_required, signup_phone_number, signup_email,
                latitude, longitude, last_update_date, geom
            ) VALUES %s
            ON CONFLICT (id) DO NOTHING
        """
        try:
            execute_values(cur, insert_query, insert_data)
            conn.commit()
            print(f"Inserted {len(insert_data)} records into the free_low_cost_food table.")
        except Exception as e:
            print(f"Error inserting data: {e}")
    else:
        print("No data available for insertion.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_data()
