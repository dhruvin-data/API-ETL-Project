import requests
import psycopg2
from psycopg2.extras import execute_values
from geoalchemy2 import WKTElement

# PostgreSQL connection details
DB_NAME = "api_data_v2"
USER = "postgres"
PASSWORD = "Nivurhd"
HOST = "localhost"
PORT = "5432"

# API endpoint for Rental Standards
url = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/rental-standards-current-issues/records"

def fetch_and_store_data():
    # Establish database connection
    conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cur = conn.cursor()

    # Truncate the existing data in the table
    cur.execute("TRUNCATE TABLE rental_standards;")
    conn.commit()

    # Initialize an empty list to hold all data
    all_data = []
    offset = 0
    limit = 100  # API allows a maximum limit of 100

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
            print("API Response:", data)
            break

        # Retrieve the records
        records = data.get('results', [])
        if not records:
            print("No more records to retrieve.")
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
        geom = None
        if item.get('geom') and item['geom'].get('geometry') and item['geom']['geometry'].get('coordinates'):
            geom = WKTElement(f"POINT({item['geom']['geometry']['coordinates'][0]} {item['geom']['geometry']['coordinates'][1]})", srid=4326)
            geom = geom.desc  # Convert WKTElement to its WKT string representation

        insert_data.append((
            item.get('businessoperator', ''),
            item.get('detailurl', ''),
            item.get('streetnumber', ''),
            item.get('street', ''),
            item.get('totaloutstanding', None),
            item.get('totalunits', None),
            geom,  # Insert the WKT string representation
            item.get('geo_local_area', ''),
            item['geom']['geometry']['coordinates'][1] if item.get('geom') else None,  # Latitude
            item['geom']['geometry']['coordinates'][0] if item.get('geom') else None   # Longitude
        ))

    if insert_data:
        # Insert data into the PostgreSQL table
        insert_query = """
            INSERT INTO rental_standards (
                business_operator, detail_url, street_number, street, 
                total_outstanding, total_units, geom, geo_local_area, latitude, longitude
            ) VALUES %s
            ON CONFLICT (id) DO NOTHING
        """
        execute_values(cur, insert_query, insert_data)
        conn.commit()
        print(f"Inserted {len(insert_data)} records into the rental_standards table.")
    else:
        print("No data available for insertion.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_data()
