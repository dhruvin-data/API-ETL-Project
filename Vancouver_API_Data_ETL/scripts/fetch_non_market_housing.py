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

# API endpoint for Non-Market Housing
url = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/non-market-housing/records"

def fetch_and_store_data():
    # Establish database connection
    conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cur = conn.cursor()

    # Truncate the existing data in the table
    cur.execute("TRUNCATE TABLE non_market_housing;")
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
            item.get('name', ''),
            item.get('address', ''),
            item.get('project_status', ''),
            item.get('occupancy_year', None),
            item.get('design_accessible_1br', 0),
            item.get('design_accessible_2br', 0),
            item.get('design_accessible_3br', 0),
            item.get('design_accessible_4br', 0),
            item.get('design_accessible_studio', 0),
            item.get('design_accessible_room', 0),
            item.get('design_adaptable_1br', 0),
            item.get('design_adaptable_2br', 0),
            item.get('design_adaptable_3br', 0),
            item.get('design_adaptable_4br', 0),
            item.get('design_standard_1br', 0),
            item.get('design_standard_2br', 0),
            item.get('design_standard_3br', 0),
            item.get('design_standard_4br', 0),
            item.get('design_standard_studio', 0),
            item.get('design_standard_room', 0),
            item.get('clientele_families', 0),
            item.get('clientele_seniors', 0),
            item.get('clientele_other', 0),
            item.get('operator', ''),
            item.get('url', ''),
            geom,  # Insert the WKT string representation
            item['geom']['geometry']['coordinates'][1] if item.get('geom') else None,  # Latitude
            item['geom']['geometry']['coordinates'][0] if item.get('geom') else None   # Longitude
        ))

    if insert_data:
        # Insert data into the PostgreSQL table
        insert_query = """
            INSERT INTO non_market_housing (
                name, address, project_status, occupancy_year, 
                design_accessible_1br, design_accessible_2br, design_accessible_3br, design_accessible_4br, design_accessible_studio,
                design_accessible_room, design_adaptable_1br, design_adaptable_2br, design_adaptable_3br, design_adaptable_4br,
                design_standard_1br, design_standard_2br, design_standard_3br, design_standard_4br, design_standard_studio,
                design_standard_room, clientele_families, clientele_seniors, clientele_other, operator, url, geom, latitude, longitude
            ) VALUES %s
            ON CONFLICT (index_number) DO NOTHING
        """
        execute_values(cur, insert_query, insert_data)
        conn.commit()
        print(f"Inserted {len(insert_data)} records into the non_market_housing table.")
    else:
        print("No data available for insertion.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_data()
