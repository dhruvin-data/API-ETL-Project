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

# API endpoint for Community Gardens and Food Trees
url = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/community-gardens-and-food-trees/records"

def fetch_and_store_data():
    # Establish database connection
    conn = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST, port=PORT)
    cur = conn.cursor()

    # Truncate the existing data in the table
    cur.execute("TRUNCATE TABLE community_gardens;")
    conn.commit()

    # Initialize an empty list to hold all data
    all_data = []
    offset = 0
    limit = 100  # API allows a maximum limit of 100

    while True:
        # Set parameters with pagination handling
        params = {
            'limit': limit,
            'start': offset  # Offset to fetch the next set of records
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            records = data['results']

            if not records:
                print("No more records to retrieve.")
                break

            all_data.extend(records)
            offset += limit  # Move to the next set of records

            print(f"Retrieved {len(records)} records, total so far: {len(all_data)}")
        else:
            print(f'Failed to fetch data. Status code: {response.status_code}')
            print('Response:', response.text)
            break

    if not all_data:
        print("No data retrieved from the API.")
        return

    # Prepare data for insertion
    insert_data = []
    for item in all_data:
        # Handle non-integer and float year_created values
        year_created = None
        if item.get('year_created'):
            if isinstance(item['year_created'], (int, float)):
                year_created = str(int(item['year_created']))
            elif isinstance(item['year_created'], str):
                if 'pre-' in item['year_created'].lower():
                    year_created = None  # Set to None for any "pre-" values
                elif item['year_created'].isdigit():
                    year_created = item['year_created']
            else:
                year_created = None

        number_of_plots = int(item.get('number_of_plots', 0)) if item.get('number_of_plots') and str(item.get('number_of_plots')).isdigit() else None
        number_of_food_trees = int(item.get('number_of_food_trees', 0)) if item.get('number_of_food_trees') and str(item.get('number_of_food_trees')).isdigit() else None

        geom = None
        if item.get('geom') and item['geom'].get('geometry') and item['geom']['geometry'].get('coordinates'):
            geom = WKTElement(f"POINT({item['geom']['geometry']['coordinates'][0]} {item['geom']['geometry']['coordinates'][1]})", srid=4326)
            geom = geom.desc  # Convert WKTElement to its WKT string representation

        insert_data.append((
            item.get('mapid', ''),
            year_created,
            item.get('name', ''),
            item.get('street_number', ''),
            item.get('street_direction', ''),
            item.get('street_name', ''),
            item.get('street_type', ''),
            number_of_plots,
            number_of_food_trees,
            item.get('food_tree_varieties', ''),
            item.get('jurisdiction', ''),
            item.get('steward_or_managing_organization', ''),
            item.get('public_e_mail', ''),
            item.get('website', ''),
            item.get('geo_local_area', ''),
            item['geom']['geometry']['coordinates'][1] if item.get('geom') else None,  # Latitude
            item['geom']['geometry']['coordinates'][0] if item.get('geom') else None,  # Longitude
            geom  # Insert the WKT string representation
        ))

    # Insert data into PostgreSQL
    if insert_data:
        insert_query = """
            INSERT INTO community_gardens (
                mapid, year_created, name, street_number, street_direction, street_name,
                street_type, number_of_plots, number_of_food_trees, food_tree_varieties,
                jurisdiction, steward_or_managing_organization, public_e_mail, website,
                geo_local_area, latitude, longitude, geom
            ) VALUES %s
            ON CONFLICT (mapid) DO NOTHING
        """
        try:
            execute_values(cur, insert_query, insert_data)
            conn.commit()
            print(f"Inserted {len(insert_data)} records into the community_gardens table.")
        except Exception as e:
            print(f"Error inserting data: {e}")
    else:
        print("No data available for insertion.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_store_data()

