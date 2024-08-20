CREATE EXTENSION postgis;

CREATE TABLE rental_standards (
    id SERIAL PRIMARY KEY,
    business_operator VARCHAR(255),
    detail_url VARCHAR(255),
    street_number VARCHAR(10),
    street VARCHAR(255),
    total_outstanding INT,
    total_units INT,
    geom GEOGRAPHY(Point, 4326),
    geo_local_area VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8
);
select * from rental_standards;

CREATE TABLE non_market_housing (
    index_number SERIAL PRIMARY KEY,
    name VARCHAR(255),
    address VARCHAR(255),
    project_status VARCHAR(50),
    occupancy_year INT,
    design_accessible_1br INT,
    design_accessible_2br INT,
    design_accessible_3br INT,
    design_accessible_4br INT,
    design_accessible_studio INT,
    design_accessible_room INT,
    design_adaptable_1br INT,
    design_adaptable_2br INT,
    design_adaptable_3br INT,
    design_adaptable_4br INT,
    design_standard_1br INT,
    design_standard_2br INT,
    design_standard_3br INT,
    design_standard_4br INT,
    design_standard_studio INT,
    design_standard_room INT,
    clientele_families INT,
    clientele_seniors INT,
    clientele_other INT,
    operator VARCHAR(255),
    url VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    geom GEOGRAPHY(Point, 4326)
);

select * from non_market_housing;


CREATE TABLE free_low_cost_food (
    id SERIAL PRIMARY KEY,
    program_name VARCHAR(255),
    description TEXT,
    program_status VARCHAR(50),
    organization_name VARCHAR(255),
    location_address VARCHAR(255),
    local_areas VARCHAR(255),
    provides_meals BOOLEAN,
    provides_hampers BOOLEAN,
    delivery_available BOOLEAN,
    takeout_available BOOLEAN,
    wheelchair_accessible VARCHAR(50),
    meal_cost VARCHAR(50),
    hamper_cost VARCHAR(50),
    signup_required BOOLEAN,
    signup_phone_number VARCHAR(50),
    signup_email VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    last_update_date TIMESTAMP,
    geom GEOGRAPHY(Point, 4326)
);

select * from free_low_cost_food;

CREATE TABLE homeless_shelters (
    id SERIAL PRIMARY KEY,
    facility VARCHAR(255),
    category VARCHAR(255),
    phone VARCHAR(50),
    meals BOOLEAN,
    pets BOOLEAN,
    carts BOOLEAN,
    geo_local_area VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    geom GEOGRAPHY(Point, 4326)
);
select * from homeless_shelters;


CREATE TABLE community_gardens (
    mapid VARCHAR(10) PRIMARY KEY,
    year_created INT,
    name VARCHAR(255),
    street_number VARCHAR(10),
    street_direction VARCHAR(10),
    street_name VARCHAR(255),
    street_type VARCHAR(10),
    merged_address VARCHAR(255),
    number_of_plots INT,
    number_of_food_trees INT,
    food_tree_varieties VARCHAR(255),
    jurisdiction VARCHAR(50),
    steward_or_managing_organization VARCHAR(255),
    public_e_mail VARCHAR(255),
    website VARCHAR(255),
    geo_local_area VARCHAR(255),
    latitude FLOAT8,
    longitude FLOAT8,
    geom GEOGRAPHY(Point, 4326)
);
ALTER TABLE community_gardens ALTER COLUMN food_tree_varieties TYPE VARCHAR(1000);

ALTER TABLE community_gardens
DROP COLUMN merged_address

select * from community_gardens;



SELECT DISTINCT geo_local_area, 'Community Gardens' AS source_table
FROM public.community_gardens
UNION
SELECT DISTINCT local_areas, 'Free Low-Cost Food Programs' AS source_table
FROM public.free_low_cost_food
UNION
SELECT DISTINCT geo_local_area, 'Homeless Shelters' AS source_table
FROM public.homeless_shelters
UNION
SELECT DISTINCT geo_local_area, 'Rental Standards' AS source_table
FROM public.rental_standards;
