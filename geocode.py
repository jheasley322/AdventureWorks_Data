import psycopg2
import yaml
import overpy
from geopy.geocoders import Nominatim
from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import dumps as to_wkt

# Load database configuration from YAML file
with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)
db_config = config["database"]

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=db_config["dbname"],
    user=db_config["user"],
    password=db_config["password"],
    host=db_config["host"],
    port=db_config.get("port", 5432)
)
cursor = conn.cursor()

# Fetch unique cities, states, and countries that have not been geocoded
cursor.execute("""
    SELECT DISTINCT "City", "State-Province", "Country-Region"
    FROM "adventureworks"."Reseller"
    WHERE "Latitude" IS NULL OR "Longitude" IS NULL OR "CityShape" IS NULL;
""")
rows = cursor.fetchall()

# Initialize geocoders and Overpass API
geolocator = Nominatim(user_agent="geo_app")
api = overpy.Overpass()

# Geocode each City, State-Province, and Country-Region
for city, state, country in rows:
    latitude = longitude = None
    city_shape = None

    try:
        # Step 1: Geocode the city for latitude and longitude
        location_query = f"{city}, {state}, {country}"
        location = geolocator.geocode(location_query)
        if location:
            latitude = location.latitude
            longitude = location.longitude
            print(f"Geocoded {location_query}: {latitude}, {longitude}")
        else:
            print(f"Could not geocode {location_query}")
        
        # Step 2: Fetch city shape from Overpass API
        if latitude and longitude:
            shape_query = f"""
                [out:json];
                area["name"="{state}"]["admin_level"="4"];   // State or province area
                relation["name"="{city}"]["boundary"="administrative"](area);
                out geom;
            """
            result = api.query(shape_query)

            polygons = []
            for relation in result.relations:
                for way in relation.ways:
                    coords = [(node.lon, node.lat) for node in way.nodes]
                    polygons.append(Polygon(coords))
            
            if polygons:
                merged_geometry = MultiPolygon(polygons)
                city_shape = to_wkt(merged_geometry)
                print(f"Retrieved shape for {city}, {state}, {country}")
            else:
                print(f"No shape data for {city}, {state}, {country}")

    except Exception as e:
        print(f"Error processing {city}, {state}, {country}: {e}")

    # Step 3: Update the Region table with geocoded data and shape
    cursor.execute("""
        UPDATE "adventureworks"."Reseller"
        SET "Latitude" = %s, "Longitude" = %s, "CityShape" = ST_GeomFromText(%s, 4326)
        WHERE "City" = %s AND "State-Province" = %s AND "Country-Region" = %s;
    """, (latitude, longitude, city_shape, city, state, country))
    conn.commit()

# Close the connection
cursor.close()
conn.close()
print("Geocoding and shape processing completed.")