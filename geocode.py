import psycopg2
import yaml
import overpy
from geopy.geocoders import Nominatim
from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import dumps as to_wkt

# Load database configuration from YAML
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

# Initialize Overpass API and geolocator
api = overpy.Overpass()
geolocator = Nominatim(user_agent="geo_app")

# Fetch rows from Reseller where geocoding is incomplete
cursor.execute("""
    SELECT DISTINCT "City", "State-Province", "Country-Region"
    FROM "adventureworks"."Reseller"
    WHERE "Latitude" IS NULL OR "Longitude" IS NULL OR "CityShape" IS NULL;
""")
rows = cursor.fetchall()

# Function to fetch city boundary (shape) from Overpass API
def get_city_shape(city, state):
    try:
        query = f"""
        [out:json];
        area["name"="{state}"]["admin_level"="4"];
        relation["name"="{city}"]["boundary"="administrative"](area);
        out geom;
        """
        result = api.query(query)

        # Collect polygons from relation members
        polygons = []
        for relation in result.relations:
            for member in relation.members:
                if member.role == "outer" and isinstance(member, overpy.Way):
                    coords = [(node.lon, node.lat) for node in member.nodes]
                    polygons.append(Polygon(coords))

        # Combine polygons into MultiPolygon
        if polygons:
            return MultiPolygon(polygons)
        else:
            print(f"No polygons found for {city}, {state}")
            return None

    except Exception as e:
        print(f"Error fetching shape for {city}, {state}: {e}")
        return None

# Process each row
for city, state, country in rows:
    latitude = longitude = city_shape_wkt = None

    # Geocode the city for latitude and longitude
    try:
        location = geolocator.geocode(f"{city}, {state}, {country}")
        if location:
            latitude = location.latitude
            longitude = location.longitude
            print(f"Geocoded {city}, {state}, {country}: {latitude}, {longitude}")
        else:
            print(f"Could not geocode {city}, {state}, {country}")
    except Exception as e:
        print(f"Error geocoding {city}, {state}, {country}: {e}")

    # Fetch city shape data
    city_shape = get_city_shape(city, state)
    if city_shape:
        city_shape_wkt = to_wkt(city_shape)
        print(f"Retrieved shape for {city}, {state}, {country}")

    # Update the Reseller table with the geocoded data
    try:
        cursor.execute("""
            UPDATE "adventureworks"."Reseller"
            SET "Latitude" = %s, "Longitude" = %s, "CityShape" = ST_GeomFromText(%s, 4326)
            WHERE "City" = %s AND "State-Province" = %s AND "Country-Region" = %s;
        """, (latitude, longitude, city_shape_wkt, city, state, country))
        conn.commit()
    except Exception as e:
        print(f"Error updating database for {city}, {state}, {country}: {e}")

# Close the connection
cursor.close()
conn.close()
print("Geocoding and shape processing completed.")