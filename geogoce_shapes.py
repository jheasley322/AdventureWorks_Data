import psycopg2
import yaml
import overpy
from shapely.geometry import Polygon, MultiPolygon, Point
from shapely.ops import unary_union
from shapely.wkt import dumps as to_wkt
from geopy.geocoders import Nominatim

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

def geocode_city(city, state, country):
    """Geocode a city to get its latitude and longitude."""
    try:
        location = geolocator.geocode(f"{city}, {state}, {country}")
        if location:
            print(f"Geocoded {city}, {state}, {country}: {location.latitude}, {location.longitude}")
            return location.latitude, location.longitude
        else:
            print(f"Could not geocode {city}, {state}, {country}")
            return None, None
    except Exception as e:
        print(f"Error geocoding {city}, {state}, {country}: {e}")
        return None, None

def normalize_postal_code(postal_code, country):
    """Normalize postal code based on country."""
    if country == "United States":
        return postal_code.split('-')[0]  # Strip to basic ZIP code
    elif country == "Canada":
        return postal_code.replace(" ", "").upper()  # Remove spaces, uppercase
    elif country == "United Kingdom":
        return postal_code.replace(" ", "").upper()  # Remove spaces, uppercase
    elif country == "Australia":
        return postal_code  # Australian postal codes are simple numbers
    elif country == "France":
        return postal_code  # French postal codes are 5-digit numbers
    elif country == "Germany":
        return postal_code  # German postal codes are 5-digit numbers
    return postal_code  # Default: no modification

def fetch_postal_codes(city, state, country):
    """Fetch postal codes for a city using Overpass API."""
    try:
        query = f"""
        [out:json];
        area["name"="{state}"]["admin_level"="4"];
        node["addr:city"="{city}"]["addr:postcode"](area);
        out;
        """
        result = api.query(query)
        postal_codes = {
            normalize_postal_code(node.tags.get("addr:postcode"), country)
            for node in result.nodes if "addr:postcode" in node.tags
        }
        print(f"Postal codes for {city}, {state}, {country}: {postal_codes}")
        return postal_codes
    except Exception as e:
        print(f"Error fetching postal codes for {city}, {state}, {country}: {e}")
        return set()

def fetch_postal_code_shapes(postal_codes):
    """Fetch boundary shapes for postal codes and return dissolved geometry."""
    polygons = []
    for postal_code in postal_codes:
        try:
            query = f"""
            [out:json];
            relation["addr:postcode"="{postal_code}"];
            out geom;
            """
            result = api.query(query)

            # Extract polygons from relation members
            for relation in result.relations:
                for member in relation.members:
                    if member.role == "outer" and isinstance(member, overpy.Way):
                        coords = [(node.lon, node.lat) for node in member.nodes]
                        polygons.append(Polygon(coords))
        except Exception as e:
            print(f"Error fetching shape for postal code {postal_code}: {e}")

    # Dissolve all polygons into a single geometry
    if polygons:
        dissolved_polygon = unary_union(polygons)
        print(f"Dissolved geometry created for postal codes: {postal_codes}")
        return dissolved_polygon
    else:
        print(f"No polygons found for postal codes: {postal_codes}")
        return None

# Fetch rows needing city shapes
cursor.execute("""
    SELECT DISTINCT "City", "State-Province", "Country-Region"
    FROM "adventureworks"."Reseller"
    WHERE "CityShape" IS NULL;
""")
rows = cursor.fetchall()

# Process each city
for city, state, country in rows:
    # Step 1: Geocode the city
    latitude, longitude = geocode_city(city, state, country)

    # Step 2: Fetch postal codes
    postal_codes = fetch_postal_codes(city, state, country)

    # Step 3: Fetch and dissolve postal code shapes
    city_shape = fetch_postal_code_shapes(postal_codes)

    # Step 4: Write the dissolved geometry to the database
    try:
        if city_shape:
            city_shape_wkt = to_wkt(city_shape)
            cursor.execute("""
                UPDATE "adventureworks"."Reseller"
                SET "CityShape" = ST_GeomFromText(%s, 4326), "Latitude" = %s, "Longitude" = %s
                WHERE "City" = %s AND "State-Province" = %s AND "Country-Region" = %s;
            """, (city_shape_wkt, latitude, longitude, city, state, country))
        else:
            print(f"Falling back to point geometry for {city}, {state}.")
            if latitude and longitude:
                point_wkt = f"POINT({longitude} {latitude})"
                cursor.execute("""
                    UPDATE "adventureworks"."Reseller"
                    SET "CityShape" = ST_GeomFromText(%s, 4326), "Latitude" = %s, "Longitude" = %s
                    WHERE "City" = %s AND "State-Province" = %s AND "Country-Region" = %s;
                """, (point_wkt, latitude, longitude, city, state, country))
        conn.commit()  # Commit after each update
    except Exception as e:
        print(f"Error updating database for {city}, {state}, {country}: {e}")
        conn.rollback()  # Roll back only the failed transaction

# Close the connection
cursor.close()
conn.close()
print("Processing completed.")
