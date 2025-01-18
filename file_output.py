import os
import yaml
import pandas as pd
from sqlalchemy import create_engine

def load_config(config_file="config.yaml"):
    """Load configuration from YAML file."""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def create_subdirectories(base_dir):
    """Create subdirectories for Parquet, JSON, and CSV outputs."""
    parquet_dir = os.path.join(base_dir, "parquet")
    json_dir = os.path.join(base_dir, "json")
    csv_dir = os.path.join(base_dir, "csv")
    os.makedirs(parquet_dir, exist_ok=True)
    os.makedirs(json_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)
    return parquet_dir, json_dir, csv_dir

def export_table(engine, schema, table_name, parquet_dir, json_dir, csv_dir, delimiter):
    """Export a single table to Parquet, JSON, and CSV formats."""
    print(f"Exporting table: {schema}.{table_name}")
    query = f'SELECT * FROM "{schema}"."{table_name}"'
    df = pd.read_sql(query, engine)

    # Parquet
    parquet_file = os.path.join(parquet_dir, f"{table_name}.parquet")
    df.to_parquet(parquet_file, index=False)
    
    # JSON
    json_file = os.path.join(json_dir, f"{table_name}.json")
    df.to_json(json_file, orient="records", indent=4)
    
    # CSV
    csv_file = os.path.join(csv_dir, f"{table_name}.csv")
    df.to_csv(csv_file, index=False, sep=delimiter)

def export_database(config):
    """Export all tables in the specified schema of the database."""
    # Load settings
    db_config = config["database"]
    schema = config.get("schema", "adventureworks")  # Default schema is 'adventureworks'
    base_path = config["paths"]["output_directory"]
    delimiter = config.get("csv_settings", {}).get("delimiter", ",")

    # Build database URL
    database_url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    
    # Create database engine
    engine = create_engine(database_url)
    
    # Create output directories
    parquet_dir, json_dir, csv_dir = create_subdirectories(base_path)
    
    # Get the list of tables in the specified schema
    query = f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{schema}';
    """
    tables = pd.read_sql(query, engine)["table_name"].tolist()
    
    # Export each table
    for table in tables:
        export_table(engine, schema, table, parquet_dir, json_dir, csv_dir, delimiter)
    
    print(f"Export completed. Data saved in {base_path}")

if __name__ == "__main__":
    config = load_config("config.yaml")
    export_database(config)