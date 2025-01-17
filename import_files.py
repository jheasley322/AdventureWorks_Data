import os
import yaml
import pandas as pd
import psycopg2
from psycopg2 import sql

# Load configuration from YAML file
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Database connection function
def connect_to_db(db_config):
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

# Function to create the defects table
def create_defects_table(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS defects (
                id SERIAL PRIMARY KEY,
                table_name TEXT,
                line_number INT,
                contents TEXT,
                error_message TEXT
            )
        """)
        conn.commit()
        print("Defects table ensured.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating defects table: {e}")
    finally:
        cursor.close()

# Function to log defects
def log_defect(conn, table_name, line_number, contents, error_message):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO defects (table_name, line_number, contents, error_message)
            VALUES (%s, %s, %s, %s)
        """, (table_name, line_number, contents, error_message))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error logging defect: {e}")
    finally:
        cursor.close()

# Function to create a table and insert data
def create_table_and_insert_data(conn, table_name, df):
    cursor = conn.cursor()
    columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
    create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table} ({columns})").format(
        table=sql.Identifier(table_name),
        columns=sql.SQL(columns)
    )
    try:
        cursor.execute(create_table_query)
        conn.commit()
        for _, row in df.iterrows():
            insert_query = sql.SQL("INSERT INTO {table} VALUES ({values})").format(
                table=sql.Identifier(table_name),
                values=sql.SQL(", ").join(sql.Placeholder() * len(row))
            )
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Table {table_name} processed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table or inserting data for {table_name}: {e}")
    finally:
        cursor.close()

# Function to handle bad lines
def handle_bad_line(conn, table_name, line_number, line, error_message):
    print(f"Skipped line {line_number} in table {table_name}: {error_message}")
    log_defect(conn, table_name, line_number, line, error_message)

# Main script
if __name__ == "__main__":
    # Load configuration
    config = load_config('config.yaml')
    db_config = config['database']
    csv_directory = config['paths']['csv_directory']
    csv_delimiter = config['csv_settings']['delimiter']

    conn = connect_to_db(db_config)
    if not conn:
        exit()

    # Ensure defects table exists
    create_defects_table(conn)

    # Process each CSV file
    for file_name in os.listdir(csv_directory):
        if file_name.endswith('.csv'):
            table_name = os.path.splitext(file_name)[0]
            csv_path = os.path.join(csv_directory, file_name)
            
            skipped_lines = []
            
            def process_bad_line(line):
                line_number = len(skipped_lines) + 1
                error_message = "Malformed line detected."
                handle_bad_line(conn, table_name, line_number, line, error_message)
                skipped_lines.append(line)
                return None  # Skip the line
            
            try:
                df = pd.read_csv(
                    csv_path,
                    delimiter=csv_delimiter,
                    encoding='utf-8',
                    on_bad_lines=process_bad_line,
                    engine='python'  # Use Python engine for handling bad lines
                )
                create_table_and_insert_data(conn, table_name, df)
            except Exception as e:
                print(f"Error processing file {file_name}: {e}")

    conn.close()