import pandas as pd
import psycopg2
import streamlit

# Read the file data into a DataFrame
file_path = 'C:/Users/ashish.kshirsagar/Downloads/1000-Sales-Records/1000 Sales Records.csv'  # Replace with your file path
df = pd.read_csv(file_path)

# Function to map file data types to PostgreSQL data types
def map_data_types(file_data_type):
    data_type_mapping = {
        'int64': 'INTEGER',
        'float64': 'NUMERIC',
        'object': 'TEXT'  # You may need to handle other object types based on your data
    }
    return data_type_mapping.get(file_data_type, 'TEXT')   # Default to TEXT for unrecognized types

# Get the file data types and map to PostgreSQL data types
pg_data_types = [map_data_types(str(df[col].dtype)) for col in df.columns]

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="fR!UVs9M37999rjG",
    host="hackthon2024-codingninjas.csc17vwdnwfj.us-east-1.rds.amazonaws.com",
    port="5432"
)

# Create a cursor
cur = conn.cursor()

# Create the PostgreSQL table with the mapped data types
table_name = 'sales_1'
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} {pg_type}' for col, pg_type in zip(df.columns, pg_data_types)])})"
cur.execute(create_table_query)

# Insert the file data into the PostgreSQL table
for index, row in df.iterrows():
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
    try:
        print("inserting rows into database")
        cur.execute(insert_query, tuple(row))
    except:
        print("Error while matching the table names")

# Commit the changes and close the connection
print("File data inserted into PostgreSQL database successfully.")
conn.commit()
conn.close()

print("File data inserted into PostgreSQL database successfully.")
