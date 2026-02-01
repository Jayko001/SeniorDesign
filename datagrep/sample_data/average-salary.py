# Import necessary libraries
import pandas as pd
import psycopg2
import json
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import os

# Define the file path
file_path = 'employees.csv'

# Read the CSV data
try:
    df = pd.read_csv(file_path)
except Exception as e:
    print(f"Error occurred while reading the CSV file: {e}")
    raise

# Validate the data
assert 'id' in df.columns, "Missing 'id' in CSV data"
assert 'name' in df.columns, "Missing 'name' in CSV data"
assert 'department_id' in df.columns, "Missing 'department_id' in CSV data"
assert 'salary' in df.columns, "Missing 'salary' in CSV data"

# Connect to the PostgreSQL database (Supabase requires SSL)
try:
    conn = psycopg2.connect(
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
        host=os.environ['POSTGRES_HOST'],
        port=os.environ['POSTGRES_PORT'],
        sslmode='require'
    )
except Exception as e:
    print(f"Error occurred while connecting to the database: {e}")
    raise

# Fetch department data from the database
try:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM departments")
        departments = cur.fetchall()
except Exception as e:
    print(f"Error occurred while fetching data from the database: {e}")
    raise

# Convert the department data into a DataFrame
df_departments = pd.DataFrame(departments)

# Merge the CSV data with the department data
# Use suffixes to handle the 'name' column conflict (employee name vs department name)
df_merged = pd.merge(df, df_departments, left_on='department_id', right_on='id', how='inner', suffixes=('_employee', '_dept'))

# Rename columns for clarity
df_merged = df_merged.rename(columns={
    'name_employee': 'employee_name',
    'name_dept': 'department_name'
})

# Select only the necessary columns
df_final = df_merged[['employee_name', 'salary', 'department_name']]

# Print the final result as JSON
print(json.dumps(df_final.to_dict(orient='records')))