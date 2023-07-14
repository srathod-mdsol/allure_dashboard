import os
import pandas as pd
import sqlite3
import great_expectations as ge
from great_expectations.dataset import PandasDataset

import subprocess

def initialize_great_expectations():
    try:
        subprocess.run(["great_expectations", "init"], check=True, text=True, input="y")
        print("Great Expectations initialized successfully.")
    except subprocess.CalledProcessError as e:
        print("Error initializing Great Expectations:", e)
    
# Extract data from a source (e.g., CSV file)
def extract_data():
    df = pd.read_csv('data.csv')
    return df

# Transform data
def transform_data(data):
    # Perform data transformation operations
    transformed_data = data.dropna()
    return transformed_data

# Load data to a destination (e.g., a database)
def load_data(data):
    # Establish a connection to the SQLite database
    conn = sqlite3.connect('database.db')

    # Create a database table (if it doesn't exist)
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS water_stock (
    region TEXT,
    variable TEXT,
    RID INTEGER,
    yq REAL,
    value INTEGER,
    year INTEGER,
    Series TEXT,
    Unit TEXT,
    Source TEXT,
    Quarter INTEGER
)
    '''
    conn.execute(create_table_query)

    # Insert the transformed data into the database
    with conn:
        data.to_sql('water_stock', conn, if_exists='replace', index=False)

    # Close the database connection
    conn.close()

# Main ETL function
def etl_pipeline():
    # Step 1: Extract data
    extracted_data = extract_data()

    # Step 2: Transform data
    transformed_data = transform_data(extracted_data)

    # Initialize Great Expectations
    context = ge.data_context.DataContext()

    # Create a PandasDataset from the transformed data
    dataset = PandasDataset(transformed_data)

    # Define expectations
    expectation_suite = context.create_expectation_suite("my_expectations")

    # Expect the 'region' column to exist
    expectation_suite.expect_column_to_exist("region")

    # Expect the 'value' column to not have null values
    expectation_suite.expect_column_values_to_not_be_null("value")

    # Save the expectation suite
    context.save_expectation_suite(expectation_suite, "my_expectations")

    # Validate the transformed data
    result = dataset.validate(expectation_suite="my_expectations")

    # Check the validation status
    assert result.success, "Validation failed. Check the data quality."

if __name__ == '__main__':
    initialize_great_expectations()
    etl_pipeline()
