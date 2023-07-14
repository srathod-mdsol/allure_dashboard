import pytest
import allure
import pandas as pd
import sqlite3

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

    # Step 3: Load data
    load_data(transformed_data)

@pytest.fixture(scope="module")
def data():
    # Set up test data
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    return transformed_data

@allure.feature("ETL Pipeline")
class TestETLPipeline:

    @allure.story("Extract Data")
    def test_extract_data(self):
        data = extract_data()
        assert isinstance(data, pd.DataFrame)
        assert not data.empty

    @allure.story("Transform Data")
    def test_transform_data(self, data):
        assert isinstance(data, pd.DataFrame)
        assert not data.empty

    @allure.story("Load Data")
    def test_load_data(self, data):
        load_data(data)
        # Add any additional assertions here if necessary

if __name__ == '__main__':
    pytest.main(['-s', '-v', '--alluredir', 'allure-results'])
