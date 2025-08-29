import os
from src.db_manager import DuckDBManager
from src.utils import load_config

def merge_data(collision_file, holiday_file, table_name, sql_file):
    """
    Merges collisions and holidays data based on dates and performs cleaning, transformations for analysis
    """
    try:
        db = DuckDBManager()

        # create duckdb tables from the stored parquet files
        db.run_query(f"create or replace table collisions as select * from read_parquet('{collision_file}')")
        db.run_query(f"create or replace table holidays as select * from read_parquet('{holiday_file}')")
        print(f"Tables created in duckdb from {collision_file} and {holiday_file}")

        # performs join, cleaning and transformation based on the sql file query
        with open(sql_file, 'r') as file:
            transform_query = file.read()
        transform_query = transform_query.format(table_name=table_name)

        db.run_query(transform_query)
        print(f"Transformation done based on {sql_file}")
        
        # creates an index on the crash_date column for faster data retrieval
        db.run_query("create index idx_crash_date on {table_name} (crash_date)".format(table_name=table_name))
        print(f"Index created on {table_name}")
    
    except Exception as e:
        print(f"Error while performing data transformation: {e}")

    finally:
        # closing the db connection
        db.close()
        print("DB connection closed")

if __name__ == "__main__":
    config = load_config()
    year = config['data']['year']

    collision_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['collision_file'].format(year=year)
    )
    holiday_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['holiday_file'].format(year=year)
    )
    transform_sql_file = os.path.join(
        config['paths']['sql_queries_dir'], 
        config['files']['transform_query']
    )
    table_name = config['database']['table_name']

    try:
        merge_data(collision_file, holiday_file, table_name, transform_sql_file)
    except Exception as e:
        print(f"Data transformation failed: {e}")