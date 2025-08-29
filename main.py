import os
from src.fetch_data import fetch_collisions, fetch_holidays
from src.transformer import merge_data
from src.save_parquet import save_data_as_parquet
from src.utils import load_config

    
def main():
    config = load_config()
    
    year = config['data']['year']
    country = config['data']['country']

    # parquet file for storing collisions named with year. Eg : collisions_2022.parquet
    collision_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['collision_file'].format(year=year)
    )

    # parquet file for storing holidays named with year. Eg : holidays_2022.parquet
    holiday_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['holiday_file'].format(year=year)
    )

    collision_base_url = config['apis']['collision']['base_url']
    holiday_base_url = config['apis']['holiday']['base_url'].format(year=year, country=country)

    # query to filter collsions data for a year
    collision_query = config['apis']['collision']['params']['query'].format(year=year) 



    # Load the data for collisions and holidays into raw source files
    try:
        total_collisions = fetch_collisions(collision_file, collision_base_url, collision_query)
        total_holidays = fetch_holidays(holiday_file, holiday_base_url)
        print(f"Extracted {total_collisions} collisions and {total_holidays} holidays for {year}")
    except Exception as e:
        print(f"Data extraction failed: {e}")
        return
    
    # merge the data from both the files, transform and store the data in a table
    transform_sql_file = os.path.join(
        config['paths']['sql_queries_dir'], 
        config['files']['transform_query']
    )
    table_name = config['database']['table_name']
    try:
        merge_data(collision_file, holiday_file, table_name, transform_sql_file)
    except Exception as e:
        print(f"Data transformation failed: {e}")
        return

    # save the transformed data into a parquet file
    parquet_file = os.path.join(
        config['paths']['output_dir'], 
        config['files']['output_file']
    )
    try:
        save_data_as_parquet(table_name, parquet_file)
    except Exception as e:
        print(f"Table saving as parquet failed: {e}")
        return

if __name__ == "__main__":
    main()