import os
import pandas as pd
import requests
from io import StringIO
from src.utils import load_config
import fastparquet as fp

def fetch_collisions(output_file,  base_url, collision_query, batch_size=20000):
    """
    Fetches data for collisions and stores in a parquet file 
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # removes the file if it already exists
    if os.path.exists(output_file):
        os.remove(output_file)

    offset = 0
    total_rows = 0
    append_flag = False
    try:
        # fetch data in batches of 20,000 and write each chunk to the output parquet file
        while True:
            resp = requests.get(
                base_url,
                params={
                    "$where": collision_query,
                    "$limit": batch_size,
                    "$offset": offset
                }
            )
            if resp.status_code != 200:
                print(f"Request failed: {resp.status_code} - {resp.text}")
                break

            chunk = pd.read_csv(StringIO(resp.text))
            if chunk.empty:
                break

            try:
                fp.write(output_file, chunk, append=append_flag)
                append_flag = True
            except Exception as e:
                print(f"Error writing to Parquet file: {e}")

            total_rows += len(chunk)
            offset += batch_size
            print(f"Fetched {len(chunk)} rows (total={total_rows})")

    except Exception as e:
        print(f"Error fetching collisions data: {e}")

    return total_rows

def fetch_holidays(output_file, base_url) :
    """
    Fetches data for public holidays and stores in a parquet file
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # removes the file if it already exists
    if os.path.exists(output_file):
        os.remove(output_file)

    try:
        # fetches data for public holidays and writes to parquet file
        resp = requests.get(base_url)      
        if resp.status_code == 200:
            chunk = pd.DataFrame(resp.json())
            try:
                fp.write(output_file, chunk)
            except Exception as e:
                print(f"Error writing to Parquet file: {e}")

        else:
            print(f"Error: {resp.status_code} - {resp.text}")
            return 0

    except Exception as e:
        print(f"Error fetching holidays data: {e}")

    return len(chunk)

if __name__ == "__main__":
    config = load_config()

    year = config['data']['year']
    country = config['data']['country']

    collision_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['collision_file'].format(year=year)
    )
    holiday_file = os.path.join(
        config['paths']['raw_data_dir'], 
        config['files']['holiday_file'].format(year=year)
    )

    collision_base_url = config['apis']['collision']['base_url']
    collision_query = config['apis']['collision']['params']['query'].format(year=year)
    holiday_base_url = config['apis']['holiday']['base_url'].format(year=year, country=country)

    try:
        total_collisions = fetch_collisions(collision_file, collision_base_url, collision_query)
        total_holidays = fetch_holidays(holiday_file, holiday_base_url)
        print(f"Extracted {total_collisions} collisions and {total_holidays} holidays for {year}")
    except Exception as e:
        print(f"Data extraction failed: {e}")