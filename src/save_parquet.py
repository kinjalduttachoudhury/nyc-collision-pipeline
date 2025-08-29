import os
from src.utils import load_config
from src.db_manager import DuckDBManager

def save_data_as_parquet(table_name, parquet_file):
    """
    Saves the duckdb table as a parquet file
    """
    try:
        db = DuckDBManager()
        os.makedirs(os.path.dirname(parquet_file), exist_ok=True)

        # saves the data in transformed table as a parquet file
        db.save_table_as_parquet(table_name, parquet_file)
        print(f"Table {table_name} saved in {parquet_file}")

    except Exception as e:
        print(f"Error while saving final table to parquet: {e}")

    finally:
        # closing the db connection
        db.close()
        print("DB connection closed")


if __name__ == "__main__":
    config = load_config()

    table_name = config['database']['table_name']
    parquet_file = os.path.join(
        config['paths']['output_dir'], 
        config['files']['output_file']
    )
    try:
        save_data_as_parquet(table_name, parquet_file)
    except Exception as e:
        print(f"Table saving as parquet failed: {e}")