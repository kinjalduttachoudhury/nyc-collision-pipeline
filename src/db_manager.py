import duckdb

class DuckDBManager:
    def __init__(self, db_path="pipeline.duckdb"):
        """
        Opens a duckdb connection
        """
        self.db_path = db_path
        self.conn = duckdb.connect(self.db_path)

    def run_query(self, query):
        """
        runs a SQL query against the connection on duckdb
        """
        return self.conn.execute(query).df()
    
    def save_table_as_parquet(self, table_name, output_file):
        """
        Runs a query to copy data from a duckdb table to a parquet file
        """
        query = "copy {table_name} to '{output_file}' (format 'parquet');".format(table_name=table_name, output_file=output_file)
        self.conn.execute(query)
        

    def close(self):
        """
        Gracefully closes the db connection
        """
        self.conn.close()
    
