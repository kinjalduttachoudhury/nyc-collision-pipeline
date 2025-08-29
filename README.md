# nyc-collision-pipeline
A data pipeline that retrieves and combines data from two different sources – a public open dataset and a web API – to create an integrated dataset for analysis.

## Data Pipeline
There are three steps in the data pipeline
1. Data Fetching  
In this step we fetch the data for collisions and holidays from the corresponding APIs and store the data in parquet files inside `raw_data`. 
2. Data Merging and Transformation  
In this step we integrate the data from both the sources and perform data cleaning and transformation to make it easy to query for insights and analysis.
3. Saving Final Table as Parquet  
In this step we take the table having the transformed data and store it in a parquet file in `output_data`

## Steps to run the pipeline
- Clone the repository  
- Navigate to the repo directory
```bash
cd nyc-collision-pipeline
```
- Create a virtual environment  
```bash
python3 -m venv venv
```
- Activate the virtual environment
```bash
source venv/bin/activate
```
- We can run the pipeline in two different ways  
    - Run `main.py`       
        ```bash
        python3 main.py
        ```
        `main.py` file will run all the three steps of the pipeline mentioned [here](#data-pipeline)  
        The directory `raw_data` will have `collisions_2022.parquet` and `holidays_2022.parquet` with the raw data fetched from the APIs  
        The directory `output_data` will have the final transformed data `transformed_data.parquet`  


    - Alternatively we can run each step separately  
        ```bash
        python3 -m src.fetch_data
        ```
        This will store `collisions_2022.parquet` and `holidays_2022.parquet` with the raw data fetched from the APIs in the directory `raw_data`
        ```bash
        python3 -m src.transformer
        ```
        This will perform the join, cleaning, transformations and store the data in the table `transformed_data` in duckdb
        ```bash
        python3 -m src.save_parquet
        ```
        This will save the data in `transformed_data` table to the file `transformed_data.parquet` file in the directory `output_data`  

**Note** - Our code will create a database file `pipeline.duckdb` when we connect to the DuckDB database. This file can be used to explore all the tables stored in the database.

## Generated Parquet File
The generated parquet file for the final transformed data can be found in `output_data/transformed_data.parquet`  
Alternatively, it can be generated too by following the steps [here](#steps-to-run-the-pipeline)

## Analysis
The file `analysis/collisions_insights.ipynb` has the analysis and insights