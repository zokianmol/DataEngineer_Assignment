import os
import sqlite3
import pandas as pd
import gzip
from tqdm import tqdm
from typing import Optional, List
from extract.uniprot_parser import parse_uniprot_xml_gz

def load_string_data_to_staging(data_path:str, db_path:str, table_name:str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS staging_string ("protein1" TEXT, "protein2" TEXT, "combined_score" INT)')

    with gzip.open(data_path, 'rt') as f:
        df = pd.read_csv(f, sep=' ' )
    
    df.to_sql(table_name, conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()

def load_uniprot_data_to_staging(data_path:str, db_path:str, table_name:str, sample_size:Optional[int]=None):
    # Connect to SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            "Primary Accession" TEXT,
            "Recommended Protein Name" TEXT,
            "Primary Gene Name" TEXT,
            "Species Common Name" TEXT,
            "STRING dbReference" TEXT,
            "OpenTargets dbReference" TEXT,
            "Sequence Length" INT,
            "Sequence Mass" INT
        )
    ''')

    for idx, entry in enumerate(tqdm(parse_uniprot_xml_gz(data_path))):
        if sample_size is not None and idx > sample_size:
            break

        # Define keys, placeholders, and values dynamically
        keys = ', '.join([f'"{key}"' for key in entry.keys()])
        placeholders = ', '.join(['?' for _ in entry.keys()])
        values = tuple(entry.get(key, None) for key in entry.keys())  # Use None for missing fields

        try:
            # Insert each entry as a row in the specified table
            cursor.execute(f'INSERT INTO {table_name} ({keys}) VALUES ({placeholders})', values)
        except sqlite3.Error as e:
            print(f"Error inserting data: {e}")
            print(f"Problematic entry: {entry}")

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

def load_opentarget_data_to_staging(data_folder: str, db_path: str, table_name: str):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    parquet_files = [f"{data_folder}/{file}" for file in os.listdir(data_folder) if file.endswith('.parquet')]
    # Iterate over all Parquet files in the specified folder

    # Check if the table exists and create it if not
    first_file = parquet_files[0] if len(parquet_files) > 0 else None
    if first_file:
        # Read schema from the first file
        df = pd.read_parquet(first_file)
        columns = ", ".join([f'"{col}" TEXT' for col in df.columns])  # Set all columns as TEXT for simplicity
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
        conn.commit()
    
    for file in tqdm(parquet_files, desc="Loading parquet files to sqlite database", total = len(parquet_files)):
        # Read the Parquet file into a DataFrame
        df = pd.read_parquet(file)
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, dict)).any():
                df[col] = df[col].apply(str)
        
        # Append DataFrame to the SQLite table
        df.to_sql(table_name, conn, if_exists="append", index=False)

    # Close the database connection
    conn.close()

    
#     # Additional transformations and joins go here...

# def populate_semantic_layer():
#     query = """
#     INSERT INTO semantic_protein_data
#     SELECT uniprot.*, opentargets.disease, string.associated_protein
#     FROM clean_uniprot AS uniprot
#     LEFT JOIN clean_opentargets AS opentargets ON uniprot.protein_id = opentargets.protein_id
#     LEFT JOIN clean_string AS string ON uniprot.protein_id = string.protein_id
#     """
#     cursor.execute(query)
#     conn.commit()

# # Workflow execution
# create_tables()
# load_data_to_staging("path_to_uniprot_data.tsv", "staging_uniprot")
# load_data_to_staging("path_to_string_data.tsv", "staging_string")
# load_data_to_staging("path_to_opentargets_data.tsv", "staging_opentargets")
# transform_and_clean_data()
# populate_semantic_layer()

# # Close connection
# conn.close()
