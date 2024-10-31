import settings
import sqlite3
from tqdm import tqdm
import pandas as pd
from typing import Optional, List

def _dedupe_table(staging_table_name: str, cleaned_table_name:str, dedupe_field:str, selected_fields:Optional[List] = None) -> str:
    if selected_fields is not None:
        selected_fields = ', '.join([f'"{selected_field}"' for selected_field in selected_fields])
    else:
        selected_fields = "*"

    query = f"""
    CREATE TABLE {cleaned_table_name} AS
    WITH dedupe AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {dedupe_field}) AS rn
        FROM {staging_table_name}
    )
    SELECT {selected_fields}
    FROM dedupe
    WHERE rn = 1;
    """

    print(query)

    return query

def transform_and_clean_data(db_path: str):
    # ==> Remove duplicates and only pick relevant fields to clean_data_layer
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    #==> Uniprot data
    cursor.execute(f"""DROP TABLE IF EXISTS {settings.UNIPROT_CLEANED_TABLE}""")
    cursor.execute(_dedupe_table(
        staging_table_name=settings.UNIPROT_STAGING_TABLE,
        cleaned_table_name=settings.UNIPROT_CLEANED_TABLE,
        dedupe_field='"Primary Accession"'
    ))
    
    #==> String data
    cursor.execute(f"""DROP TABLE IF EXISTS {settings.STRING_CLEANED_TABLE}""")
    cursor.execute(_dedupe_table(
        staging_table_name=settings.STRING_STAGING_TABLE,
        cleaned_table_name=settings.STRING_CLEANED_TABLE,
        dedupe_field='"protein1", "protein2"',
        selected_fields=['protein1', 'protein2', 'combined_score']
    ))

    # #==> Opentarget data
    cursor.execute(f"""DROP TABLE IF EXISTS {settings.OPENTARGET_CLEANED_TABLES['association']}""")
    cursor.execute(_dedupe_table(
        staging_table_name=settings.OPENTARGET_STAGING_TABLES['association'],
        cleaned_table_name=settings.OPENTARGET_CLEANED_TABLES['association'],
        dedupe_field='"diseaseId","targetId"'
    ))

    cursor.execute(f"""DROP TABLE IF EXISTS {settings.OPENTARGET_CLEANED_TABLES['diseases']}""")
    cursor.execute(_dedupe_table(
        staging_table_name=settings.OPENTARGET_STAGING_TABLES['diseases'],
        cleaned_table_name=settings.OPENTARGET_CLEANED_TABLES['diseases'],
        dedupe_field='id',
        selected_fields=['id', 'name']
    ))

    cursor.execute(f"""DROP TABLE IF EXISTS {settings.OPENTARGET_CLEANED_TABLES['targets']}""")
    cursor.execute(_dedupe_table(
        staging_table_name=settings.OPENTARGET_STAGING_TABLES['targets'],
        cleaned_table_name=settings.OPENTARGET_CLEANED_TABLES['targets'],
        dedupe_field='id',
        selected_fields=['id', 'approvedSymbol', 'biotype']
    ))

    conn.commit()
    conn.close()


def create_semantic_layer(db_path: str):
    # ==> Remove duplicates and only pick relevant fields to clean_data_layer
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""DROP TABLE IF EXISTS {settings.FINAL_SEMANTIC_TABLE}""")
    cursor.execute(
        f"""
        CREATE TABLE {settings.FINAL_SEMANTIC_TABLE} AS
        WITH opentaget_substage_table AS (
            -- Merging the opentarget tables on targetId and diseaseId respectively
            SELECT 
                opentarget_association.*,
                opentarget_diseases.name,
                opentarget_targets.approvedSymbol,
                opentarget_targets.biotype

            FROM {settings.OPENTARGET_CLEANED_TABLES['association']} AS opentarget_association
            LEFT JOIN {settings.OPENTARGET_CLEANED_TABLES['diseases']} AS opentarget_diseases ON opentarget_association.diseaseId = opentarget_diseases.id
            LEFT JOIN {settings.OPENTARGET_CLEANED_TABLES['targets']} AS opentarget_targets ON opentarget_targets.id = opentarget_association.targetId
        ),
        filtered_string_data AS (
            -- Filtering the string_data where combined_score > 200
            SELECT 
                protein1,
                protein2,
                combined_score
            FROM {settings.STRING_DATA_TABLE}
            WHERE combined_score > 200
        )
        SELECT 
            uniprot.*,
            opentaget_substage_table.*,
            filtered_string_data.protein2 AS associated_protein,
            filtered_string_data.combined_score AS association_score

        FROM {settings.UNIPROT_CLEANED_TABLE} AS uniprot
        LEFT JOIN opentaget_substage_table 
            ON uniprot."Primary Accession" = opentaget_substage_table.diseaseId
        LEFT JOIN filtered_string_data 
            ON uniprot."Primary Accession" = filtered_string_data.protein1
        """
    )
