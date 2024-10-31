# ==> Author: Anmolpreet Singh
# ==> This is the main function which extracts, loads and transforms the data from three different sources

from load_and_transform.load_and_transform import (
    load_opentarget_data_to_staging,
    load_string_data_to_staging,
    load_uniprot_data_to_staging
)
from load_and_transform.transform_data import (
    transform_and_clean_data,
    create_semantic_layer
)
import settings

# Load UniProt data into the staging area
load_uniprot_data_to_staging(
    data_path=settings.UNIPROT_DATA_PATH,
    db_path=settings.DB_PATH,
    table_name= settings.UNIPROT_STAGING_TABLE, 
    sample_size= 1000000)

print('uniprot data ingested')

# Load STRING data into the staging area
load_string_data_to_staging(
    data_path= settings.STRING_DATA_PATH,
    db_path = settings.DB_PATH,
    table_name= settings.STRING_STAGING_TABLE
)

print('string data ingested')

# Load OpenTargets data for each specified folder in settings
for opentarget_data_folder in settings.OPENTARGET_DATA_FOLDERS:
    load_opentarget_data_to_staging(data_folder=settings.OPENTARGET_FOLDER_PATHS[opentarget_data_folder], 
                                    db_path=settings.DB_PATH, 
                                    table_name=settings.OPENTARGET_STAGING_TABLES[opentarget_data_folder])
    
    print(f'opentarget: {opentarget_data_folder} data ingested')

# Transform and clean the ingested data
transform_and_clean_data(settings.DB_PATH)

# Create a semantic layer on the cleaned data
create_semantic_layer(settings.DB_PATH)

