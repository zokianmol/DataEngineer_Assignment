## ==> Database info
DB_FOLDER = "data"
DB_NAME = "proteins_db.sqlite"
DB_PATH = f"{DB_FOLDER}//{DB_NAME}"

## ==> Input data folders
INPUT_DATA_FOLDER = "input_data"

## ==> Uniprot datasource info
UNIPROT_DATA_FILE_NAME = "uniprot_sprot.xml.gz"
UNIPROT_DATA_PATH = f"{INPUT_DATA_FOLDER}//{UNIPROT_DATA_FILE_NAME}"
UNIPROT_STAGING_TABLE= "staging_uniprot"
UNIPROT_CLEANED_TABLE= "cleaned_uniprot"

## ==> String datasource info
STRING_DATA_FILE_NAME = "9606.protein.links.detailed.v12.0.txt.gz"
STRING_DATA_PATH = f"{INPUT_DATA_FOLDER}//{STRING_DATA_FILE_NAME}"
STRING_STAGING_TABLE= "staging_string"
STRING_CLEANED_TABLE= "cleaned_string"

## ==> Opentarget datasource info
OPENTARGET_PARENT_FOLDER = f'{INPUT_DATA_FOLDER}/target'
OPENTARGET_DATA_FOLDERS = ['association', 'diseases', 'targets']
OPENTARGET_FOLDER_PATHS = {
    'association'   :   f'{OPENTARGET_PARENT_FOLDER}/association',
    'diseases'      :   f'{OPENTARGET_PARENT_FOLDER}/diseases',
    'targets'       :   f'{OPENTARGET_PARENT_FOLDER}/targets'
}

OPENTARGET_STAGING_TABLES = {
    'association'   :   f'staging_opentarget_association',
    'diseases'      :   f'staging_opentarget_diseases',
    'targets'       :   f'staging_opentarget_targets'
}

OPENTARGET_CLEANED_TABLES = {
    'association'   :   f'cleaned_opentarget_association',
    'diseases'      :   f'cleaned_opentarget_diseases',
    'targets'       :   f'cleaned_opentarget_targets'
}


FINAL_SEMANTIC_TABLE = 'final_semantic_table_view'
