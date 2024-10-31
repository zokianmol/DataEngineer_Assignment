import gzip
from lxml import etree

def parse_uniprot_xml_gz(file_path):
    """
    Parses a large UniProt XML .gz file and extracts specified fields.

    Parameters:
    - file_path (str): Path to the .xml.gz file.

    Yields:
    - A dictionary containing the extracted fields for each entry.
    """
    with gzip.open(file_path, 'rb') as f:
        context = etree.iterparse(f, events=('end',), tag='{http://uniprot.org/uniprot}entry')
        
        for _, elem in context:
            entry_data = {}

            # Extract Primary Accession
            accession = elem.findtext('{http://uniprot.org/uniprot}accession')
            if accession:
                entry_data['Primary Accession'] = accession
            
            # Extract Recommended Protein Name
            protein = elem.find('.//{http://uniprot.org/uniprot}recommendedName/{http://uniprot.org/uniprot}fullName')
            if protein is not None:
                entry_data['Recommended Protein Name'] = protein.text

            # Extract Primary Gene Name
            gene = elem.find('.//{http://uniprot.org/uniprot}gene/{http://uniprot.org/uniprot}name[@type="primary"]')
            if gene is not None:
                entry_data['Primary Gene Name'] = gene.text

            # Extract Species Common Name
            species = elem.find('.//{http://uniprot.org/uniprot}organism/{http://uniprot.org/uniprot}name[@type="common"]')
            if species is not None:
                entry_data['Species Common Name'] = species.text

            # Extract STRING dbReference
            for db_ref in elem.findall('.//{http://uniprot.org/uniprot}dbReference'):
                if db_ref.get('type') == 'STRING':
                    entry_data['STRING dbReference'] = db_ref.get('id')
                elif db_ref.get('type') == 'OpenTargets':
                    entry_data['OpenTargets dbReference'] = db_ref.get('id')

            # Extract Sequence Length and Mass
            sequence = elem.find('{http://uniprot.org/uniprot}sequence')
            if sequence is not None:
                entry_data['Sequence Length'] = sequence.get('length')
                entry_data['Sequence Mass'] = sequence.get('mass')

            yield entry_data

            elem.clear()  # Clear element to free memory
            while elem.getprevious() is not None:
                del elem.getparent()[0]  # Remove previous elements to free memory

        del context  # Clean up the context after finishing


# if not os.path.exists('data'):
#     os.makedirs('data')

# # Example usage:
# iterator = 0
# MAX_SIZE = 10

# for entry in tqdm(parse_uniprot_xml_gz('uniprot_sprot.xml.gz'), total=MAX_SIZE):
#     if iterator > MAX_SIZE:
#         break
#     with open('data/data.ndjson', 'a') as out_file:
#         out_file.write(json.dumps(entry) + '\n')
#     iterator += 1

