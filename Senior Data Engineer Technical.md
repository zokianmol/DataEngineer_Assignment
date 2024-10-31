# Senior Data Engineer Technical

#### Intro
In this task, we will focus is on the quality and clarity of the code rather than solely on its functionality. We highly value well-written code that adheres to best practices and promotes maintainability. Additionally, we expect the code to be designed in a way that allows for easy extension, reuse and integration with other components. Evidence of unit tests and documentation should be included.

In this technical we would like you to build a ELT pipeline with some open source datasets using Python and SQL.  The 3 datasets are the UniProt SwissProt data, the STRING protein interactions data and the OpenTargets disease associations data. We would like you load them into a SQL database of choice SQLite is recommended. Inputs should be parsed to tabular data where needed. 

The database should feature a data model containing a Staging area/Landing zone, a intermediate clean data layer and a semantic layer where an Analyst or Data Scientist can access the data .

In the semantic layer the end result should be a single table containing all data from UniProt about a single entry, linked to the OpenTarget data about its directly associated diseases and a list of other proteins with which it is strongly associated (STRING combinedscore > 200). 

There 3 data sources we would like you to download and parse are 

#### Uniprot 
This can be access via an FTP server. The data is available in xml and text gzipped formats. We recommend the xml which can be parsed using the python lxml library in a for loop using a generator. Note that due to the size of the data you will have to clear entries after they are used to avoid a MemoryError. 

Downloads Documentation Server: https://www.uniprot.org/help/downloads

XML FTP Link: https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz


From the data we want you to extract the following fields. Examples of values have been included using https://rest.uniprot.org/uniprotkb/P05067.xml

- Primary Accession: P05067
- Recommended Protein Name: Amyloid-beta precursor protein
- Primary Gene Name: APP
- Species Common Name: Human
- STRING dbReference: 9606.ENSP00000284981
- OpenTargets dbReference: ENSG00000142192
- Sequence Length: 770
- Sequence Mass: 86943

#### STRING 
This is available as a gzipped tab separated csv. To keep the filesize small it is recommended to just download the "homo sapiens" data 

Data: https://string-db.org/cgi/download?sessionId=baXq4yzPPB1H&species_text=Homo+sapiens

From the data we want you to extract the following fields.

- protein1("9606.ENSP00000000233") 
- protein2 ("9606.ENSP00000379496")
- combined_score ("155")

#### OpenTargets

This data is available via either a FTP web download, Public S3 bucket or a Public Google Cloud bucket as parquet files. From this you will want the Targets, Direct Associations and Diseases data 

Documentation: https://platform.opentargets.org/downloads

From the data we want you to extract the following fields.

###### Targets:
- id 
- approvedSymbol
- biotype

Diseases
- id
- name

Direct Association:
- All Columns



#### How to Combine 

The UniProt dbReference fields directly link to the one targets data.
