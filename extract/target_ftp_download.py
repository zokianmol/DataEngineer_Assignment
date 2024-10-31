from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import os

# url = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl/parquet/targets/"
url = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl/parquet/diseases/"
# url = "https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/24.09/output/etl/parquet/associationByOverallDirect/"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")

# Create a directory to store the downloaded files
os.makedirs("input_data/target/diseases", exist_ok=True)

for link in tqdm(soup.find_all("a")):
    href = link.get("href")
    if href.endswith(".parquet"):
        file_url = url + href
        print(f"Downloading {file_url}")
        file_response = requests.get(file_url)
        with open(os.path.join("input_data/target/diseases", href), "wb") as f:
            f.write(file_response.content)

print("Download completed.")
