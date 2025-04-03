import os
import zipfile
import requests
import pandas as pd
from Logger import logger

def header_details(header_url, raw_folder_path,file_name):
    logger.info(f"Downloading header details start for {file_name}")
    
    absolute_path = os.path.join(raw_folder_path, file_name)
    
    # Download and save the file
    csv_path = absolute_path + "/" + file_name + ".csv"
    response = requests.get(header_url, stream=True)

    with open(csv_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    df = pd.read_csv(csv_path)
    headers = df.columns.to_list()
    logger.info(f"Downloading header details complete for {file_name}")
    
    return headers

