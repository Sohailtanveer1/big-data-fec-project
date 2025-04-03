import os
import zipfile
import requests
import pandas as pd
from Logger import logger
from get_header_via_api import header_details


def download_data(zip_url, header_url, raw_folder_path, file_name):

    # Ensure proper path joining
    absolute_path = os.path.join(raw_folder_path, file_name)

    if not os.path.exists(absolute_path):
        os.makedirs(absolute_path)

    zip_path = absolute_path + "/" + file_name + ".zip"

    logger.info(f"Downloading start for {file_name} at {zip_path}")

    # Download the ZIP file
    response = requests.get(zip_url, stream=True)
    with open(zip_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    # Extract ZIP file
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(absolute_path)

    # Get list of extracted files
    extracted_files = os.listdir(absolute_path)
    txt_files = [f for f in extracted_files if f.endswith(".txt")]

    if not txt_files:
        logger.error(f"No .txt file found in {absolute_path}")
        return None  # Return None to handle missing files gracefully

    data_file = txt_files[0]  # Select the first .txt file
    file_path = os.path.join(absolute_path, data_file)

    # Get header details (Ensure this function is defined correctly)
    header = header_details(header_url, raw_folder_path, file_name)
    df = pd.read_csv(file_path, names=header, delimiter="|", dtype=str)

    logger.info(f"Downloading complete for {file_name}")

    return df