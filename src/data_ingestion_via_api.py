import os
import zipfile
import requests
import pandas as pd
from Logger import logger
from get_header_via_api import header_details

def download_and_process_data(zip_url, file_name, base_folder_path, header_url):
    try:
        logger.info(f"Starting full process for {file_name}")

        # Define and create the base directory (file_name folder)
        file_folder = os.path.join(base_folder_path, file_name)
        os.makedirs(file_folder, exist_ok=True)

        # Set ZIP file path directly inside file_name folder
        zip_path = os.path.join(file_folder, f"{file_name}.zip")

        # Download ZIP
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()
        with open(zip_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logger.info(f"ZIP file saved at {zip_path}")

        # Extract ZIP to the same folder
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(file_folder)

        logger.info(f"ZIP file extracted to {file_folder}")

        # Locate .txt file
        extracted_files = os.listdir(file_folder)
        txt_files = [f for f in extracted_files if f.endswith(".txt")]
        if not txt_files:
            logger.error(f"No .txt file found in {file_folder}")
            return None

        txt_file_path = os.path.join(file_folder, txt_files[0])

        # Get header from API
        header = header_details(header_url, base_folder_path, file_name)

        # Convert TXT to CSV
        df = pd.read_csv(txt_file_path, names=header, delimiter="|", dtype=str)
        csv_file_path = os.path.join(file_folder, f"{file_name}.csv")
        df.to_csv(csv_file_path, index=False)

        logger.info(f"CSV file saved at {csv_file_path}")
        logger.info(f"Process completed for {file_name}")

        return {
            "zip_path": zip_path,
            "csv_path": csv_file_path
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Download error for {file_name}: {e}")
    except zipfile.BadZipFile:
        logger.error(f"The file {zip_path} is not a valid ZIP.")
    except Exception as e:
        logger.error(f"Error in processing {file_name}: {e}")
