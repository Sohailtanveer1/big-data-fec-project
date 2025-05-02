from config_manager import load_config
from data_ingestion_via_api import download_and_process_data

# Load DEV config.yml
config = load_config()
base_folder_path = config["raw_folder_path"]
file_names = config["file_name"]

# get all API data to raw location
for i in range(len(file_names)):
    
    download_and_process_data(config["zip_url"][i], file_names[i], base_folder_path, config["header_url"][i])
    
    # add logic to insert data into GCS