from config_manager import load_config
from download_data_via_api import download_data

# Load config.yml
config = load_config()

raw_folder_path = config["raw_folder_path"]
file_names = config["file_name"]

for i in range(len(file_names)):
    df = download_data(config["zip_url"][i], config["header_url"][i], raw_folder_path,file_names[i])
    print(df.shape)

    # add logic to insert data into GCS