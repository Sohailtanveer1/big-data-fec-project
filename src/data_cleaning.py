import pandas as pd
import json
import numpy as np
import os
from Logger import logger

# Define file paths
base_folder_path = "D:/DE_Project_FEC/fec_env/big-data-fec-project/data/raw"
file_name = "candidate_master"
csv_path = os.path.join(base_folder_path, file_name, f"{file_name}.csv")
json_path = "C:/Users/mdsoh/Downloads/USCities.json"

logger.info(f"Data processing started for: {file_name}")

# Load candidate master CSV
try:
    #cand_master_df = pd.read_csv(csv_path)
    cand_master_Df = spark.read('csv').option('header',True).load(csv_path)
    logger.info(f"Successfully loaded {file_name}.csv from {csv_path}")
except FileNotFoundError:
    logger.error(f"{file_name}.csv not found at path: {csv_path}")
    raise
except pd.errors.EmptyDataError:
    logger.error(f"{file_name}.csv is empty or corrupted at: {csv_path}")
    raise

# Fill null values
cand_master_df['CAND_PTY_AFFILIATION'] = cand_master_df['CAND_PTY_AFFILIATION'].fillna('NNE')
logger.info("Filled null values in CAND_PTY_AFFILIATION with 'NNE'")

cand_master_df['CAND_OFFICE_DISTRICT'] = cand_master_df['CAND_OFFICE_DISTRICT'].fillna(0.0).astype(int)
logger.info("Filled nulls in CAND_OFFICE_DISTRICT with 0 and converted to int")

# Fill missing CAND_ICI
unique_ici = cand_master_df['CAND_ICI'].dropna().unique()
if unique_ici.size > 0:
    cand_master_df['CAND_ICI'] = cand_master_df['CAND_ICI'].apply(
        lambda x: np.random.choice(unique_ici) if pd.isna(x) else x
    )
    logger.info("Filled nulls in CAND_ICI with random choices")

# Fill missing CAND_PCC
unique_pcc = cand_master_df['CAND_PCC'].dropna().unique()
if unique_pcc.size > 0:
    cand_master_df['CAND_PCC'] = cand_master_df['CAND_PCC'].apply(
        lambda x: np.random.choice(unique_pcc) if pd.isna(x) else x
    )
    logger.info("Filled nulls in CAND_PCC with random choices")

# Drop rows where CAND_CITY is null
initial_count = len(cand_master_df)
cand_master_df.dropna(subset=['CAND_CITY'], inplace=True)
logger.info(f"Dropped {initial_count - len(cand_master_df)} rows with null CAND_CITY")

# Load city-zip JSON
try:
    with open(json_path, "r") as f:
        city_zip_data = json.load(f)
    logger.info(f"Successfully loaded USCities.json from {json_path}")
except FileNotFoundError:
    logger.error(f"JSON file not found at: {json_path}")
    raise
except json.JSONDecodeError:
    logger.error(f"Failed to decode JSON from: {json_path}")
    raise

# Prepare ZIP DataFrame
zip_df = pd.DataFrame(city_zip_data)
zip_df['city'] = zip_df['city'].str.upper()
zip_df['state'] = zip_df['state'].str.upper()
zip_df['zip_code'] = zip_df['zip_code'].astype(str).str.zfill(5)
zip_df.drop_duplicates(subset=['city', 'state'], inplace=True)
logger.info("Prepared ZIP code DataFrame from JSON")

# Define ZIP fill function
def get_first_zip(city, state):
    match = zip_df[(zip_df['city'] == city) & (zip_df['state'] == state)]
    if match.empty:
        return 0
    return match['zip_code'].values[0]

# Normalize and clean ZIP data
cand_master_df['CAND_CITY'] = cand_master_df['CAND_CITY'].str.upper()
cand_master_df['CAND_ST'] = cand_master_df['CAND_ST'].str.upper()
cand_master_df['CAND_ZIP'] = cand_master_df['CAND_ZIP'].astype(str).str.replace('.0', '', regex=False)
cand_master_df['CAND_ZIP'] = cand_master_df['CAND_ZIP'].replace(['nan', 'None', '', 'NAN'], np.nan)
logger.info("Normalized CAND_CITY, CAND_ST, and cleaned CAND_ZIP values")

# Fill missing ZIPs
missing_zip_mask = cand_master_df['CAND_ZIP'].isna()
cand_master_df.loc[missing_zip_mask, 'CAND_ZIP'] = cand_master_df[missing_zip_mask].apply(
    lambda row: get_first_zip(row['CAND_CITY'], row['CAND_ST']),
    axis=1
)
logger.info(f"Filled {missing_zip_mask.sum()} missing CAND_ZIP entries based on city/state")

# Convert ZIP to integer
try:
    cand_master_df['CAND_ZIP'] = cand_master_df['CAND_ZIP'].astype(int)
    logger.info("Converted CAND_ZIP column to integer type")
except ValueError as e:
    logger.error("Error converting CAND_ZIP to int. Possibly non-numeric data remains.")
    raise e

# Save cleaned DataFrame
processed_path = f"D:/DE_Project_FEC/fec_env/big-data-fec-project/data/processed/{file_name}"
os.makedirs(processed_path, exist_ok=True)
output_file = os.path.join(processed_path, f"{file_name}.csv")

try:
    cand_master_df.to_csv(output_file, index=False)
    logger.info(f"Successfully saved cleaned data to: {output_file}")
except Exception as e:
    logger.error(f"Failed to save cleaned data to {output_file}")
    raise e