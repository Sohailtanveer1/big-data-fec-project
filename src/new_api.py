import requests
from datetime import datetime, timedelta

# base url
base_url =  'https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/paper/'

# base folder
base_folder = "D:/DE_Project_FEC/fec_env/big-data-fec-project/data/raw/downloads/"


# Define start and end dates
start_date = datetime(2019, 11, 1)
end_date = datetime(2020, 12, 31)

# Generate list of dates in YYYYMMDD format
date_list = [(start_date + timedelta(days=i)).strftime("%Y%m%d") 
             for i in range((end_date - start_date).days + 1)]


# Print all dates
for date in date_list:
    
    file_name = date
    relative_url = f'{file_name}.nofiles.zip'
    absolute_url = base_url+relative_url

    file_response = requests.get(absolute_url, stream=True)
    
    with open(f"{base_folder}{relative_url}", "wb") as f:
        for chunk in file_response.iter_content(chunk_size=8192):
            f.write(chunk)

