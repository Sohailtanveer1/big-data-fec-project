# Define a function to convert the timestamp to both local and UTC times
import time
import logging
from datetime import datetime

def format_time(record, datefmt=None):
    local_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(record.created))
    utc_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(record.created))
    return f'{local_time} (local) - {utc_time} (UTC)'


logger = logging.getLogger()
logger.setLevel(logging.INFO) # DEBUG FOR DEV AND ERROR FOR PROD

# out_dir = "./"
out_dir = "D:/DE_Project_FEC/fec_env/DE_Project_FEC/logs"
filename = 'app'
log_filename = f"{out_dir}/{filename}.log"

file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
formatter.formatTime = format_time
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
