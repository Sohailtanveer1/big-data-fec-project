import pandas as pd
import json
import requests
import numpy as np
import os
from Logger import logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType


spark = SparkSession.builder \
    .appName("Data Cleaning App") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()



base_folder_path = "D:/Sohail_DE_Project/fec_env/big-data-fec-project/data/raw"
file_name = "candidate_master"
csv_path = os.path.join(base_folder_path, file_name, f"{file_name}.csv")

logger.info(f"Data processing started for: {file_name}")

cand_master_df = spark.read.option('header',True).option('inferSchema','true').csv(csv_path)
# Find Count of Null, None, NaN of All DataFrame Columns
from pyspark.sql.functions import col,isnan, when, count
cand_master_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in cand_master_df.columns]
   ).show()

# replace null cand_pty_affiliation with NNE(None) code
cand_master_df = cand_master_df.fillna({"CAND_PTY_AFFILIATION": "NNE"})

# CAND_OFFICE_DISTRICT
cand_master_df = cand_master_df.withColumn(
    "CAND_OFFICE_DISTRICT",
    when(col("CAND_OFFICE_DISTRICT").isNull(), lit(0.0)).otherwise(col("CAND_OFFICE_DISTRICT")).cast(IntegerType())
)

# CAND_ICI fill with random choice between other three values
uniq_ici_rows = cand_master_df.select("CAND_ICI").distinct().dropna().collect()
uniq_ici_list = [row["CAND_ICI"] for row in uniq_ici_rows]
cand_master_df = cand_master_df.withColumn('CAND_ICI',when(col('CAND_ICI').isNull(),np.random.choice(uniq_ici_list)).otherwise(col('CAND_ICI')))

# CAND_PCC
uniq_pcc_rows = cand_master_df.select("CAND_PCC").distinct().dropna().collect()
uniq_pcc_list = [row["CAND_PCC"] for row in uniq_pcc_rows]

cand_master_df = cand_master_df.withColumn("CAND_PCC",when(col("CAND_PCC").isNull(),np.random.choice(uniq_pcc_list)).otherwise(col("CAND_PCC")))

# 'CAND_CITY' drop  null rows
cand_master_df = cand_master_df.na.drop(subset=["CAND_CITY"])

spark.stop()