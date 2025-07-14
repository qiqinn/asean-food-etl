import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, lit, avg, when
)

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ASEAN-ETL")

# Environment variables
HOME = os.getenv("HOME")
DATA_DIR = os.getenv("DATA_DIR", f"{HOME}/projects/asean-food-etl/data")
RAW_PATH = os.path.join(DATA_DIR, "raw", "*.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "processed", "asean_food.parquet")

# ASEAN country codes (incl. Timor Leste, Papua New Guinea)
asean_countries = [
    "IDN", "MYS", "SGP", "THA", "VNM", "PHL", "LAO", "MMR", "KHM", "BRN", "TLS", "PNG"
]

# Commodity mapping (simplified version)
commodity_mapping = {
    "Rice": "Rice",
    "Wheat": "Wheat",
    "Cassava": "Cassava",
    "Sugar": "Sugar",
    "Oil": "Oil",
    "Salt": "Salt",
    "Maize": "Maize",
    "Lentils": "Lentils"
    # Add more if needed
}

def simplify_commodity(name):
    for key in commodity_mapping:
        if key.lower() in name.lower():
            return commodity_mapping[key]
    return "Other"

# Initialize Spark
spark = SparkSession.builder \
    .appName("ASEAN Food Price ETL") \
    .getOrCreate()

logger.info("Spark session created")

# Load all raw CSVs
logger.info(f"Loading data from: {RAW_PATH}")

df = spark.read.csv(
    RAW_PATH,
    header=True,
    inferSchema=True
)

logger.info(f"Loaded {df.count()} rows")

# Filter for ASEAN countries and Retail prices only
df_asean = df.filter(
    (col("countryiso3").isin(asean_countries)) &
    (col("pricetype") == "Retail")
)

# Add month column
df_asean = df_asean.withColumn("month", to_date(col("date")))

# Simplify commodity names
from pyspark.sql.types import StringType
simplify_udf = spark.udf.register("simplify_commodity", simplify_commodity, StringType())

df_asean = df_asean.withColumn("commodity_group", simplify_udf(col("commodity")))

# Use USD price if available, otherwise convert
df_asean = df_asean.withColumn(
    "usd_price",
    when(col("usdprice").isNotNull(), col("usdprice")).otherwise(col("price"))  # fallback if needed
)

# Aggregate data: average price per country, commodity, and month
df_agg = df_asean.groupBy(
    "countryiso3", "commodity_group", "month"
).agg(
    avg("usd_price").alias("avg_usd_price")
)

# Save to Parquet partitioned by country
logger.info(f"Saving processed data to: {OUTPUT_PATH}")

df_agg.write.mode("overwrite").partitionBy("countryiso3").parquet(OUTPUT_PATH)

logger.info("ETL process completed successfully")
