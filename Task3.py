from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from functools import reduce

# Initialize Spark session
spark = SparkSession.builder.appName("HighCrimeTopListings").getOrCreate()

# Load your crime data
crime_data = spark.read.csv("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/neighborhoods_crimme_rates_deprec.csv", header=True, inferSchema=True)

# Calculate total crimes for each neighborhood
numeric_columns = [col(c) for c in crime_data.columns if c not in ["objectid", "area_name", "hood_id", "neighborhood_id", "popn_proj_2022"]]

# Calculate the sum of numeric columns and add a new column "total_crimes"
crime_last_5_years = crime_data.withColumn("total_crimes", reduce(lambda a, b: a + b, numeric_columns))

# Load your listings data
listings_data = spark.read.csv("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/listing_neighborhoods.csv", header=True, inferSchema=True)

# Join crime data with listings data
joined_data = listings_data.join(crime_last_5_years, on="neighborhood_id")

# Order neighborhoods by total crimes in descending order
highest_crime_neighborhoods = joined_data.orderBy(col("total_crimes").desc())

# Select top 10 listings in highest crime neighborhoods
top_10_listings = highest_crime_neighborhoods.select("listing_id", "neighborhood_id", "total_crimes", "latitude", "longitude").limit(10)

top_10_listings.show()

crime_data.printSchema()
crime_data.show(5)
