from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("LowCrimeTopListings").getOrCreate()

# Load your crime data
crime_data = spark.read.csv("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/neighborhoods_crimme_rates_deprec.csv", header=True, inferSchema=True)

# Calculate total crimes for each neighborhood
crime_last_5_years = crime_data.select("neighborhood_id", "assault_2018", "assault_2019", "assault_2020", "assault_2021", "assault_2022", "autotheft_2018", "autotheft_2019", "autotheft_2020", "autotheft_2021", "autotheft_2022", "biketheft_2018", "biketheft_2019", "biketheft_2020", "biketheft_2021", "biketheft_2022", "breakenter_2018", "breakenter_2019", "breakenter_2020", "breakenter_2021", "breakenter_2022", "homicide_2018", "homicide_2019", "homicide_2020", "homicide_2021", "homicide_2022", "robbery_2018", "robbery_2019", "robbery_2020", "robbery_2021", "robbery_2022", "shooting_2018", "shooting_2019", "shooting_2020", "shooting_2021", "shooting_2022", "theftfrommv_2018", "theftfrommv_2019", "theftfrommv_2020", "theftfrommv_2021", "theftfrommv_2022")

crime_last_5_years = crime_last_5_years.withColumn("total_crimes", sum(crime_last_5_years[col] for col in crime_last_5_years.columns if col != "neighborhood_id"))

# Load your listings data
listings_data = spark.read.csv("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/listing_neighborhoods.csv", header=True, inferSchema=True)

# Join crime data with listings data
joined_data = listings_data.join(crime_last_5_years, on="neighborhood_id")

# Filter neighborhoods with lowest crime rates
lowest_crime_neighborhoods = joined_data.orderBy("total_crimes").limit(10)

# Select and display relevant columns for top 10 listings
top_10_listings = lowest_crime_neighborhoods.select("listing_id", "neighborhood_id", "total_crimes", "latitude", "longitude")


top_10_listings.show()
