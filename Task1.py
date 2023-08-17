from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create a Spark session
spark = SparkSession.builder.appName("TopCrimeNeighborhoods").getOrCreate()

# Load the crime data from CSV
crime_rates_df = spark.read.csv("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/neighborhoods_crimme_rates_deprec.csv", header=True, inferSchema=True)

# Calculate the total number of crimes for each neighborhood in the last 5 years using SQL expression
crime_rates_df.createOrReplaceTempView("crime_rates")
crime_last_5_years = spark.sql("""
    SELECT neighborhood_id,
        SUM(
            assault_2018 + assault_2019 + assault_2020 + assault_2021 + assault_2022 +
            autotheft_2018 + autotheft_2019 + autotheft_2020 + autotheft_2021 + autotheft_2022 +
            biketheft_2018 + biketheft_2019 + biketheft_2020 + biketheft_2021 + biketheft_2022 +
            breakenter_2018 + breakenter_2019 + breakenter_2020 + breakenter_2021 + breakenter_2022 +
            homicide_2018 + homicide_2019 + homicide_2020 + homicide_2021 + homicide_2022 +
            robbery_2018 + robbery_2019 + robbery_2020 + robbery_2021 + robbery_2022 +
            shooting_2018 + shooting_2019 + shooting_2020 + shooting_2021 + shooting_2022 +
            theftfrommv_2018 + theftfrommv_2019 + theftfrommv_2020 + theftfrommv_2021 + theftfrommv_2022
        ) AS total_crimes
    FROM crime_rates
    GROUP BY neighborhood_id
    ORDER BY total_crimes DESC
    LIMIT 3
""")

# Show the top 3 neighborhoods
crime_last_5_years.show()

# Stop the Spark session
spark.stop()
