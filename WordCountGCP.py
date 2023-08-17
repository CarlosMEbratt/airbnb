#wordcount for gcp
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Load text data
text_data = spark.read.text("gs://dataproc-staging-us-central1-624132863060-znbwsdyo/Project/hosts_id.txt")

# Split each line into words and count occurrences
word_counts = text_data.selectExpr("explode(split(value, ' ')) as word").groupBy("word").count()

# Show word counts
word_counts.show()
