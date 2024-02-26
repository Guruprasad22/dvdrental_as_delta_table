import pyspark
from delta import *
from delta.tables import *


builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "/Users/bbg/projects/delta-table-creator/postgresql-42.7.2.jar")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
jdbc_url = "jdbc:postgresql://localhost:5432/dvdrental"
connection_props = {
    "user": "postgres",
    "password": "",
    "driver": "org.postgresql.Driver"
}
table_list = ["actor", "store", "address", "category", "city", "country", "customer", "film_actor", "film_category",
              "inventory", "language", "rental", "staff", "payment", "film"]

for name in table_list:
    table_df = spark.read.jdbc(jdbc_url, name, properties=connection_props)
    table_df.write.format("delta").mode("overwrite").saveAsTable(f"{name}")

