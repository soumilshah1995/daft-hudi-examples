try:
    import os
    import sys
    import uuid
    import pyspark
    import datetime
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from faker import Faker
    import datetime
    from datetime import datetime
    import random
    import pandas as pd  # Import Pandas library for pretty printing
    print("Imports loaded ")
    from pyspark.sql.types import StructType, StructField, StringType
    from pyspark.sql.functions import lit
except Exception as e:
    print("error", e)

os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@11'

HUDI_VERSION = '0.14.0'
SPARK_VERSION = '3.4'

SUBMIT_ARGS = f"--packages org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:{HUDI_VERSION} pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .getOrCreate()

# Define customer data
customers_data = [
    ("1", "John Doe", "New York", "john@example.com", "2022-01-01", "123 Main St", "NY", "I", "2022"),
    ("2", "Jane Smith", "Los Angeles", "jane@example.com", "2022-01-02", "456 Elm St", "CA", "I", "2023"),
    ("3", "Alice Johnson", "Chicago", "alice@example.com", "2022-01-03", "789 Oak St", "IL", "I", "2020")
]

customers_schema = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("city", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("created_at", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("OP", StringType(), nullable=False),
    StructField("year", StringType(), nullable=False)
])

# Create DataFrame
customers_df = spark.createDataFrame(data=customers_data, schema=customers_schema)
# customers_df = customers_df.withColumn("_hoodie_is_deleted", lit(False))

# Show DataFrame
customers_df.show()


def write_to_hudi(spark_df,
                  table_name,
                  db_name,
                  method='upsert',
                  table_type='COPY_ON_WRITE',
                  recordkey='',
                  precombine='',
                  partition_fields='',
                  sql_transformer_query="",
                  use_sql_transformer=False
                  ):

    path = f"file:///Users/soumilshah/IdeaProjects/SparkProject/tem/{db_name}/{table_name}"

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': table_type,
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': method,
        'hoodie.datasource.write.recordkey.field': recordkey,
        'hoodie.datasource.write.precombine.field': precombine,
        "hoodie.datasource.write.partitionpath.field": partition_fields,

    }


    print("\n")
    print(path)
    print("\n")

    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)


    spark_df.write.format("hudi"). \
        options(**hudi_options). \
        mode("append"). \
        save(path)


write_to_hudi(
    spark_df=customers_df,
    db_name="hudidb",
    table_name="customers",
    recordkey="customer_id",
    precombine="created_at",
    partition_fields="year",
    use_sql_transformer=True,
    sql_transformer_query="SELECT *, CASE WHEN OP = 'D' THEN true ELSE false END AS _hoodie_is_deleted FROM temp"
)
