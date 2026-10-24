from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder \
    .appName("HurtoStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("departamento", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("tipo_hurto", StringType(), True),
    StructField("timestamp", LongType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hurto_eventos") \
    .option("startingOffsets", "latest") \
    .load()

datos = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

resultado = datos.groupBy("tipo_hurto").count()

query = resultado.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()