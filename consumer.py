import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from time import sleep
from IPython.display import display, clear_output
from pyspark.sql.functions import col, concat, lit, to_json, struct
from pyspark.sql.types import *
from pyspark.sql import functions as f
from kafka import KafkaProducer
from json import dumps, loads

scala_version = '2.12'  # your scala version
spark_version = '3.5.0' # your spark version
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.6.0', #your kafka version
    'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0'
]
spark = SparkSession.builder.master("local").appName("kafka-example") \
                                            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/stock_prediction.prediction")\
                                            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/stock_prediction.prediction")\
                                            .config("spark.jars.packages", ",".join(packages)).getOrCreate()

topic_name = 'TimePredict'
kafka_server = 'localhost:9092'

kafkaDf = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name).option("startingOffsets", "earliest").load()

schema = StructType([ 
    StructField("date", StringType(), True),
    StructField("time" , StringType(), True),
    StructField("day" , StringType(), True),
    StructField("station" , StringType(), True),
    StructField("code" , StringType(), True),
    StructField("min_gap" , FloatType(), True),
    StructField("bound" , StringType(), True),
    StructField("line" , StringType(), True),
    StructField("vehicle" , FloatType(), True),
    StructField("min_delay" , FloatType(), True),
    StructField("features" , ArrayType(FloatType()), True),
        ])
columns = ["date", "time", "day", "station", "code", "min_gap", "bound", "line", "vehicle", "min_delay", "predicted"]

streamRawDf = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_server).option("subscribe", topic_name).load()
streamDF = streamRawDf.select(f.from_json(f.col("value").cast("string"), schema).alias("parsed_value"))
parseDF = streamDF.select(f.col("parsed_value.*"))

stream_writer = (parseDF.writeStream.queryName("stream_test").trigger(processingTime="5 seconds").outputMode("append").format("memory"))
query1 = stream_writer.start()

topic_name = 'TimePredict_Topic2'
kafka_server = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=kafka_server,value_serializer = lambda x:dumps(x).encode('utf-8'))

import predict

old =0
new =0

for x in range(0, 700):
    try:
        result1 = spark.sql(f"SELECT * from {query1.name}")
        # print(result1.select(f.col("value")).toPandas().values[-1][0])
        if (len(result1.toPandas()) !=0):
          new = len(result1.toPandas())
          if (new > old):
            df_pre = predict.predict(result1.toPandas())

            jsonDF = df_pre.withColumn("value", to_json(struct(columns)))

            print(jsonDF.select(f.col("value")).toPandas().values[-1][0])

            for i in range(new-old):
            #    print(result1.select(f.col("value")).toPandas().values[(old - new)+i][0])
               producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[(old - new)+i][0]))
            #    producer.send(topic_name, value= loads(result1.select(f.col("code")).toPandas().values[(old - new)+i][0]))

            old = new

            # if (len(jsonDF.select(f.col("value")).toPandas()) == 1):
            #     producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[0][0]))
            # else:
            #     producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[-2][0]))
            #     producer.send(topic_name, value= loads(jsonDF.select(f.col("value")).toPandas().values[-1][0]))

            sleep(5)
            clear_output(wait=True)
    except KeyboardInterrupt:
        print("break")
        break
print("Live view ended...")
