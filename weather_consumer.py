import os
import json
from _collections import defaultdict
import math

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.types as tp
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from subprocess import check_output
import datetime

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-20_2.12:7.17.5 pyspark-shell"

topic = "weather"
ETH0_IP = check_output(["hostname"]).decode(encoding="utf-8").strip()
SPARK_MASTER_URL = "local[*]"
SPARK_DRIVER_HOST = ETH0_IP

spark_conf = SparkConf()
spark_conf.set("es.port",9200)
spark_conf.set("es.output.json","true")

spark_conf.setAll(
    [
        ("spark.master", SPARK_MASTER_URL),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.driver.host", SPARK_DRIVER_HOST),
        ("spark.app.name", "weather_app"),
        ("spark.submit.deployMode", "client"),
        ("spark.ui.showConsoleProgress", "true"),
        ("spark.eventLog.enabled", "false"),
        ("spark.logConf", "false"),
    ])

es_host = "localhost"
index = "weather"
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
#es.options(ignore_status=[400, 404]).indices.delete(index=elastic_index)
es_mapping = {
    "properties":{
        "location": {"type": "geo_point"},
        "elevation": {"type": "float"},
        "datetime": {"type": "date"},
        "temperature_2m": {"type": "float"},
        "relativehumidity_2m": {"type": "integer"},
        "apparent_temperature": {"type": "float"},
        "precipitation": {"type": "float"},
        "windspeed_10m": {"type": "float"},
    }
}

response = es.indices.create(
    index = index,
    mappings = es_mapping,
    ignore = 400
)

weatherSchema = tp.StructType([
    #tp.StructField("latitude", tp.FloatType(), True),
    #tp.StructField("longitude", tp.FloatType(), True),
    tp.StructField("location", tp.StringType()),
    tp.StructField("elevation", tp.FloatType()),
    #tp.StructField("Units", tp.StringType(), True),
    tp.StructField("datetime", tp.TimestampType()),
    tp.StructField("temperature_2m", tp.FloatType()),
    tp.StructField("relativehumidity_2m", tp.IntegerType()),
    tp.StructField("apparent_temperature", tp.FloatType()),
    tp.StructField("precipitation", tp.FloatType()),
    tp.StructField("windspeed_10m", tp.FloatType()),
])

def test(batchDF, batchID):
    valueRdd = batchDF.rdd.map(lambda x: x[1])
    locationWeathers = valueRdd.map(lambda x: json.loads(x)).collect()
    if len(locationWeathers) != 0:
        for j in range(len(locationWeathers)):
            locationWeather = locationWeathers[j]
            latitude  = locationWeather["latitude"]
            longitude = locationWeather["longitude"]
            elevation = locationWeather["elevation"]
            units = locationWeather["hourly_units"]
            timestamps = locationWeather["hourly"]["time"]
            temperature_2m = locationWeather["hourly"]["temperature_2m"]
            relativehumidity_2m = locationWeather["hourly"]["relativehumidity_2m"]
            apparent_temperature = locationWeather["hourly"]["apparent_temperature"]
            precipitation = locationWeather["hourly"]["precipitation"]
            windspeed_10m = locationWeather["hourly"]["windspeed_10m"]
            winddirection_10m = locationWeather["hourly"]["winddirection_10m"]
            nbrData = len(timestamps)
            weather = []
            positions = set()
            for i in range(nbrData):
                entry = dict()
                weather = []
                dt_time = timestamps[i].split("T")
                dt = dt_time[0].split("-")
                _time = dt_time[1].split(":")
                timestamp = datetime.datetime(int(dt[0]), int(dt[1]), int(dt[2]), hour=int(_time[0]), minute=int(_time[1]))
                temp = temperature_2m[i]
                humi = relativehumidity_2m[i]
                apparentTemp = apparent_temperature[i]
                precip = precipitation[i]
                windspeed = windspeed_10m[i]
                winddir = winddirection_10m[i]
                #weather.append((latitude, longitude, elevation, timestamp, temp, humi, apparentTemp, precip, windspeed, winddir))
                location = str(latitude) + "," + str(longitude)
                entry["location"] = location
                entry["elevation"] = elevation
                entry["temperature_2m"] = temp
                entry["precipitation"] = precip
                entry["windspeed_10m"] = windspeed
                entry["relativehumidity_2m"] = humi
                entry["apparent_temperature"] = apparentTemp
                entry["datetime"] = timestamp
                weather.append(entry)
                data = spark.createDataFrame(data = weather, schema = weatherSchema)
                print(location)
                #positions.add(location)
                data.write.format("org.elasticsearch.spark.sql").mode("append").option("es.nodes", es_host).option('es.port', 9200).save(index)
            #print(data)
        else:
            print("empty")


if __name__ == "__main__":
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .load()

    df2 = df.writeStream.foreachBatch(test).start().awaitTermination()