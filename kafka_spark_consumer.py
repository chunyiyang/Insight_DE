
from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark_cassandra import streaming
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import Row
from pyspark import StorageLevel
from pyspark.sql.functions import sum
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import SQLContext 
from pyspark.sql import functions
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import pyspark_cassandra, sys
import json
import sys
import datetime

app_name = 'StockRank'
spark_master_address = 'spark://10.0.0.7:7077'
s3_checkpoint = 's3n://chunyiinsightprojects3/examples/'
ipfile_path = 'spark_ip_addresses.txt'
stock_topic_name = 'stock-topic-6'
user_topic_name = 'user-topic-6'


def raw_data_tojson(raw_data):
  """ convert the data stream into json file
        Args:   <raw_data: tuple (None,  {"price": 421, "ticker": "CSTR", "time": "2018:1:22:35:54"})
                <table_name: String>
        Return: json file
  """  
  json_data = raw_data.map(lambda k: json.loads(k[1]))  
  return(json_data)



if __name__ == '__main__':
  """ receives data streams from Kafka and send data into cassandra database
  """  
  spark = SparkSession.builder.appName(app_name).master(spark_master_address).getOrCreate()
  sc = spark.sparkContext
  ssc = StreamingContext(sc, 10)
  ssc.checkpoint(s3_checkpoint)

  # get all kafka broker nodes address
  ipfile = open(ipfile_path, 'r')
  ips = ipfile.read()
  ips = ips.strip('\n')
  ipfile.close()

  kafkaBrokers = {"metadata.broker.list": ips}

  s1 = KafkaUtils.createDirectStream(ssc, [stock_topic_name], kafkaBrokers)
  s2 = KafkaUtils.createDirectStream(ssc, [user_topic_name], kafkaBrokers)

  raw_stock = raw_data_tojson(s1)
  raw_user = raw_data_tojson(s2)

  
  # save the original stock price information to table stock_price
  stock_info = raw_stock.map(lambda x: {"price": x["price"], "ticker": x["ticker"],\
   "time": x["time"]})
  stock_info.saveToCassandra('mykeyspace', 'day_stock_price')

  # save the original stock price information to table user_action
  user_info = raw_user.map(lambda x: {"user_id": x["uid"], "action":x["action"], \
    "ticker": x["ticker"], "qty":x["qty"], "time": x["time"]})
  user_info.saveToCassandra('mykeyspace', 'user_action')

  
  # get the current time for UI purpose
  cur_time = datetime.datetime.now()
  time_string = str(cur_time.year) + ":" + str(cur_time.month) + ":" + str(cur_time.day)\
   + ":" + str(cur_time.minute) + ":" + str(cur_time.second)

  # save the avg stock price of the window and save it to table window_stock
  transform_stock = stock_info.map(lambda x: (x["ticker"], x["price"])).\
    reduceByKey(lambda x, y: (x+y)/2.0)
  window_stock =  transform_stock.map(lambda x: {"ticker": x[0], "low_price":x[1]})
  window_stock.saveToCassandra('mykeyspace', 'window_stock')

  # save the sum qty of the window and save it to table window_user
  transform_user = user_info.map(lambda x: (x["ticker"], x["qty"])).\
    reduceByKey(lambda x, y: x + y)  
  window_user = transform_user.map(lambda x: {"ticker": x[0], "sum_qty":x[1]})  
  window_user.saveToCassandra('mykeyspace', 'window_user')
  
  # combine these two data stream
  combined_info = transform_stock.join(transform_user).persist(StorageLevel.MEMORY_ONLY)
#  combined_info = combined_info.map(lambda x: {"ticker": x[0], "sum_qty": x[1][1], "low_price": x[1][0]}) 
#  combined_info.saveToCassandra('mykeyspace', 'combined_info')

  ssc.start()
  ssc.awaitTermination()


