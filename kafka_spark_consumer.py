
from __future__ import print_function

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark_cassandra import streaming
from pyspark.streaming.kafka import KafkaUtils
import pyspark_cassandra, sys
import json
import sys
import datetime
from pyspark.sql.types import Row

from pyspark import StorageLevel

from pyspark.sql.functions import sum
from pyspark.sql.types import *
from pyspark.sql import Window
from pyspark.sql import SQLContext 
from pyspark.sql import functions
from cassandra.cluster import Cluster


def raw_data_tojson(raw_data):
  """ Parse input json stream """
  # The Kafka messages arrive as a tuple (None,  {"price": 421, "ticker": "CSTR", "time": "2018:1:22:35:54"})
  # this first line grabs the nested json (second element) 
  raw_data = raw_data.map(lambda k: json.loads(k[1]))  
  return(raw_data)



if __name__ == '__main__':
    # Kafka and Spark Streaming specific vars

  sc = SparkContext(appName="StockRank")
  ssc = StreamingContext(sc, 10)
  ssc.checkpoint("s3n://chunyiinsightprojects3/examples/")

  '''
  cluster = Cluster()
  session = cluster.connect('mykeyspace')
 
  session.execute('CREATE TABLE IF NOT EXISTS stock_price(ticker text, price int, time text, PRIMARY KEY (ticker, time), ) WITH CLUSTERING ORDER BY (time DESC);')
  session.execute('CREATE TABLE IF NOT EXISTS user_action(ticker text, qty int, time text, user_id text, action text, PRIMARY KEY (ticker, time), ) WITH CLUSTERING ORDER BY (time DESC);')
  session.execute('CREATE TABLE IF NOT EXISTS window_stock(ticker text, low_price int, time text, PRIMARY KEY (ticker,time)) WITH CLUSTERING ORDER BY (time DESC);')
  session.execute('CREATE TABLE IF NOT EXISTS window_user(ticker text, time text, sum_qty int, PRIMARY KEY (ticker,time)) WITH CLUSTERING ORDER BY (time DESC);')
  session.execute('CREATE TABLE IF NOT EXISTS combined_info(ticker text, low_price int, time text, sum_qty int, PRIMARY KEY (ticker,time)) WITH CLUSTERING ORDER BY (time DESC);')
  '''
  ipfile = open('spark_ip_addresses.txt', 'r')
  ips = ipfile.read()[:-1]
  ipfile.close()
  ips = ips.split('\n')

  kafkaBrokers = {"metadata.broker.list": ips}
  s1 = KafkaUtils.createDirectStream(ssc, ['stock-topic'], kafkaBrokers)
  s2 = KafkaUtils.createDirectStream(ssc, ['user-topic'], kafkaBrokers)

  raw_stock = raw_data_tojson(s1)
  raw_user = raw_data_tojson(s2)

  # save the original stock price information to table stock_price
  stock_info = raw_stock.map(lambda x: {"price": x["price"], "ticker": x["ticker"], "time": x["time"]})
  stock_info.saveToCassandra('mykeyspace', 'stock_price')

  # save the original stock price information to table user_action
  user_info = raw_user.map(lambda x: {"user_id": x["uid"], "action":x["action"], "ticker": x["ticker"], "qty":x["qty"], "time": x["time"]})
  user_info.saveToCassandra('mykeyspace', 'user_action')

  # get the current time for UI purpose
  cur_time = datetime.datetime.now()
  time_string = str(cur_time.year) + ":" + str(cur_time.month) + ":" + str(cur_time.day) + ":" + str(cur_time.minute) + ":" + str(cur_time.second)

  # save the min stock price of the window and save it to table window_stock
  transform_stock = stock_info.map(lambda x: (x["ticker"], x["price"])).reduceByKey(lambda x, y: (x+y)/2.0)
  window_stock =  transform_stock.map(lambda x: {"ticker": x[0], "low_price":x[1]})
  

  # save the sum qty of the window and save it to table window_user
  transform_user = user_info.map(lambda x: (x["ticker"], x["qty"])).reduceByKey(lambda x, y: x + y)  
  window_user = transform_user.map(lambda x: {"ticker": x[0], "sum_qty":x[1]})

  start = timeit.default_timer()
  window_stock.saveToCassandra('mykeyspace', 'window_stock')
  window_user.saveToCassandra('mykeyspace', 'window_user')
  stop = timeit.default_timer()
  
  # combine these two data stream
  combined_info = transform_stock.join(transform_user).persist(StorageLevel.MEMORY_ONLY)

#  combined_info = combined_info.map(lambda x: {"ticker": x[0], "sum_qty": x[1][1], "low_price": x[1][0]}) 
#  combined_info.saveToCassandra('mykeyspace', 'combined_info')

  ssc.start()
  ssc.awaitTermination()


